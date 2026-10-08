"""Load and validate gateway configuration from a local JSON file.

The file is the single editable source of truth for upstreams, routes and
per-caller quotas. Parsing/validation never mutates a running gateway:
callers receive a fully built ``GatewayConfig`` plus version metadata, or a
``ConfigError`` listing every operator-readable reason for rejection.

Config document layout::

    {
      "version": "ops-2026-10-08-1",          # optional free-form label
      "defaults": {"rate_per_sec": 20, "burst": 20, "max_concurrency": 10},
      "upstreams": [
        {"name": "u1", "base_url": "http://127.0.0.1:9001",
         "weight": 1, "fail_threshold": 1, "recover_threshold": 1}
      ],
      "routes": [
        {"name": "echo", "path_prefix": "/v1/echo", "upstreams": ["u1"],
         "strategy": "round_robin", "strip_prefix": "/v1",
         "timeout_ms": 800, "max_retries": 2, "cache_ttl_ms": 2000}
      ],
      "rate_limits": [
        {"caller": "premium", "rate_per_sec": 100, "burst": 100,
         "max_concurrency": 20}
      ]
    }
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from .config import (
    BalanceStrategy, GatewayConfig, RateLimitConfig, RouteConfig,
    UpstreamConfig,
)

# Field defaults mirrored from the dataclasses so the file only needs to
# specify what it wants to change.
_ROUTE_DEFAULTS: dict[str, Any] = {
    "strategy": "round_robin",
    "strip_prefix": None,
    "cache_ttl_ms": 0,
    "timeout_ms": 1000,
    "max_retries": 0,
}
_UPSTREAM_DEFAULTS: dict[str, Any] = {
    "weight": 1,
    "fail_threshold": 1,
    "recover_threshold": 1,
}
_QUOTA_DEFAULTS: dict[str, Any] = {
    "rate_per_sec": 100,
    "burst": 100,
    "max_concurrency": 100,
}

_ALLOWED_TOP = {"version", "defaults", "upstreams", "routes", "rate_limits"}
_ALLOWED_STRATEGY = {s.value for s in BalanceStrategy}


@dataclass(frozen=True)
class LoadedConfig:
    """Result of successfully reading a config file."""
    config: GatewayConfig
    version: str | None  # operator-provided label, may be absent
    content_sha256: str  # hash of raw file bytes, identifies exact content


class ConfigError(ValueError):
    """Raised when a config document cannot be accepted.

    ``reasons`` contains human-readable, location-tagged explanations so an
    operator can fix the file without reading gateway source code.
    """

    def __init__(self, reasons: list[str]) -> None:
        self._reasons = reasons
        super().__init__("config rejected: " + "; ".join(reasons))

    @property
    def reasons(self) -> list[str]:
        return list(self._reasons)


def load_config_file(path: str) -> LoadedConfig:
    """Read, parse and validate the JSON config file at ``path``.

    Raises :class:`ConfigError` (with a file-tagged reason) when the file
    cannot be read or is not a valid JSON object.
    """
    try:
        with open(path, "rb") as handle:
            raw_bytes = handle.read()
    except OSError as exc:
        raise ConfigError([f"file '{path}': cannot read ({exc})"]) from None
    content_sha = hashlib.sha256(raw_bytes).hexdigest()
    try:
        document = json.loads(raw_bytes.decode("utf-8"))
    except UnicodeDecodeError:
        raise ConfigError([f"file '{path}': content is not valid UTF-8"]) from None
    except json.JSONDecodeError as exc:
        raise ConfigError([
            f"file '{path}': invalid JSON at line {exc.lineno} "
            f"column {exc.colno} ({exc.msg})"
        ]) from None
    config, version = build_config(document)
    return LoadedConfig(config=config, version=version,
                       content_sha256=content_sha)


def build_config(raw: object) -> tuple[GatewayConfig, str | None]:
    """Validate a decoded JSON document and build an immutable config.

    All detectable problems are collected and reported together (best-effort
    full validation); nothing is applied by the caller afterwards.
    """
    errors: list[str] = []
    if not isinstance(raw, dict):
        raise ConfigError(["top level must be a JSON object"])

    for key in raw:
        if key not in _ALLOWED_TOP:
            errors.append(f"$.{key}: unknown field (allowed: "
                          f"{sorted(_ALLOWED_TOP)})")

    version = _parse_version(raw.get("version"), errors)

    defaults = _parse_defaults(raw.get("defaults", {}), errors)

    upstreams, upstream_index = _parse_upstreams(
        raw.get("upstreams", []), errors)
    routes = _parse_routes(raw.get("routes", []), errors, upstream_index)
    rate_limits = _parse_rate_limits(raw.get("rate_limits", []), errors)

    if errors:
        raise ConfigError(errors)
    return (
        GatewayConfig(
            upstreams=upstreams,
            routes=routes,
            rate_limits=rate_limits,
            default_rate_per_sec=defaults["rate_per_sec"],
            default_burst=defaults["burst"],
            default_max_concurrency=defaults["max_concurrency"],
        ),
        version,
    )


def _parse_version(value: object, errors: list[str]) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        errors.append("$.version: must be a non-empty string when present")
        return None
    label = value.strip()
    if len(label) > 200:
        errors.append("$.version: label longer than 200 characters")
    return label


def _is_int(value: object) -> bool:
    # bool is a subclass of int; reject it explicitly for config numbers.
    return isinstance(value, int) and not isinstance(value, bool)


def _require_positive_int(field: str, value: object,
                          errors: list[str]) -> int | None:
    if not _is_int(value) or value <= 0:
        errors.append(f"$.{field}: must be an integer > 0 (got {value!r})")
        return None
    return value


def _parse_defaults(value: object, errors: list[str]) -> dict[str, int]:
    result = dict(_QUOTA_DEFAULTS)
    if value is None:
        return result
    if not isinstance(value, dict):
        errors.append("$.defaults: must be an object")
        return result
    allowed = set(_QUOTA_DEFAULTS)
    for key, val in value.items():
        if key not in allowed:
            errors.append(f"$.defaults.{key}: unknown field")
            continue
        parsed = _require_positive_int(f"defaults.{key}", val, errors)
        if parsed is not None:
            result[key] = parsed
    return result


def _parse_upstreams(
    value: object, errors: list[str]
) -> tuple[tuple[UpstreamConfig, ...], dict[str, UpstreamConfig]]:
    items: list[UpstreamConfig] = []
    index: dict[str, UpstreamConfig] = {}
    if not isinstance(value, list):
        errors.append("$.upstreams: must be an array")
        return (), {}
    seen: set[str] = set()
    for i, node in enumerate(value):
        where = f"$.upstreams[{i}]"
        if not isinstance(node, dict):
            errors.append(f"{where}: must be an object")
            continue
        name = node.get("name")
        if not isinstance(name, str) or not name.strip():
            errors.append(f"{where}.name: must be a non-empty string")
            name = None
        elif name in seen:
            errors.append(f"{where}.name: duplicate upstream name {name!r}")
        base_url = node.get("base_url")
        if (not isinstance(base_url, str)
                or not (base_url.startswith("http://")
                        or base_url.startswith("https://"))):
            errors.append(
                f"{where}.base_url: must be an http(s) URL (got {base_url!r})")
            base_url = None

        kwargs = dict(_UPSTREAM_DEFAULTS)
        for key in ("weight", "fail_threshold", "recover_threshold"):
            if key in node:
                parsed = _require_positive_int(
                    f"upstreams[{i}].{key}", node[key], errors)
                if parsed is not None:
                    kwargs[key] = parsed
        for key in node:
            if key not in ("name", "base_url", *_UPSTREAM_DEFAULTS):
                errors.append(f"{where}.{key}: unknown field")

        if name is not None and name not in seen and base_url is not None:
            upstream = UpstreamConfig(name=name, base_url=base_url, **kwargs)
            items.append(upstream)
            index[name] = upstream
            seen.add(name)
    return tuple(items), index


def _parse_routes(value: object, errors: list[str],
                  upstreams: dict[str, UpstreamConfig]
                  ) -> tuple[RouteConfig, ...]:
    items: list[RouteConfig] = []
    if not isinstance(value, list):
        errors.append("$.routes: must be an array")
        return ()
    seen_names: set[str] = set()
    seen_prefixes: set[str] = set()
    for i, node in enumerate(value):
        where = f"$.routes[{i}]"
        if not isinstance(node, dict):
            errors.append(f"{where}: must be an object")
            continue
        name = node.get("name")
        if not isinstance(name, str) or not name.strip():
            errors.append(f"{where}.name: must be a non-empty string")
        elif name in seen_names:
            errors.append(f"{where}.name: duplicate route name {name!r}")

        prefix = node.get("path_prefix")
        if not isinstance(prefix, str) or not prefix.startswith("/"):
            errors.append(
                f"{where}.path_prefix: must be a string starting with '/' "
                f"(got {prefix!r})")
            prefix = None
        elif prefix in seen_prefixes:
            errors.append(
                f"{where}.path_prefix: duplicate prefix {prefix!r}")

        names = node.get("upstreams")
        picked: list[str] = []
        if not isinstance(names, list) or not names:
            errors.append(
                f"{where}.upstreams: must be a non-empty array of upstream "
                f"names")
        else:
            local_seen: set[str] = set()
            for upstream_name in names:
                if not isinstance(upstream_name, str):
                    errors.append(
                        f"{where}.upstreams: entries must be strings "
                        f"(got {upstream_name!r})")
                    continue
                if upstream_name in local_seen:
                    errors.append(
                        f"{where}.upstreams: {upstream_name!r} listed more "
                        f"than once")
                    continue
                local_seen.add(upstream_name)
                if upstream_name not in upstreams:
                    errors.append(
                        f"{where}.upstreams: references unknown upstream "
                        f"{upstream_name!r} (declare it in $.upstreams)")
                else:
                    picked.append(upstream_name)

        strategy = node.get("strategy", _ROUTE_DEFAULTS["strategy"])
        if strategy not in _ALLOWED_STRATEGY:
            errors.append(
                f"{where}.strategy: must be one of "
                f"{sorted(_ALLOWED_STRATEGY)} (got {strategy!r})")
            strategy = _ROUTE_DEFAULTS["strategy"]

        strip_prefix = node.get("strip_prefix",
                                _ROUTE_DEFAULTS["strip_prefix"])
        if strip_prefix is not None and not isinstance(strip_prefix, str):
            errors.append(
                f"{where}.strip_prefix: must be a string or null "
                f"(got {strip_prefix!r})")
            strip_prefix = _ROUTE_DEFAULTS["strip_prefix"]

        kwargs: dict[str, Any] = {}
        for key in ("timeout_ms", "max_retries", "cache_ttl_ms"):
            if key in node:
                if not _is_int(node[key]) or node[key] < 0:
                    errors.append(
                        f"{where}.{key}: must be a non-negative integer "
                        f"(got {node[key]!r})")
                else:
                    kwargs[key] = node[key]
        # timeout must stay strictly positive even though the other two may be 0.
        if "timeout_ms" in kwargs and kwargs["timeout_ms"] == 0:
            errors.append(f"{where}.timeout_ms: must be > 0")
        for key in node:
            if key not in ("name", "path_prefix", "upstreams", "strategy",
                           "strip_prefix", *_ROUTE_DEFAULTS):
                errors.append(f"{where}.{key}: unknown field")

        if (isinstance(name, str) and name.strip()
                and name not in seen_names
                and prefix is not None and prefix not in seen_prefixes
                and picked):
            route_kwargs = {
                k: v for k, v in _ROUTE_DEFAULTS.items()
                if k not in ("strategy", "strip_prefix")
            }
            route_kwargs.update(kwargs)
            items.append(RouteConfig(
                name=name,
                path_prefix=prefix,
                upstream_names=tuple(picked),
                strategy=BalanceStrategy(strategy),
                strip_prefix=strip_prefix,
                **route_kwargs,
            ))
            seen_names.add(name)
            seen_prefixes.add(prefix)
    return tuple(items)


def _parse_rate_limits(value: object, errors: list[str]
                       ) -> tuple[RateLimitConfig, ...]:
    items: list[RateLimitConfig] = []
    if not isinstance(value, list):
        errors.append("$.rate_limits: must be an array")
        return ()
    seen: set[str] = set()
    for i, node in enumerate(value):
        where = f"$.rate_limits[{i}]"
        if not isinstance(node, dict):
            errors.append(f"{where}: must be an object")
            continue
        caller = node.get("caller")
        if not isinstance(caller, str) or not caller.strip():
            errors.append(f"{where}.caller: must be a non-empty string")
            caller = None
        elif caller in seen:
            errors.append(f"{where}.caller: duplicate quota entry "
                          f"{caller!r}")
        parsed_fields: dict[str, int] = {}
        for key in ("rate_per_sec", "burst", "max_concurrency"):
            if key not in node:
                errors.append(f"{where}.{key}: required positive integer")
                continue
            number = _require_positive_int(
                f"rate_limits[{i}].{key}", node[key], errors)
            if number is not None:
                parsed_fields[key] = number
        for key in node:
            if key not in ("caller", "rate_per_sec", "burst",
                           "max_concurrency"):
                errors.append(f"{where}.{key}: unknown field")
        if caller is not None and caller not in seen \
                and len(parsed_fields) == 3:
            items.append(RateLimitConfig(caller=caller, **parsed_fields))
            seen.add(caller)
    return tuple(items)
