"""Load and validate gateway traffic configuration from a local editable file.

The file format is JSON. Loading is all-or-nothing: structural parsing and
full semantic validation happen before any running component is touched, so a
failed reload leaves the previous configuration serving traffic.

Schema (see README for the prose version)::

    {
      "version": "2026-10-08-r1",
      "upstreams": [
        {"name": "u1", "base_url": "http://127.0.0.1:9001", "weight": 1,
         "fail_threshold": 1, "recover_threshold": 1}
      ],
      "routes": [
        {"name": "report", "path_prefix": "/v1/report",
         "upstreams": ["u1"], "strategy": "round_robin",
         "strip_prefix": "/v1/report", "cache_ttl_ms": 2000,
         "timeout_ms": 1000, "max_retries": 1}
      ],
      "rate_limits": [
        {"caller": "premium", "rate_per_sec": 100, "burst": 100,
         "max_concurrency": 20}
      ],
      "defaults": {"rate_per_sec": 20, "burst": 20, "max_concurrency": 10}
    }

``base_url`` may also be ``sim://<node-name>`` which is resolved against the
locally started upstream-simulator cluster (used by run_gateway/tests).
"""
from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass

from .config import (
    BalanceStrategy, GatewayConfig, RateLimitConfig, RouteConfig,
    UpstreamConfig,
)

_STRATEGIES = {s.value for s in BalanceStrategy}


class ConfigError(Exception):
    """The configuration file cannot be read or parsed at all."""


class ConfigValidationError(ConfigError):
    """The file parsed, but one or more semantic rules were violated."""

    def __init__(self, errors: list[str]) -> None:
        self.errors = errors
        super().__init__("; ".join(errors))


@dataclass(frozen=True)
class LoadedConfig:
    """A fully validated configuration plus its version metadata."""
    config: GatewayConfig
    version: str
    source_path: str
    content_hash: str
    loaded_at_ms: int


def load_config_file(path: str,
                     sim_urls: dict[str, str] | None = None) -> LoadedConfig:
    """Read, parse and validate a configuration file atomically.

    Raises :class:`ConfigError` (unreadable/invalid JSON) or
    :class:`ConfigValidationError` (semantic problems, see ``errors``).
    """
    try:
        with open(path, "rb") as handle:
            data = handle.read()
    except OSError as exc:
        raise ConfigError(f"cannot read config file {path}: {exc}") from exc

    try:
        raw = json.loads(data.decode("utf-8"))
    except UnicodeDecodeError as exc:
        raise ConfigError(f"config file {path} is not UTF-8: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise ConfigError(
            f"invalid JSON in {path} at line {exc.lineno} col {exc.colno}: "
            f"{exc.msg}"
        ) from exc

    content_hash = hashlib.sha256(data).hexdigest()
    loaded_at_ms = time.time_ns() // 1_000_000
    return parse_config(raw, source=path, sim_urls=sim_urls,
                        content_hash=content_hash, loaded_at_ms=loaded_at_ms)


def parse_config(raw: object, source: str = "<inline>",
                 sim_urls: dict[str, str] | None = None,
                 content_hash: str = "",
                 loaded_at_ms: int = 0) -> LoadedConfig:
    """Validate already-parsed JSON data and build a GatewayConfig."""
    errors: list[str] = []
    sim_urls = sim_urls or {}

    if not isinstance(raw, dict):
        raise ConfigValidationError(
            [f"{source}: top-level value must be an object, got "
             f"{type(raw).__name__}"]
        )

    version = _require_string(raw, "version", errors)
    if version is not None and not version.strip():
        errors.append("version: must be a non-empty string")

    upstreams = _build_upstreams(raw.get("upstreams"), errors, sim_urls)
    routes = _build_routes(raw.get("routes"), errors, upstreams)
    rate_limits, default_rps, default_burst, default_conc = _build_limits(
        raw.get("rate_limits"), raw.get("defaults"), errors
    )

    if errors:
        raise ConfigValidationError(
            [f"{source}: {msg}" for msg in errors]
        )

    config = GatewayConfig(
        upstreams=upstreams,
        routes=routes,
        rate_limits=rate_limits,
        default_rate_per_sec=default_rps,
        default_burst=default_burst,
        default_max_concurrency=default_conc,
    )
    return LoadedConfig(
        config=config,
        version=version or "unnamed",
        source_path=source,
        content_hash=content_hash,
        loaded_at_ms=loaded_at_ms,
    )


def _require_string(data: dict, key: str, errors: list[str]) -> str | None:
    value = data.get(key)
    if not isinstance(value, str):
        errors.append(f"{key}: string is required")
        return None
    return value


def _as_positive_int(value: object, label: str, errors: list[str],
                     minimum: int = 1, default: int | None = None) -> int | None:
    if value is None:
        if default is not None:
            return default
        errors.append(f"{label}: integer >= {minimum} is required")
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        errors.append(f"{label}: must be an integer >= {minimum}, "
                      f"got {value!r}")
        return None
    if value < minimum:
        errors.append(f"{label}: must be >= {minimum}, got {value}")
        return None
    return value


def _resolve_base_url(value: str, sim_urls: dict[str, str],
                      errors: list[str], label: str) -> str:
    if value.startswith("sim://"):
        node = value[len("sim://"):].strip("/")
        resolved = sim_urls.get(node)
        if resolved is None:
            errors.append(
                f"{label}: references local simulator node {node!r} which is "
                f"not running (available: {sorted(sim_urls) or 'none'})"
            )
        return resolved or ""
    if not (value.startswith("http://") or value.startswith("https://")):
        errors.append(
            f"{label}: must start with http://, https:// or sim://, "
            f"got {value!r}"
        )
    return value


def _build_upstreams(raw: object, errors: list[str],
                     sim_urls: dict[str, str]) -> tuple[UpstreamConfig, ...]:
    if not isinstance(raw, list) or not raw:
        errors.append("upstreams: non-empty list is required")
        return ()
    result: list[UpstreamConfig] = []
    seen: set[str] = set()
    for idx, item in enumerate(raw):
        where = f"upstreams[{idx}]"
        if not isinstance(item, dict):
            errors.append(f"{where}: must be an object")
            continue
        name = item.get("name")
        if not isinstance(name, str) or not name.strip():
            errors.append(f"{where}.name: non-empty string is required")
            name = None
        elif name in seen:
            errors.append(f"{where}.name: duplicate upstream name {name!r}")
        if name is not None:
            seen.add(name)

        base = item.get("base_url")
        if not isinstance(base, str) or not base.strip():
            errors.append(f"{where}.base_url: non-empty string is required")
            resolved_url = ""
        else:
            resolved_url = _resolve_base_url(
                base.strip(), sim_urls, errors, f"{where}.base_url"
            )

        weight = _as_positive_int(item.get("weight", 1),
                                  f"{where}.weight", errors, default=1)
        fail_threshold = _as_positive_int(
            item.get("fail_threshold", 1),
            f"{where}.fail_threshold", errors, default=1)
        recover_threshold = _as_positive_int(
            item.get("recover_threshold", 1),
            f"{where}.recover_threshold", errors, default=1)

        if name and resolved_url and weight is not None \
                and fail_threshold is not None \
                and recover_threshold is not None:
            result.append(UpstreamConfig(
                name=name,
                base_url=resolved_url,
                weight=weight,
                fail_threshold=fail_threshold,
                recover_threshold=recover_threshold,
            ))
    return tuple(result)


def _build_routes(raw: object, errors: list[str],
                  upstreams: tuple[UpstreamConfig, ...]) -> tuple[RouteConfig, ...]:
    if not isinstance(raw, list) or not raw:
        errors.append("routes: non-empty list is required")
        return ()
    known = {u.name for u in upstreams}
    result: list[RouteConfig] = []
    seen: set[str] = set()
    for idx, item in enumerate(raw):
        where = f"routes[{idx}]"
        if not isinstance(item, dict):
            errors.append(f"{where}: must be an object")
            continue
        name = item.get("name")
        if not isinstance(name, str) or not name.strip():
            errors.append(f"{where}.name: non-empty string is required")
            name = None
        elif name in seen:
            errors.append(f"{where}.name: duplicate route name {name!r}")
        if name is not None:
            seen.add(name)

        prefix = item.get("path_prefix")
        if not isinstance(prefix, str) or not prefix.startswith("/"):
            errors.append(
                f"{where}.path_prefix: absolute path starting with '/' is "
                f"required, got {prefix!r}"
            )
            prefix = None
        else:
            prefix = _normalize_route_prefix(prefix)

        strategy_value = item.get("strategy", "round_robin")
        if strategy_value not in _STRATEGIES:
            errors.append(f"{where}.strategy: one of {sorted(_STRATEGIES)} "
                          f"is required, got {strategy_value!r}")
            strategy = BalanceStrategy.ROUND_ROBIN
        else:
            strategy = BalanceStrategy(strategy_value)

        names = item.get("upstreams")
        picked: list[str] = []
        if not isinstance(names, list) or not names:
            errors.append(f"{where}.upstreams: non-empty list of upstream "
                          f"names is required")
        else:
            local_seen: set[str] = set()
            for j, ref in enumerate(names):
                if not isinstance(ref, str):
                    errors.append(f"{where}.upstreams[{j}]: upstream name "
                                  f"string is required, got {ref!r}")
                    continue
                if ref in local_seen:
                    errors.append(f"{where}.upstreams[{j}]: upstream "
                                  f"{ref!r} listed more than once")
                local_seen.add(ref)
                if ref not in known:
                    errors.append(
                        f"{where}.upstreams[{j}]: references unknown upstream "
                        f"{ref!r} (known: {sorted(known) or 'none'})"
                    )
                else:
                    picked.append(ref)

        strip = item.get("strip_prefix", None)
        if strip is not None:
            if not isinstance(strip, str) or (strip and not strip.startswith("/")):
                errors.append(f"{where}.strip_prefix: must be null, '' or an "
                              f"absolute path, got {strip!r}")
                strip = None

        cache_ttl = _as_positive_int(
            item.get("cache_ttl_ms", 0), f"{where}.cache_ttl_ms", errors,
            minimum=0, default=0)
        timeout = _as_positive_int(
            item.get("timeout_ms", 1000), f"{where}.timeout_ms", errors,
            default=1000)
        retries = _as_positive_int(
            item.get("max_retries", 0), f"{where}.max_retries", errors,
            minimum=0, default=0)

        if name and prefix is not None and picked and cache_ttl is not None \
                and timeout is not None and retries is not None:
            result.append(RouteConfig(
                name=name,
                path_prefix=prefix,
                upstream_names=tuple(picked),
                strategy=strategy,
                strip_prefix=strip,
                cache_ttl_ms=cache_ttl,
                timeout_ms=timeout,
                max_retries=retries,
            ))
    return tuple(result)


def _normalize_route_prefix(prefix: str) -> str:
    if len(prefix) > 1:
        return prefix.rstrip("/") or "/"
    return prefix


def _build_limits(raw_rl: object, raw_defaults: object,
                  errors: list[str]
                  ) -> tuple[tuple[RateLimitConfig, ...], int, int, int]:
    default_rps = default_burst = default_conc = None
    if raw_defaults is None:
        default_rps, default_burst, default_conc = 100, 100, 100
    elif not isinstance(raw_defaults, dict):
        errors.append("defaults: must be an object")
    else:
        default_rps = _as_positive_int(
            raw_defaults.get("rate_per_sec", 100),
            "defaults.rate_per_sec", errors, default=100)
        default_burst = _as_positive_int(
            raw_defaults.get("burst", 100), "defaults.burst", errors,
            default=100)
        default_conc = _as_positive_int(
            raw_defaults.get("max_concurrency", 100),
            "defaults.max_concurrency", errors, default=100)

    result: list[RateLimitConfig] = []
    if raw_rl is not None:
        if not isinstance(raw_rl, list):
            errors.append("rate_limits: must be a list")
        else:
            seen: set[str] = set()
            for idx, item in enumerate(raw_rl):
                where = f"rate_limits[{idx}]"
                if not isinstance(item, dict):
                    errors.append(f"{where}: must be an object")
                    continue
                caller = item.get("caller")
                if not isinstance(caller, str) or not caller.strip():
                    errors.append(f"{where}.caller: non-empty string required")
                    caller = None
                elif caller in seen:
                    errors.append(f"{where}.caller: duplicate caller "
                                  f"{caller!r}")
                if caller is not None:
                    seen.add(caller)
                rps = _as_positive_int(item.get("rate_per_sec"),
                                       f"{where}.rate_per_sec", errors)
                burst = _as_positive_int(item.get("burst"),
                                         f"{where}.burst", errors)
                conc = _as_positive_int(item.get("max_concurrency"),
                                        f"{where}.max_concurrency", errors)
                if caller and rps is not None and burst is not None \
                        and conc is not None:
                    result.append(RateLimitConfig(
                        caller=caller, rate_per_sec=rps, burst=burst,
                        max_concurrency=conc,
                    ))
    return (tuple(result),
            default_rps or 100, default_burst or 100, default_conc or 100)
