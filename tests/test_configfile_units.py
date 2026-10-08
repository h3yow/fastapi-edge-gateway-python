"""Unit tests for file config parsing, validation and error reporting."""
from __future__ import annotations

import json

import pytest

from gateway.configfile import (
    ConfigError, ConfigValidationError, load_config_file, parse_config,
)


def _write(tmp_path, body) -> str:
    path = tmp_path / "cfg.json"
    if isinstance(body, (dict, list)):
        path.write_text(json.dumps(body), encoding="utf-8")
    else:
        path.write_text(body, encoding="utf-8")
    return str(path)


VALID = {
    "version": "v1",
    "upstreams": [
        {"name": "u1", "base_url": "http://127.0.0.1:9001"},
        {"name": "u2", "base_url": "sim://u2", "weight": 2},
    ],
    "routes": [
        {"name": "r1", "path_prefix": "/v1/report",
         "upstreams": ["u1", "u2"], "cache_ttl_ms": 100,
         "timeout_ms": 500, "max_retries": 1, "strategy": "random"},
    ],
    "rate_limits": [
        {"caller": "premium", "rate_per_sec": 10, "burst": 20,
         "max_concurrency": 5},
    ],
    "defaults": {"rate_per_sec": 1, "burst": 2, "max_concurrency": 3},
}


def test_valid_file_loads_with_metadata(tmp_path) -> None:
    path = _write(tmp_path, VALID)
    loaded = load_config_file(path, sim_urls={"u2": "http://127.0.0.1:9002"})
    assert loaded.version == "v1"
    assert loaded.source_path == path
    assert loaded.content_hash and len(loaded.content_hash) == 64
    cfg = loaded.config
    assert cfg.upstream("u2").base_url == "http://127.0.0.1:9002"
    assert cfg.upstream("u2").weight == 2
    assert cfg.rate_limit_for("premium").max_concurrency == 5
    assert cfg.rate_limit_for("other").rate_per_sec == 1


def test_missing_file_raises_config_error(tmp_path) -> None:
    with pytest.raises(ConfigError):
        load_config_file(str(tmp_path / "nope.json"))


def test_invalid_json_reports_position(tmp_path) -> None:
    path = _write(tmp_path, '{"version": "v1", broken}')
    with pytest.raises(ConfigError) as exc:
        load_config_file(path)
    assert "invalid JSON" in str(exc.value)
    assert "line" in str(exc.value)


def test_non_object_top_level_rejected(tmp_path) -> None:
    with pytest.raises(ConfigValidationError) as exc:
        parse_config([1, 2], source="inline")
    assert exc.value.errors[0].startswith("inline:")


def test_semantic_errors_are_aggregated(tmp_path) -> None:
    bad = {
        "version": "  ",
        "upstreams": [
            {"name": "u1", "base_url": "ftp://nohost"},
            {"name": "u1", "base_url": "http://x"},
        ],
        "routes": [
            {"name": "r1", "path_prefix": "relative", "upstreams": ["ghost"]},
            {"name": "r1", "path_prefix": "/ok", "upstreams": ["u1"]},
        ],
        "rate_limits": [
            {"caller": "c", "rate_per_sec": -1, "burst": 0,
             "max_concurrency": "x"},
        ],
        "defaults": {"rate_per_sec": 0},
    }
    path = _write(tmp_path, bad)
    with pytest.raises(ConfigValidationError) as exc:
        load_config_file(path)
    joined = "\n".join(exc.value.errors)
    # Every problem class must be reported, not just the first one.
    assert "version" in joined
    assert "base_url" in joined
    assert "duplicate upstream name" in joined
    assert "absolute path" in joined
    assert "unknown upstream 'ghost'" in joined
    assert "duplicate route name" in joined
    assert "rate_per_sec" in joined
    assert len(exc.value.errors) >= 7


def test_unresolved_sim_node_rejected(tmp_path) -> None:
    data = json.loads(json.dumps(VALID))
    data["upstreams"][1]["base_url"] = "sim://not-running"
    path = _write(tmp_path, data)
    with pytest.raises(ConfigValidationError) as exc:
        load_config_file(path, sim_urls={"u2": "http://127.0.0.1:9002"})
    assert any("not running" in m for m in exc.value.errors)


def test_empty_routes_or_upstreams_rejected(tmp_path) -> None:
    data = json.loads(json.dumps(VALID))
    data["upstreams"] = []
    data["routes"] = []
    path = _write(tmp_path, data)
    with pytest.raises(ConfigValidationError) as exc:
        load_config_file(path)
    joined = "\n".join(exc.value.errors)
    assert "upstreams" in joined and "routes" in joined


def test_route_matching_segment_boundaries_and_specificity() -> None:
    from gateway.config import GatewayConfig, RouteConfig

    def r(name: str, prefix: str) -> RouteConfig:
        return RouteConfig(name=name, path_prefix=prefix,
                           upstream_names=("u1",))

    cfg = GatewayConfig(routes=(
        r("rootish", "/"),
        r("api", "/api"),
        r("report", "/v1/report"),
        r("reporter", "/v1/reporter"),
        r("report_detail", "/v1/report/detail"),
    ))
    # Segment boundary: adjacent spelling is not swallowed.
    assert cfg.route_for("/v1/report").name == "report"
    assert cfg.route_for("/v1/report/anything").name == "report"
    assert cfg.route_for("/v1/reporter").name == "reporter"
    # Adjacent spelling must NOT be swallowed by the reporter route.
    assert cfg.route_for("/v1/reporters").name == "rootish"
    # Same for the report route: falls through to the catch-all, never report.
    assert cfg.route_for("/v1/reportage").name == "rootish"
    # Overlapping rules: the more specific (longer) prefix wins.
    assert cfg.route_for("/v1/report/detail/x").name == "report_detail"
    # Prefix normalization: trailing slash does not change the resource.
    assert cfg.route_for("/api/").name == "api"
    assert cfg.route_for("/api/x").name == "api"
    assert cfg.route_for("/elsewhere").name == "rootish"
