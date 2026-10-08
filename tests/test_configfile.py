"""Unit tests for local config-file parsing and validation."""
from __future__ import annotations

import json

import pytest

from gateway.config import GatewayConfig, prefix_matches
from gateway.configfile import ConfigError, build_config, load_config_file


def _write(tmp_path, document: object, name: str = "gw.json") -> str:
    path = tmp_path / name
    path.write_text(json.dumps(document), encoding="utf-8")
    return str(path)


VALID_DOC = {
    "version": "r-1",
    "defaults": {"rate_per_sec": 7, "burst": 9, "max_concurrency": 11},
    "upstreams": [
        {"name": "u1", "base_url": "http://127.0.0.1:9001"},
        {"name": "u2", "base_url": "http://127.0.0.1:9002", "weight": 3,
         "fail_threshold": 2, "recover_threshold": 2},
    ],
    "routes": [
        {"name": "report", "path_prefix": "/v1/report",
         "upstreams": ["u1"], "strategy": "round_robin",
         "timeout_ms": 500, "max_retries": 1, "cache_ttl_ms": 1500},
        {"name": "v1", "path_prefix": "/v1", "upstreams": ["u2"],
         "strategy": "random", "strip_prefix": "/v1"},
    ],
    "rate_limits": [
        {"caller": "premium", "rate_per_sec": 100, "burst": 100,
         "max_concurrency": 20},
    ],
}


def test_load_valid_file_has_version_and_sha(tmp_path) -> None:
    path = _write(tmp_path, VALID_DOC)
    loaded = load_config_file(path)
    assert loaded.version == "r-1"
    assert len(loaded.content_sha256) == 64
    cfg = loaded.config
    assert isinstance(cfg, GatewayConfig)
    assert [u.name for u in cfg.upstreams] == ["u1", "u2"]
    assert cfg.upstream("u2").weight == 3
    assert cfg.default_rate_per_sec == 7
    report = next(r for r in cfg.routes if r.name == "report")
    assert report.timeout_ms == 500 and report.cache_ttl_ms == 1500
    v1 = next(r for r in cfg.routes if r.name == "v1")
    assert v1.strategy.value == "random" and v1.strip_prefix == "/v1"
    assert cfg.rate_limit_for("premium").max_concurrency == 20
    # Callers without an explicit quota fall back to defaults.
    assert cfg.rate_limit_for("other").rate_per_sec == 7


def test_missing_file_is_rejected_with_path(tmp_path) -> None:
    with pytest.raises(ConfigError) as exc:
        load_config_file(str(tmp_path / "absent.json"))
    assert "absent.json" in exc.value.reasons[0]


def test_invalid_json_reports_position(tmp_path) -> None:
    path = tmp_path / "bad.json"
    path.write_text('{"routes": [}\n', encoding="utf-8")
    with pytest.raises(ConfigError) as exc:
        load_config_file(str(path))
    assert "invalid JSON" in exc.value.reasons[0]
    assert "line 1" in exc.value.reasons[0]


def test_top_level_must_be_object(tmp_path) -> None:
    path = tmp_path / "list.json"
    path.write_text("[]", encoding="utf-8")
    with pytest.raises(ConfigError, match="top level"):
        load_config_file(str(path))


def test_route_referencing_unknown_upstream_rejected(tmp_path) -> None:
    doc = json.loads(json.dumps(VALID_DOC))
    doc["routes"].append({"name": "ghost", "path_prefix": "/g",
                          "upstreams": ["u1", "no-such"]})
    path = _write(tmp_path, doc)
    with pytest.raises(ConfigError) as exc:
        load_config_file(path)
    assert any("unknown upstream 'no-such'" in r for r in exc.value.reasons)


def test_illegal_quota_values_rejected(tmp_path) -> None:
    doc = json.loads(json.dumps(VALID_DOC))
    doc["rate_limits"] = [
        {"caller": "bad", "rate_per_sec": -1, "burst": 0,
         "max_concurrency": "ten"},
    ]
    path = _write(tmp_path, doc)
    with pytest.raises(ConfigError) as exc:
        load_config_file(path)
    joined = " | ".join(exc.value.reasons)
    assert "rate_per_sec" in joined and "burst" in joined
    assert "max_concurrency" in joined


def test_duplicate_names_and_prefixes_rejected(tmp_path) -> None:
    doc = {
        "upstreams": [
            {"name": "u1", "base_url": "http://127.0.0.1:1"},
            {"name": "u1", "base_url": "http://127.0.0.1:2"},
        ],
        "routes": [
            {"name": "r", "path_prefix": "/x", "upstreams": ["u1"]},
            {"name": "r", "path_prefix": "/y", "upstreams": ["u1"]},
            {"name": "r2", "path_prefix": "/x", "upstreams": ["u1"]},
        ],
    }
    with pytest.raises(ConfigError) as exc:
        build_config(doc)
    joined = " | ".join(exc.value.reasons)
    assert "duplicate upstream name 'u1'" in joined
    assert "duplicate route name 'r'" in joined
    assert "duplicate prefix '/x'" in joined


def test_route_requires_nonempty_upstreams_and_absolute_prefix(tmp_path) -> None:
    doc = {
        "upstreams": [{"name": "u1", "base_url": "http://127.0.0.1:1"}],
        "routes": [
            {"name": "empty", "path_prefix": "/e", "upstreams": []},
            {"name": "relative", "path_prefix": "e", "upstreams": ["u1"]},
        ],
    }
    with pytest.raises(ConfigError) as exc:
        build_config(doc)
    joined = " | ".join(exc.value.reasons)
    assert "non-empty array" in joined
    assert "starting with '/'" in joined


def test_unknown_and_bad_strategy_fields_rejected(tmp_path) -> None:
    doc = {
        "upstreams": [{"name": "u1", "base_url": "http://127.0.0.1:1"}],
        "routes": [{"name": "r", "path_prefix": "/r",
                    "upstreams": ["u1"], "strategy": "weird"}],
        "mystery": 42,
    }
    with pytest.raises(ConfigError) as exc:
        build_config(doc)
    joined = " | ".join(exc.value.reasons)
    assert "strategy" in joined
    assert "$.mystery" in joined


def test_duplicate_upstream_listing_rejected(tmp_path) -> None:
    doc = {
        "upstreams": [{"name": "u1", "base_url": "http://127.0.0.1:1"}],
        "routes": [{"name": "r", "path_prefix": "/r",
                    "upstreams": ["u1", "u1"]}],
    }
    with pytest.raises(ConfigError) as exc:
        build_config(doc)
    assert any("listed more than once" in r for r in exc.value.reasons)


def test_segment_boundary_matching() -> None:
    """The exact bug class: /v1/report must not swallow /v1/reporter."""
    assert prefix_matches("/v1/report", "/v1/report")
    assert prefix_matches("/v1/report/2026", "/v1/report")
    assert prefix_matches("/v1/report/", "/v1/report")
    assert not prefix_matches("/v1/reporter", "/v1/report")
    assert not prefix_matches("/v1/reporters/list", "/v1/report")
    # Directory-style prefixes still match their children.
    assert prefix_matches("/api/echo/x", "/api/")
    assert not prefix_matches("/apix", "/api/")
    # Root matches everything.
    assert prefix_matches("/anything", "/")


def test_longest_specific_prefix_wins(tmp_path) -> None:
    path = _write(tmp_path, VALID_DOC)
    cfg = load_config_file(path).config
    assert cfg.route_for("/v1/report/1").name == "report"
    assert cfg.route_for("/v1/report").name == "report"
    # Sibling resource falls through to the wider /v1 rule.
    assert cfg.route_for("/v1/reporter").name == "v1"
    assert cfg.route_for("/v1/other").name == "v1"
    # No matching prefix at all.
    assert cfg.route_for("/v2/thing") is None
