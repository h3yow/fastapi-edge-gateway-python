"""Shared fixtures: a real two-node upstream cluster and gateways."""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from gateway.app import build_gateway
from gateway.defaults import build_default_config
from upstream_sim.cluster import start_node, stop_node, UpstreamNode


@pytest.fixture(scope="module")
def cluster() -> dict[str, UpstreamNode]:
    nodes = [start_node("u1"), start_node("u2")]
    yield {n.name: n for n in nodes}
    for n in nodes:
        stop_node(n)


@pytest.fixture()
def client(cluster: dict[str, UpstreamNode]):
    for node in cluster.values():
        node.post("/__admin/resume")
        node.post("/__admin/reset")
    config = build_default_config({n.name: n.base_url for n in cluster.values()})
    gateway = build_gateway(config, probe_interval_ms=200)
    with TestClient(gateway.app) as test_client:
        # Expose internals for assertions that need cache/health directly.
        test_client.gateway = gateway  # type: ignore[attr-defined]
        yield test_client


@pytest.fixture()
def client_short_cache(cluster: dict[str, UpstreamNode]):
    """Gateway whose echo cache TTL is only 100ms for expiry tests."""
    for node in cluster.values():
        node.post("/__admin/resume")
        node.post("/__admin/reset")
    config = build_default_config({n.name: n.base_url for n in cluster.values()},
                                  echo_cache_ttl_ms=100)
    gateway = build_gateway(config, probe_interval_ms=200)
    with TestClient(gateway.app) as test_client:
        yield test_client


def write_config(tmp_path, name: str, body: dict) -> str:
    path = tmp_path / name
    path.write_text(__import__("json").dumps(body), encoding="utf-8")
    return str(path)


def base_config_dict(cluster: dict[str, UpstreamNode]) -> dict:
    """File-driven config equivalent to the built-in demo defaults."""
    return {
        "version": "cfg-v1",
        "upstreams": [
            {"name": n.name, "base_url": n.base_url,
             "fail_threshold": 1, "recover_threshold": 1}
            for n in cluster.values()
        ],
        "routes": [
            {"name": "echo", "path_prefix": "/api/",
             "upstreams": list(cluster), "strategy": "round_robin",
             "strip_prefix": "/api", "cache_ttl_ms": 2000,
             "timeout_ms": 800, "max_retries": 2},
            {"name": "effects", "path_prefix": "/effects",
             "upstreams": list(cluster), "strategy": "round_robin",
             "strip_prefix": "/effects", "cache_ttl_ms": 0,
             "timeout_ms": 800, "max_retries": 2},
            {"name": "slow", "path_prefix": "/slow",
             "upstreams": list(cluster), "strategy": "round_robin",
             "strip_prefix": "", "cache_ttl_ms": 0,
             "timeout_ms": 300, "max_retries": 1},
            {"name": "hold", "path_prefix": "/hold",
             "upstreams": list(cluster), "strategy": "round_robin",
             "strip_prefix": "", "cache_ttl_ms": 0,
             "timeout_ms": 1500, "max_retries": 0},
            {"name": "fail", "path_prefix": "/fail",
             "upstreams": list(cluster), "strategy": "round_robin",
             "strip_prefix": "", "cache_ttl_ms": 0,
             "timeout_ms": 800, "max_retries": 2},
        ],
        "rate_limits": [
            {"caller": "premium", "rate_per_sec": 100, "burst": 100,
             "max_concurrency": 20},
            {"caller": "burst-demo", "rate_per_sec": 2, "burst": 5,
             "max_concurrency": 3},
        ],
        "defaults": {"rate_per_sec": 20, "burst": 20, "max_concurrency": 10},
    }


@pytest.fixture()
def file_client(tmp_path, cluster: dict[str, UpstreamNode]):
    """Gateway started from an editable config file (reload enabled)."""
    import json
    for node in cluster.values():
        node.post("/__admin/resume")
        node.post("/__admin/reset")
    cfg_path = tmp_path / "gateway.json"
    cfg_path.write_text(json.dumps(base_config_dict(cluster)),
                        encoding="utf-8")
    from gateway.configfile import load_config_file
    loaded = load_config_file(str(cfg_path))
    gateway = build_gateway(
        loaded.config,
        version=loaded.version,
        config_path=loaded.source_path,
        content_hash=loaded.content_hash,
        loaded_at_ms=loaded.loaded_at_ms,
        probe_interval_ms=200,
    )
    with TestClient(gateway.app) as test_client:
        test_client.gateway = gateway  # type: ignore[attr-defined]
        test_client.config_path = cfg_path  # type: ignore[attr-defined]
        yield test_client
