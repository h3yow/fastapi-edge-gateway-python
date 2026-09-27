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
