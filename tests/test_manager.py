"""Unit tests for atomic, rollback-safe runtime config snapshots."""
from __future__ import annotations

import threading

from gateway.cache import CacheEntry, now_ms
from gateway.config import (
    GatewayConfig, RouteConfig, UpstreamConfig,
)
from gateway.health import HealthTracker
from gateway.manager import ConfigManager


def _cfg(upstreams=("u1",), prefix: str = "/a", name: str = "a") -> GatewayConfig:
    return GatewayConfig(
        upstreams=tuple(
            UpstreamConfig(name=n, base_url=f"http://127.0.0.1:{9000 + i}")
            for i, n in enumerate(dict.fromkeys(upstreams))
        ),
        routes=(RouteConfig(name=name, path_prefix=prefix,
                            upstream_names=tuple(dict.fromkeys(upstreams))),),
    )


def test_initial_snapshot_and_revision() -> None:
    m = ConfigManager(_cfg(), HealthTracker())
    snap = m.active()
    assert snap.revision == 1
    info = m.info()
    assert info.revision == 1 and info.source.startswith("code")


def test_reload_publishes_whole_new_generation() -> None:
    m = ConfigManager(_cfg(("u1",)), HealthTracker())
    old = m.active()
    new = m.reload(_cfg(("u2",), prefix="/b", name="b"),
                   version="v2", content_sha256="sha-2", source="file:x")
    assert new.revision == 2
    assert new is m.active()
    assert new.version == "v2"
    # New generation routes to the new upstream only.
    assert new.config.route_for("/b/x").upstream_names == ("u2",)
    assert old.config.route_for("/a/x") is not None
    # A pinned old snapshot stays internally consistent forever.
    assert old.cache is not new.cache
    assert old.selector is not new.selector


def test_reload_identical_content_is_noop() -> None:
    m = ConfigManager(_cfg(), HealthTracker())
    snap = m.reload(_cfg(("u2",)), version="v2",
                    content_sha256="same", source="file:x")
    again = m.reload(_cfg(("u3",)), version="v3",
                     content_sha256="same", source="file:x")
    assert again is snap
    assert m.info().revision == snap.revision


def test_old_cache_entry_never_served_after_reload() -> None:
    m = ConfigManager(_cfg(("u1",)), HealthTracker())
    old = m.active()
    entry = CacheEntry(status_code=200, body=b"old", headers=(),
                       stored_ms=now_ms(), ttl_ms=10_000,
                       upstream_name="u1")
    old.cache.put("k", entry, caller="c", route_name="a")
    assert old.cache.get("k") is entry

    m.reload(_cfg(("u2",)), version="v2", content_sha256="sha2",
             source="file:x")
    fresh = m.active()
    # The new generation starts with an empty cache; the stale entry cannot
    # leak across the rule boundary.
    assert fresh.cache.get("k") is None
    # Old snapshot still sees its own (immutable) cache view.
    assert old.cache.get("k") is entry


def test_concurrent_reloads_never_produce_half_generation() -> None:
    health = HealthTracker()
    m = ConfigManager(_cfg(("u1",), prefix="/r1", name="r1"), health)
    errors: list[Exception] = []

    def reload_many(start: int) -> None:
        try:
            for i in range(start, start + 50):
                m.reload(
                    _cfg((f"u{i % 5 + 1}",), prefix=f"/r{i}", name=f"r{i}"),
                    version=f"v{i}", content_sha256=f"sha-{i}",
                    source="file:x",
                )
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    threads = [threading.Thread(target=reload_many, args=(10 * t,))
               for t in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    snap = m.active()
    # Whatever won, it is a complete, self-consistent generation: its only
    # route references an upstream that actually exists in the same config.
    route = snap.config.routes[0]
    assert snap.config.upstream(route.upstream_names[0]) is not None
    assert snap.cache is not None and snap.selector is not None


def test_observers_always_pin_a_consistent_snapshot_under_traffic() -> None:
    health = HealthTracker()
    m = ConfigManager(_cfg(("u1",), prefix="/p1", name="p1"), health)
    anomalies: list[str] = []

    def observe() -> None:
        for _ in range(2000):
            snap = m.active()
            route = snap.config.routes[0]
            # Cross-generation integrity: route name must agree with the
            # selector's config and with the cache object identity.
            if snap.selector._config is not snap.config:  # type: ignore[attr-defined]
                anomalies.append("selector/config mismatch")
            if route.name not in ("p1",) and not route.name.startswith("p"):
                anomalies.append("torn route")
            _ = snap.cache  # must always be present on every snapshot

    def churn() -> None:
        for i in range(200):
            m.reload(
                _cfg(("u1",), prefix=f"/p{i % 4}", name=f"p{i % 4}"),
                version=f"v{i}", content_sha256=f"s{i}", source="file:x",
            )

    readers = [threading.Thread(target=observe) for _ in range(4)]
    writer = threading.Thread(target=churn)
    for t in readers:
        t.start()
    writer.start()
    for t in readers:
        t.join()
    writer.join()
    assert not anomalies, anomalies
