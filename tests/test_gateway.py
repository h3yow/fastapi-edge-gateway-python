"""End-to-end gateway tests against a real local upstream cluster.

Decision logs (upstream selection / rate-limit verdict / cache decision) are
printed by the gateway on stderr; run `pytest -s` to see them live.
"""
from __future__ import annotations

import threading
import time

import httpx
import pytest

CALLER = {"premium": {"X-Caller-Id": "premium"},
          "burst": {"X-Caller-Id": "burst-demo"}}


def test_route_distribution_round_robin(client) -> None:
    """Same route distributes requests across upstreams round-robin."""
    nodes = []
    for i in range(4):
        r = client.get(f"/api/echo/dist-{i}", headers=CALLER["premium"])
        assert r.status_code == 200
        nodes.append(r.headers["X-Upstream"])
    assert set(nodes) == {"u1", "u2"}, nodes
    assert nodes[0] != nodes[1]  # round-robin alternates


def test_route_missing_is_rejected(client) -> None:
    r = client.get("/nowhere/thing", headers=CALLER["premium"])
    assert r.status_code == 404
    assert r.headers["X-Gateway-Reason"] == "route_not_found"
    assert r.json()["error"] == "route_not_found"


def test_all_upstreams_down_rejected_with_reason(client, cluster) -> None:
    """All upstreams unavailable -> explicit 503, distinguishable from 429."""
    for node in cluster.values():
        node.post("/__admin/freeze")
    r = client.get("/api/echo/down", headers=CALLER["premium"])
    assert r.status_code == 503
    reason = r.headers["X-Gateway-Reason"]
    assert reason in ("all_upstreams_unhealthy", "status_503"), reason
    assert r.json()["error"] == "upstream_unavailable"
    # health snapshot reflects removal
    health = client.get("/_gateway/health").json()["upstreams"]
    assert all(healthy is False for healthy in health.values()), health


def test_failover_after_one_upstream_dies(client, cluster) -> None:
    """Freeze u1: requests keep succeeding via u2 and never hit dead u1."""
    cluster["u1"].post("/__admin/freeze")
    served_by = set()
    for i in range(6):
        r = client.get(f"/api/echo/fo-{i}", headers=CALLER["premium"])
        assert r.status_code == 200, r.text
        served_by.add(r.headers["X-Upstream"])
    assert served_by == {"u2"}, served_by


def test_upstream_recovery_rejoins(client, cluster) -> None:
    """A recovered upstream is re-admitted after successful probes."""
    cluster["u1"].post("/__admin/freeze")
    for i in range(3):
        client.get(f"/api/echo/rec-a-{i}", headers=CALLER["premium"])
    assert client.gateway.health.is_healthy("u1") is False
    cluster["u1"].post("/__admin/resume")
    deadline = time.time() + 5
    rejoined = False
    i = 0
    while time.time() < deadline:
        i += 1
        r = client.get(f"/api/echo/rec-b-{i}", headers=CALLER["premium"])
        if r.headers["X-Upstream"] == "u1":
            rejoined = True
            break
        time.sleep(0.25)
    assert rejoined, "u1 never re-entered rotation after recovery"


def test_rate_limit_burst_accurate(client) -> None:
    """burst=5 tokens, rate=2/s: exactly 5 admitted, rest rejected 429."""
    statuses = [
        client.get(f"/api/echo/tok-{i}", headers=CALLER["burst"]).status_code
        for i in range(8)
    ]
    admitted = [s for s in statuses if s == 200]
    limited = [s for s in statuses if s == 429]
    assert len(admitted) == 5, statuses
    assert len(limited) == 3, statuses
    # The 429 must identify the rate-limit cause, not an upstream failure.
    r = client.get("/api/echo/tok-denied", headers=CALLER["burst"])
    assert r.status_code == 429
    assert r.headers["X-Gateway-Reason"] == "rate_limited"
    assert r.json()["error"] == "rate_limited"


def test_concurrency_limit_parallel(client) -> None:
    """max_concurrency=3 for burst-demo; parallel flood admits exactly 3."""
    results: list[int] = []
    reasons: list[str] = []
    lock = threading.Lock()

    def call() -> None:
        r = client.get("/hold?ms=500", headers=CALLER["burst"])
        with lock:
            results.append(r.status_code)
            if r.status_code == 429:
                reasons.append(r.headers["X-Gateway-Reason"])

    threads = [threading.Thread(target=call) for _ in range(10)]
    for t in threads:
        t.start()
        time.sleep(0.02)
    for t in threads:
        t.join()
    assert results.count(200) == 3, results
    assert results.count(429) == 7, results
    assert set(reasons) == {"concurrency_limited"}, reasons


def test_cache_hit_marked_and_key_variants(client) -> None:
    """Second identical GET is HIT; other caller/variant/path are MISS."""
    h = CALLER["premium"]
    r1 = client.get("/api/echo/cached?x=1", headers=h)
    assert r1.headers["X-Cache"] == "MISS"
    r2 = client.get("/api/echo/cached?x=1", headers=h)
    assert r2.headers["X-Cache"] == "HIT"
    assert r2.headers["X-Cache-Key"]
    # Different caller -> different entry.
    r3 = client.get("/api/echo/cached?x=1",
                    headers={"X-Caller-Id": "someone-else"})
    assert r3.headers["X-Cache"] == "MISS"
    # Different query variant -> different entry.
    r4 = client.get("/api/echo/cached?x=2", headers=h)
    assert r4.headers["X-Cache"] == "MISS"
    # Different path -> different entry.
    r5 = client.get("/api/echo/other?x=1", headers=h)
    assert r5.headers["X-Cache"] == "MISS"


def test_cache_expiry(client_short_cache) -> None:
    """Entry expires after TTL; a later request is served upstream (MISS)."""
    r1 = client_short_cache.get("/api/echo/expiring",
                                headers=CALLER["premium"])
    assert r1.headers["X-Cache"] == "MISS"
    r2 = client_short_cache.get("/api/echo/expiring",
                                headers=CALLER["premium"])
    assert r2.headers["X-Cache"] == "HIT"
    time.sleep(0.25)
    r3 = client_short_cache.get("/api/echo/expiring",
                                headers=CALLER["premium"])
    assert r3.headers["X-Cache"] == "MISS"


def test_cache_invalidation(client) -> None:
    r1 = client.get("/api/echo/invalid", headers=CALLER["premium"])
    assert r1.headers["X-Cache"] == "MISS"
    assert client.get("/api/echo/invalid",
                      headers=CALLER["premium"]).headers["X-Cache"] == "HIT"
    removed = client.post(
        "/_gateway/cache/invalidate",
        json={"caller": "premium", "route": "echo"},
    ).json()["invalidated"]
    assert removed >= 1
    assert client.get("/api/echo/invalid",
                      headers=CALLER["premium"]).headers["X-Cache"] == "MISS"


def test_post_not_retried_no_duplicate_side_effect(client, cluster) -> None:
    """POST to /effects/side-effect: even on failover path, exactly one
    side effect per accepted request; non-safe methods are not replayed."""
    r = client.post("/effects/side-effect", headers=CALLER["premium"],
                    json={"order": 1})
    assert r.status_code == 200
    total = sum(n.get("/__admin/state").json()["side_effects"]
                for n in cluster.values())
    assert total == 1, total

    # Freeze u1: the POST must land once on u2, never twice.
    u1_before = cluster["u1"].get("/__admin/state").json()["side_effects"]
    cluster["u1"].post("/__admin/freeze")
    r2 = client.post("/effects/side-effect", headers=CALLER["premium"],
                     json={"order": 2})
    assert r2.status_code == 200
    assert r2.headers["X-Upstream"] == "u2"
    total2 = sum(n.get("/__admin/state").json()["side_effects"]
                 for n in cluster.values())
    assert total2 == 2, total2
    # u1 must not have served this POST (no replay onto a dead node).
    u1_after = cluster["u1"].get("/__admin/state").json()["side_effects"]
    assert u1_after == u1_before


def test_get_retried_across_upstreams_on_failure(client, cluster) -> None:
    """GET is safe: a 503 from one node triggers failover to the other."""
    cluster["u1"].post("/__admin/freeze")
    r = client.get("/api/echo/safe-fo", headers=CALLER["premium"])
    assert r.status_code == 200
    assert r.headers["X-Upstream"] == "u2"


def test_timeout_classified_as_upstream_failure(client, cluster) -> None:
    """Upstream exceeding timeout -> 504 with upstream reason, not 429."""
    r = client.get("/slow?ms=1000", headers=CALLER["premium"])
    assert r.status_code == 504
    assert r.headers["X-Gateway-Reason"] == "ReadTimeout"
    assert r.json()["error"] == "upstream_unavailable"


def test_forced_503_failover_and_terminal_503(client, cluster) -> None:
    """/fail/503 on both nodes: GET fails over then returns terminal 503."""
    r = client.get("/fail/503", headers=CALLER["premium"])
    assert r.status_code == 503
    assert r.headers["X-Gateway-Reason"] == "status_503"
