"""End-to-end tests for runtime reload, atomicity and segment routing.

These tests drive a real gateway via TestClient against the local upstream
cluster, exercising the local admin endpoints while traffic is in flight.
Decision logs on stderr include ``rev=`` / ``ver=`` so the generation used
by every request is visible (run ``pytest -s``).
"""
from __future__ import annotations

import json
import threading
import time

from fastapi.testclient import TestClient

from gateway.app import build_gateway
from gateway.configfile import load_config_file
from gateway.defaults import build_default_config

PREMIUM = {"X-Caller-Id": "premium"}


def _make_config(tmp_path, cluster: dict, *, version: str, routes: list,
                 rate_limits: list | None = None,
                 defaults: dict | None = None) -> str:
    upstreams = [
        {"name": name, "base_url": node.base_url}
        for name, node in cluster.items()
    ]
    doc = {
        "version": version,
        "defaults": defaults or {"rate_per_sec": 50, "burst": 50,
                                 "max_concurrency": 20},
        "upstreams": upstreams,
        "routes": routes,
        "rate_limits": rate_limits or [
            {"caller": "premium", "rate_per_sec": 100, "burst": 100,
             "max_concurrency": 20},
        ],
    }
    path = tmp_path / f"{version}.json"
    path.write_text(json.dumps(doc), encoding="utf-8")
    return str(path)


def _gateway_for(cluster, tmp_path, *, version="cfg-v1", routes=None,
                 rate_limits=None, defaults=None, cache_ttl=0):
    if routes is None:
        routes = [
            {"name": "echo", "path_prefix": "/api/", "upstreams": ["u1", "u2"],
             "strip_prefix": "/api", "timeout_ms": 800, "max_retries": 2,
             "cache_ttl_ms": cache_ttl},
        ]
    path = _make_config(tmp_path, cluster, version=version, routes=routes,
                        rate_limits=rate_limits, defaults=defaults)
    loaded = load_config_file(path)
    gateway = build_gateway(
        loaded.config, probe_interval_ms=200,
        config_path=path, initial=loaded,
    )
    client = TestClient(gateway.app)
    client.__enter__()
    return client, gateway, path


def _write_config(path: str, doc: dict) -> str:
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(doc, handle)
    return path


def test_startup_from_file_and_version_endpoint(cluster, tmp_path) -> None:
    for node in cluster.values():
        node.post("/__admin/resume"); node.post("/__admin/reset")
    client, gateway, path = _gateway_for(cluster, tmp_path)
    info = client.get("/_gateway/config").json()
    assert info["revision"] == 1
    assert info["version"] == "cfg-v1"
    assert info["source"] == f"file:{path}"
    assert [r["name"] for r in info["routes"]] == ["echo"]
    # Proxied traffic advertises the active generation.
    r = client.get("/api/echo/x", headers=PREMIUM)
    assert r.status_code == 200
    assert r.headers["X-Config-Revision"] == "1"
    assert r.headers["X-Config-Version"] == "cfg-v1"
    client.__exit__(None, None, None)


def test_reload_success_switches_generation_atomically(cluster, tmp_path) -> None:
    for node in cluster.values():
        node.post("/__admin/resume"); node.post("/__admin/reset")
    client, gateway, path = _gateway_for(cluster, tmp_path)
    doc = {
        "version": "cfg-v2",
        "defaults": {"rate_per_sec": 50, "burst": 50, "max_concurrency": 20},
        "upstreams": [
            {"name": n, "base_url": cluster[n].base_url} for n in cluster
        ],
        "routes": [
            # New explicit route; old /api/ prefix is gone in this generation.
            # Strip to /echo so the local upstream simulator can answer it.
            {"name": "only-v1", "path_prefix": "/v1", "upstreams": ["u2"],
             "strip_prefix": "/v1", "timeout_ms": 800, "max_retries": 1,
             "cache_ttl_ms": 0},
        ],
        "rate_limits": [
            {"caller": "premium", "rate_per_sec": 100, "burst": 100,
             "max_concurrency": 20},
        ],
    }
    _write_config(path, doc)
    result = client.post("/_gateway/config/reload").json()
    assert result["rejected"] is False
    assert result["previous_revision"] == 1
    assert result["revision"] == 2
    assert result["version"] == "cfg-v2"

    assert client.get("/_gateway/config").json()["revision"] == 2
    # Old route fully gone; new route fully present — no half state.
    gone = client.get("/api/echo/x", headers=PREMIUM)
    assert gone.status_code == 404
    assert gone.headers["X-Gateway-Reason"] == "route_not_found"
    served = client.get("/v1/echo/now", headers=PREMIUM)
    assert served.status_code == 200
    assert served.headers["X-Upstream"] == "u2"
    assert served.headers["X-Config-Revision"] == "2"
    client.__exit__(None, None, None)


def test_invalid_reload_rejected_old_config_keeps_serving(cluster, tmp_path) -> None:
    for node in cluster.values():
        node.post("/__admin/resume"); node.post("/__admin/reset")
    client, gateway, path = _gateway_for(cluster, tmp_path)
    bad = {
        "version": "broken",
        "upstreams": [{"name": "u1", "base_url": cluster["u1"].base_url}],
        "routes": [
            {"name": "ghost", "path_prefix": "/g",
             "upstreams": ["u1", "u1", "missing"]},
        ],
        "rate_limits": [{"caller": "x", "rate_per_sec": -1, "burst": 0}],
    }
    _write_config(path, bad)
    resp = client.post("/_gateway/config/reload")
    assert resp.status_code == 422
    assert resp.headers["X-Gateway-Reason"] == "config_reload_rejected"
    body = resp.json()
    assert body["active_revision"] == 1
    reasons = " | ".join(body["reasons"])
    assert "unknown upstream 'missing'" in reasons
    assert "listed more than once" in reasons
    assert "rate_per_sec" in reasons

    # Old generation (revision 1) still serves exactly as before the attempt.
    assert client.get("/_gateway/config").json()["revision"] == 1
    ok = client.get("/api/echo/still-here", headers=PREMIUM)
    assert ok.status_code == 200
    assert ok.headers["X-Config-Revision"] == "1"
    client.__exit__(None, None, None)


def test_sibling_prefixes_do_not_swallow_each_other(cluster, tmp_path) -> None:
    for node in cluster.values():
        node.post("/__admin/resume"); node.post("/__admin/reset")
    routes = [
        {"name": "report", "path_prefix": "/echo/report",
         "upstreams": ["u1"],
         "timeout_ms": 800, "max_retries": 1, "cache_ttl_ms": 0},
        {"name": "echo", "path_prefix": "/echo", "upstreams": ["u2"],
         "timeout_ms": 800, "max_retries": 1, "cache_ttl_ms": 0},
    ]
    client, gateway, path = _gateway_for(cluster, tmp_path, version="seg",
                                         routes=routes)
    # Resource and child hit the more specific route -> only u1.
    specific = client.get("/echo/report/x", headers=PREMIUM)
    assert specific.status_code == 200
    assert specific.headers["X-Upstream"] == "u1"
    child = client.get("/echo/report/2026/summary", headers=PREMIUM)
    assert child.status_code == 200 and child.headers["X-Upstream"] == "u1"
    # Adjacent-but-different resource must NOT be swallowed: falls to /echo/u2.
    sibling = client.get("/echo/reporter/x", headers=PREMIUM)
    assert sibling.status_code == 200
    assert sibling.headers["X-Upstream"] == "u2", sibling.text
    other = client.get("/echo/anything", headers=PREMIUM)
    assert other.headers["X-Upstream"] == "u2"
    # Truly unknown path: explicit rejection, never forwarded.
    nope = client.get("/v2/nope", headers=PREMIUM)
    assert nope.status_code == 404
    assert nope.headers["X-Gateway-Reason"] == "route_not_found"
    client.__exit__(None, None, None)


def test_quota_change_takes_effect_and_old_counters_keep_working(
        cluster, tmp_path) -> None:
    for node in cluster.values():
        node.post("/__admin/resume"); node.post("/__admin/reset")
    generous = [{"caller": "c", "rate_per_sec": 1000, "burst": 1000,
                 "max_concurrency": 1000}]
    client, gateway, path = _gateway_for(cluster, tmp_path, version="quota-1",
                                         rate_limits=generous)
    assert client.get("/api/echo/a", headers={"X-Caller-Id": "c"}).status_code == 200

    doc = json.loads(open(path, encoding="utf-8").read())
    doc["version"] = "quota-2"
    doc["rate_limits"] = [{"caller": "c", "rate_per_sec": 1, "burst": 1,
                           "max_concurrency": 1}]
    _write_config(path, doc)
    result = client.post("/_gateway/config/reload").json()
    assert result["rejected"] is False and result["version"] == "quota-2"

    statuses = [
        client.get(f"/api/echo/b{i}", headers={"X-Caller-Id": "c"}).status_code
        for i in range(4)
    ]
    # New tight quota governs immediately: first passes (token may already
    # have accrued at most 1), the rest are rate-limited, never over-admitted.
    assert statuses.count(200) <= 1, statuses
    assert any(s == 429 for s in statuses), statuses
    limited = next(s for s in [
        client.get("/api/echo/z", headers={"X-Caller-Id": "c"})
    ] if s.status_code == 429)
    # premium caller unaffected
    assert client.get("/api/echo/p", headers=PREMIUM).status_code == 200
    client.__exit__(None, None, None)


def test_cache_isolated_across_reload_no_stale_hit(cluster, tmp_path) -> None:
    for node in cluster.values():
        node.post("/__admin/resume"); node.post("/__admin/reset")
    client, gateway, path = _gateway_for(cluster, tmp_path, version="cache-1",
                                         cache_ttl=5000)
    r1 = client.get("/api/echo/hot", headers=PREMIUM)
    assert r1.headers["X-Cache"] == "MISS"
    r2 = client.get("/api/echo/hot", headers=PREMIUM)
    assert r2.headers["X-Cache"] == "HIT"

    doc = json.loads(open(path, encoding="utf-8").read())
    doc["version"] = "cache-2"
    doc["routes"][0]["cache_ttl_ms"] = 5000
    _write_config(path, doc)
    assert client.post("/_gateway/config/reload").json()["revision"] == 2

    # New generation starts cold: the old cached body cannot be reused.
    r3 = client.get("/api/echo/hot", headers=PREMIUM)
    assert r3.headers["X-Cache"] == "MISS"
    assert r3.headers["X-Config-Revision"] == "2"
    r4 = client.get("/api/echo/hot", headers=PREMIUM)
    assert r4.headers["X-Cache"] == "HIT"
    client.__exit__(None, None, None)


def test_reload_interleaved_with_traffic_judgements_stay_consistent(
        cluster, tmp_path) -> None:
    """Reload repeatedly while parallel traffic flows: every response must
    carry a coherent revision and a valid, non-torn verdict."""
    for node in cluster.values():
        node.post("/__admin/resume"); node.post("/__admin/reset")
    routes_v1 = [
        {"name": "echo", "path_prefix": "/api/", "upstreams": ["u1", "u2"],
         "strip_prefix": "/api", "timeout_ms": 800, "max_retries": 2,
         "cache_ttl_ms": 0},
    ]
    client, gateway, path = _gateway_for(cluster, tmp_path, version="traffic-1",
                                         routes=routes_v1)

    docs = []
    for i in range(2, 8):
        docs.append({
            "version": f"traffic-{i}",
            "defaults": {"rate_per_sec": 100, "burst": 100,
                         "max_concurrency": 30},
            "upstreams": [
                {"name": n, "base_url": cluster[n].base_url} for n in cluster
            ],
            # Alternate the route set so the /api/ path appears and disappears
            # across generations.
            "routes": routes_v1 if i % 2 == 0 else [
                {"name": "other", "path_prefix": "/other",
                 "upstreams": ["u1", "u2"], "strip_prefix": "/other",
                 "timeout_ms": 800, "max_retries": 2, "cache_ttl_ms": 0},
            ],
            "rate_limits": [
                {"caller": "premium", "rate_per_sec": 100, "burst": 100,
                 "max_concurrency": 30},
            ],
        })

    verdicts: list[tuple[int, str, str]] = []
    vlock = threading.Lock()
    stop = threading.Event()

    def traffic() -> None:
        i = 0
        while not stop.is_set():
            i += 1
            r = client.get(f"/api/echo/t{i}", headers=PREMIUM)
            with vlock:
                verdicts.append((
                    r.status_code,
                    r.headers.get("X-Config-Revision", "?"),
                    r.headers.get("X-Gateway-Reason", ""),
                ))

    def reloads() -> None:
        for doc in docs:
            _write_config(path, doc)
            client.post("/_gateway/config/reload")
            time.sleep(0.005)

    workers = [threading.Thread(target=traffic) for _ in range(6)]
    for w in workers:
        w.start()
    writer = threading.Thread(target=reloads)
    writer.start()
    writer.join()
    time.sleep(0.05)
    stop.set()
    for w in workers:
        w.join()

    assert verdicts, "no traffic was observed"
    # In odd generations /api/ is absent (404 route_not_found); in even ones
    # it proxies (200). Whatever the verdict, it must be one of those two
    # coherent outcomes and always carry the revision that produced it.
    for status, rev, reason in verdicts:
        assert status in (200, 404), (status, rev, reason)
        assert rev != "?", "response missing config revision"
        if status == 404:
            assert reason == "route_not_found", reason
        else:
            assert reason == "", reason
    # At least both generations were observed by traffic.
    revisions = {rev for _, rev, _ in verdicts}
    assert len(revisions) >= 2, revisions
    client.__exit__(None, None, None)


def test_decision_logs_include_config_revision(cluster, tmp_path,
                                               caplog) -> None:
    import logging
    for node in cluster.values():
        node.post("/__admin/resume"); node.post("/__admin/reset")
    client, gateway, path = _gateway_for(cluster, tmp_path, version="log-v1")
    with caplog.at_level(logging.INFO, logger="edge_gateway"):
        r = client.get("/api/echo/logged", headers=PREMIUM)
    assert r.status_code == 200
    decisions = [record.getMessage() for record in caplog.records
                 if "decision=" in record.getMessage()]
    joined = "\n".join(decisions)
    assert "rev=1" in joined and "ver=log-v1" in joined
    assert "decision=route_lookup" in joined
    client.__exit__(None, None, None)


def test_concurrency_quota_reload_during_flood_never_leaks_or_overadmits(
        cluster, tmp_path) -> None:
    """A flood against a tight concurrency cap while the quota is reloaded
    between 3 and 8 must never over-admit beyond either cap, must never lose
    a release (in-flight returns to zero), and every verdict is coherent."""
    for node in cluster.values():
        node.post("/__admin/resume"); node.post("/__admin/reset")
    routes = [
        {"name": "hold", "path_prefix": "/hold", "upstreams": ["u1", "u2"],
         "strip_prefix": "", "timeout_ms": 1500, "max_retries": 0,
         "cache_ttl_ms": 0},
    ]
    quotas = [{"caller": "cap", "rate_per_sec": 1_000_000,
               "burst": 1_000_000, "max_concurrency": 3}]
    client, gateway, path = _gateway_for(cluster, tmp_path, version="cap-3",
                                         routes=routes, rate_limits=quotas,
                                         defaults={"rate_per_sec": 1_000_000,
                                                   "burst": 1_000_000,
                                                   "max_concurrency": 1_000_000})

    statuses: list[int] = []
    in_flight_seen: list[int] = []
    lock = threading.Lock()
    stop = threading.Event()

    def call(i: int) -> None:
        r = client.get(f"/hold?ms=250", headers={"X-Caller-Id": "cap"})
        with lock:
            statuses.append(r.status_code)
            bucket = gateway.limiter._buckets.get("cap")  # type: ignore[attr-defined]
            if bucket is not None:
                in_flight_seen.append(bucket.in_flight)

    def churn_quota() -> None:
        for gen in range(4, 12):
            doc = json.loads(open(path, encoding="utf-8").read())
            doc["version"] = f"cap-{gen}"
            cap = 3 if gen % 2 == 0 else 8
            doc["rate_limits"] = [{"caller": "cap", "rate_per_sec": 1_000_000,
                                   "burst": 1_000_000,
                                   "max_concurrency": cap}]
            _write_config(path, doc)
            client.post("/_gateway/config/reload")
            time.sleep(0.03)

    threads = [threading.Thread(target=call, args=(i,)) for i in range(24)]
    writer = threading.Thread(target=churn_quota)
    for t in threads:
        t.start()
        time.sleep(0.01)
    writer.start()
    for t in threads:
        t.join()
    writer.join()

    assert set(statuses) <= {200, 429}, statuses
    # At least some requests were admitted and some throttled by the cap.
    assert 200 in statuses and 429 in statuses, statuses
    # Every acquire was matched by exactly one release: no leak / double count.
    bucket = gateway.limiter._buckets["cap"]  # type: ignore[attr-defined]
    assert bucket.in_flight == 0, bucket.in_flight
    # While requests were finishing, in-flight never exceeded the largest cap.
    assert max(in_flight_seen) <= 8, in_flight_seen
    # Revisions advanced through several quota generations.
    assert client.get("/_gateway/config").json()["revision"] >= 5
    client.__exit__(None, None, None)
