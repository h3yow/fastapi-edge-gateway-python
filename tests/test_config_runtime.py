"""End-to-end runtime config behavior: reload, rollback, concurrency.

These tests use the file-driven gateway fixture (``file_client``) and a real
two-node upstream cluster. Run ``pytest -s`` to see per-request decision logs
including ``config_version=...``.
"""
from __future__ import annotations

import json
import threading
import time

CALLER = {"X-Caller-Id": "premium"}

from .conftest import base_config_dict  # noqa: E402


def _rewrite(cfg_path, mutate) -> None:
    body = json.loads(cfg_path.read_text(encoding="utf-8"))
    mutate(body)
    cfg_path.write_text(json.dumps(body), encoding="utf-8")


def _reload(file_client, path: str | None = None) -> object:
    payload = {} if path is None else {"path": path}
    return file_client.post("/_gateway/config/reload", json=payload)


def test_startup_version_is_reported(file_client) -> None:
    info = file_client.get("/_gateway/config/version").json()
    assert info["version"] == "cfg-v1"
    assert info["revision"] == 0
    assert info["routes"] and info["upstreams"]


def test_reload_applies_new_version_atomically(file_client) -> None:
    r1 = file_client.get("/api/echo/rl1", headers=CALLER)
    assert r1.status_code == 200
    assert r1.headers["X-Config-Version"] == "cfg-v1"

    # Point /api at u2 only and bump version/quota.
    def mutate(body: dict) -> None:
        body["version"] = "cfg-v2"
        body["routes"][0]["upstreams"] = ["u2"]
        for rl in body["rate_limits"]:
            if rl["caller"] == "premium":
                rl["rate_per_sec"] = 1
                rl["burst"] = 1
                rl["max_concurrency"] = 1
    _rewrite(file_client.config_path, mutate)

    resp = _reload(file_client)
    assert resp.status_code == 200, resp.text
    assert resp.json()["version"] == "cfg-v2"
    assert resp.json()["revision"] == 1

    info = file_client.get("/_gateway/config/version").json()
    assert info["version"] == "cfg-v2" and info["revision"] == 1

    r2 = file_client.get("/api/echo/rl2", headers=CALLER)
    assert r2.status_code == 200
    assert r2.headers["X-Upstream"] == "u2"
    assert r2.headers["X-Config-Version"] == "cfg-v2"


def test_invalid_reload_is_rejected_and_old_rules_keep_serving(file_client) -> None:
    # Prime a cache entry under cfg-v1.
    assert file_client.get("/api/echo/keep", headers=CALLER
                           ).headers["X-Cache"] == "MISS"
    assert file_client.get("/api/echo/keep", headers=CALLER
                           ).headers["X-Cache"] == "HIT"

    # Write an invalid file (route references an upstream that does not exist).
    def mutate(body: dict) -> None:
        body["version"] = "cfg-bad"
        body["routes"][0]["upstreams"] = ["does-not-exist"]
    _rewrite(file_client.config_path, mutate)

    resp = _reload(file_client)
    assert resp.status_code == 400, resp.text
    payload = resp.json()
    assert payload["error"] == "invalid_config"
    assert payload["active_version"] == "cfg-v1"
    assert any("does-not-exist" in reason for reason in payload["reasons"])
    # The response itself was decided under the still-active old version.
    assert resp.headers["X-Config-Version"] == "cfg-v1"

    # Active version is untouched...
    info = file_client.get("/_gateway/config/version").json()
    assert info["version"] == "cfg-v1"
    # ...routing still works, and the old cache entry still serves (old
    # rules remain fully in force).
    r = file_client.get("/api/echo/keep", headers=CALLER)
    assert r.status_code == 200
    assert r.headers["X-Cache"] == "HIT"
    assert r.headers["X-Config-Version"] == "cfg-v1"

    # A corrected file reloads normally afterwards (rollback by fix).
    def restore(body: dict) -> None:
        body["version"] = "cfg-v3"
        body["routes"][0]["upstreams"] = [u["name"] for u in body["upstreams"]]
    _rewrite(file_client.config_path, restore)
    resp2 = _reload(file_client)
    assert resp2.status_code == 200
    assert resp2.json()["version"] == "cfg-v3"


def test_malformed_json_reload_does_not_replace_config(file_client) -> None:
    file_client.config_path.write_text("{not json", encoding="utf-8")
    resp = _reload(file_client)
    assert resp.status_code == 400
    assert resp.json()["error"] == "invalid_config"
    assert file_client.get("/_gateway/config/version").json()["version"] == "cfg-v1"


def test_adjacent_prefixes_do_not_swallow_each_other(tmp_path, cluster) -> None:
    from fastapi.testclient import TestClient
    from gateway.app import build_gateway
    from gateway.configfile import load_config_file

    body = base_config_dict(cluster)
    body["version"] = "cfg-prefix"
    # /v1/report -> u1 only; /v1/reporter -> u2 only.
    body["routes"] = [
        {"name": "report", "path_prefix": "/v1/report",
         "upstreams": ["u1"], "strategy": "round_robin",
         "strip_prefix": "/v1/report", "cache_ttl_ms": 0,
         "timeout_ms": 800, "max_retries": 0},
        {"name": "reporter", "path_prefix": "/v1/reporter",
         "upstreams": ["u2"], "strategy": "round_robin",
         "strip_prefix": "/v1/reporter", "cache_ttl_ms": 0,
         "timeout_ms": 800, "max_retries": 0},
        {"name": "reports", "path_prefix": "/v1/reports",
         "upstreams": ["u2"], "strip_prefix": "/v1/reports",
         "cache_ttl_ms": 0, "timeout_ms": 800, "max_retries": 0},
    ]
    path = tmp_path / "p.json"
    path.write_text(json.dumps(body), encoding="utf-8")
    loaded = load_config_file(str(path))
    gw = build_gateway(loaded.config, version=loaded.version,
                       config_path=loaded.source_path)
    with TestClient(gw.app) as c:
        # Paths go through the simulator's /echo/<path> endpoint after the
        # route prefix is stripped.
        r1 = c.get("/v1/report/echo/a", headers=CALLER)
        assert r1.status_code == 200 and r1.headers["X-Upstream"] == "u1"
        r2 = c.get("/v1/reporter/echo/b", headers=CALLER)
        assert r2.status_code == 200 and r2.headers["X-Upstream"] == "u2"
        r3 = c.get("/v1/report/echo/details", headers=CALLER)
        assert r3.status_code == 200 and r3.headers["X-Upstream"] == "u1"
        r4 = c.get("/v1/reporter/echo/profile", headers=CALLER)
        assert r4.status_code == 200 and r2.headers["X-Upstream"] == "u2"
        r5 = c.get("/v1/reports/echo/c", headers=CALLER)
        assert r5.status_code == 200 and r5.headers["X-Upstream"] == "u2"


def test_unknown_path_never_forwarded(file_client) -> None:
    r = file_client.get("/v1/reporter", headers=CALLER)
    assert r.status_code == 404
    assert r.headers["X-Gateway-Reason"] == "route_not_found"
    assert r.headers["X-Config-Version"] == "cfg-v1"
    assert "X-Upstream" not in r.headers
    assert r.json()["error"] == "route_not_found"


def test_reload_clears_old_cache_entries(file_client) -> None:
    assert file_client.get("/api/echo/fresh", headers=CALLER
                           ).headers["X-Cache"] == "MISS"
    assert file_client.get("/api/echo/fresh", headers=CALLER
                           ).headers["X-Cache"] == "HIT"
    _rewrite(file_client.config_path, lambda b: b.update(version="cfg-cache"))
    assert _reload(file_client).status_code == 200
    # Same request after reload: a version-private empty cache => MISS.
    r = file_client.get("/api/echo/fresh", headers=CALLER)
    assert r.headers["X-Cache"] == "MISS"
    assert r.headers["X-Config-Version"] == "cfg-cache"


def test_reload_concurrent_with_traffic_uses_consistent_versions(
        file_client, cluster) -> None:
    """Each request reports exactly one config version; routing/cache never
    mix two versions; no request is lost or mis-forwarded."""
    versions: list[str] = []
    statuses: list[int] = []
    errors: list[str] = []
    barrier = threading.Event()
    lock = threading.Lock()

    def worker(worker_id: int) -> None:
        barrier.wait()
        for i in range(20):
            r = file_client.get(f"/api/echo/c{worker_id}-{i}", headers=CALLER)
            with lock:
                statuses.append(r.status_code)
                ver_header = r.headers.get("X-Config-Version", "")
                versions.append(ver_header)
                upstream = r.headers.get("X-Upstream", "")
                if ver_header not in ("cfg-v1", "cfg-reload"):
                    errors.append(f"unexpected version {ver_header!r}")
                if r.status_code == 200 and upstream not in ("u1", "u2"):
                    errors.append(f"bad upstream {upstream!r}")

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(6)]
    for t in threads:
        t.start()

    # Trigger a reload while workers are flying.
    def mutate(body: dict) -> None:
        body["version"] = "cfg-reload"
    barrier.set()
    time.sleep(0.05)
    _rewrite(file_client.config_path, mutate)
    resp = _reload(file_client)
    assert resp.status_code == 200, resp.text
    for t in threads:
        t.join()

    assert len(statuses) == 120
    assert all(s == 200 for s in statuses), statuses
    assert not errors, errors
    assert set(versions) <= {"cfg-v1", "cfg-reload"}
    # The reload actually took effect for at least some requests.
    assert "cfg-reload" in versions


def test_quota_change_after_reload_is_enforced(file_client) -> None:
    # premium starts at burst=100; tighten to burst=1 then verify cap applies.
    def mutate(body: dict) -> None:
        body["version"] = "cfg-tight"
        for rl in body["rate_limits"]:
            if rl["caller"] == "premium":
                rl["rate_per_sec"] = 1
                rl["burst"] = 1
                rl["max_concurrency"] = 50
    _rewrite(file_client.config_path, mutate)
    assert _reload(file_client).status_code == 200
    statuses = [file_client.get(f"/api/echo/q{i}", headers=CALLER).status_code
                for i in range(4)]
    assert statuses.count(200) == 1, statuses
    assert statuses.count(429) == 3, statuses
