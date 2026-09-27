"""响应缓存的命中、失效与键隔离测试。

日志中可看到：
- "cache key ..." 缓存键构成（调用方/方法/路径/查询/请求体）；
- "cache decision=HIT/MISS/STORE/EXPIRED ..." 缓存决策依据。
响应头 X-Gateway-Cache=HIT/MISS 给出可解释标记。
"""
from __future__ import annotations

import asyncio

import httpx
import pytest

from tests.conftest import make_route

pytestmark = pytest.mark.asyncio


async def test_cache_hit_marker_and_no_upstream_traffic(upstream_factory, gateway_factory):
    u1 = upstream_factory("cache-1")
    client = await gateway_factory(
        [make_route("/api/p", [u1], cache_enabled=True, cache_ttl=60)]
    )

    r1 = await client.get("/api/p/sku/42", headers={"x-caller-id": "alice"})
    assert r1.status_code == 200
    assert r1.headers["x-gateway-cache"] == "MISS"
    seq1 = r1.json()["seq"]

    r2 = await client.get("/api/p/sku/42", headers={"x-caller-id": "alice"})
    assert r2.headers["x-gateway-cache"] == "HIT"
    assert r2.headers["x-gateway-upstream"] == "cache"
    assert r2.json()["seq"] == seq1  # 命中缓存，未到达上游

    async with httpx.AsyncClient() as diag:
        state = (await diag.get(f"{u1.base_url}/__diag/state")).json()
    assert state["requests_total"] == 1  # 只真正转发了一次


async def test_cache_expires_after_ttl(upstream_factory, gateway_factory):
    u1 = upstream_factory("cache-1")
    client = await gateway_factory(
        [make_route("/api/p", [u1], cache_enabled=True, cache_ttl=0.3)]
    )

    r1 = await client.get("/api/p/ttl", headers={"x-caller-id": "alice"})
    assert r1.headers["x-gateway-cache"] == "MISS"
    assert (
        await client.get("/api/p/ttl", headers={"x-caller-id": "alice"})
    ).headers["x-gateway-cache"] == "HIT"

    await asyncio.sleep(0.4)  # 等待过期
    r3 = await client.get("/api/p/ttl", headers={"x-caller-id": "alice"})
    assert r3.headers["x-gateway-cache"] == "MISS"
    assert r3.json()["seq"] != r1.json()["seq"]


async def test_cache_isolated_by_caller(upstream_factory, gateway_factory):
    u1 = upstream_factory("cache-1")
    client = await gateway_factory([make_route("/api/p", [u1])])

    r_alice = await client.get("/api/p/private", headers={"x-caller-id": "alice"})
    assert r_alice.headers["x-gateway-cache"] == "MISS"

    # bob 不能命中 alice 的缓存
    r_bob = await client.get("/api/p/private", headers={"x-caller-id": "bob"})
    assert r_bob.headers["x-gateway-cache"] == "MISS"
    assert r_bob.json()["seq"] != r_alice.json()["seq"]

    # 再次以 alice 请求 -> 命中
    assert (
        await client.get("/api/p/private", headers={"x-caller-id": "alice"})
    ).headers["x-gateway-cache"] == "HIT"


async def test_cache_key_distinguishes_query_variants(upstream_factory, gateway_factory):
    u1 = upstream_factory("cache-1")
    client = await gateway_factory([make_route("/api/p", [u1])])

    r1 = await client.get(
        "/api/p/search?q=phone&sort=asc", headers={"x-caller-id": "alice"}
    )
    assert r1.headers["x-gateway-cache"] == "MISS"
    assert (
        await client.get("/api/p/search?q=phone&sort=asc", headers={"x-caller-id": "alice"})
    ).headers["x-gateway-cache"] == "HIT"
    # 查询串不同 -> 不同变体，不命中
    r3 = await client.get(
        "/api/p/search?q=phone&sort=desc", headers={"x-caller-id": "alice"}
    )
    assert r3.headers["x-gateway-cache"] == "MISS"
    # query 参数值不同
    assert (
        await client.get("/api/p/search?q=laptop&sort=asc", headers={"x-caller-id": "alice"})
    ).headers["x-gateway-cache"] == "MISS"


async def test_post_not_cached(upstream_factory, gateway_factory):
    u1 = upstream_factory("cache-1")
    client = await gateway_factory(
        [make_route("/api/p", [u1], max_attempts=1)]
    )

    r1 = await client.post(
        "/api/p/write", content=b'{"v":1}', headers={"x-caller-id": "alice"}
    )
    r2 = await client.post(
        "/api/p/write", content=b'{"v":1}', headers={"x-caller-id": "alice"}
    )
    assert r1.status_code == 201 and r2.status_code == 201
    assert r1.headers["x-gateway-cache"] == "MISS"
    assert r2.headers["x-gateway-cache"] == "MISS"
    assert r2.json()["side_effect_count"] == 2  # 每次都真正转发
    assert r2.headers["x-gateway-upstream"] == "cache-1"


async def test_error_responses_not_cached(upstream_factory, gateway_factory):
    u1 = upstream_factory("cache-1")
    client = await gateway_factory(
        [make_route("/api/p", [u1], max_attempts=1, health_cooldown=0.0)]
    )

    r1 = await client.get("/api/p/cluster-fail", headers={"x-caller-id": "alice"})
    assert r1.status_code == 503
    r2 = await client.get("/api/p/cluster-fail", headers={"x-caller-id": "alice"})
    assert r2.status_code == 503
    # 5xx 不被缓存：两次都是真正打到上游（恢复探测），而不是缓存命中
    assert "x-gateway-cache" not in r2.headers or r2.headers.get("x-gateway-cache") != "HIT"


async def test_disabled_cache_always_miss(upstream_factory, gateway_factory):
    u1 = upstream_factory("cache-1")
    client = await gateway_factory(
        [make_route("/api/p", [u1], cache_enabled=False)]
    )
    for _ in range(3):
        r = await client.get("/api/p/nocache", headers={"x-caller-id": "alice"})
        assert r.headers["x-gateway-cache"] == "MISS"
        assert r.headers["x-gateway-upstream"] == "cache-1"
