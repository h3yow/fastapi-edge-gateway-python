"""按调用方维度的令牌桶限流与并发约束测试。

日志中可看到：
- "rate-limit caller=... decision=ALLOW/REJECT remaining=..."；
- "concurrency caller=... decision=ALLOW/REJECT inflight=N/M"。

重点结论：
- 突发并发下限流计数准确（不多放不漏放）；
- 429 与上游 5xx 原因可区分（error=rate_limited / concurrency_limited）；
- 不同调用方的桶互不影响；
- 并发槽在请求结束后归还，不会多计或漏计。
"""
from __future__ import annotations

import asyncio

import pytest

from tests.conftest import make_route

pytestmark = pytest.mark.asyncio


async def test_burst_rate_limit_exact_count(upstream_factory, gateway_factory):
    u1 = upstream_factory("rl-1")
    client = await gateway_factory(
        [make_route(
            "/api/r", [u1],
            rate_per_second=0.0, burst=5, max_concurrency=100,
            cache_enabled=False,
        )]
    )

    # 满桶 5 个令牌：前 5 个 200，第 6 个立即 429（零补充速率，突发计数必须准确）
    results = await asyncio.gather(*[
        client.get("/api/r/item", headers={"x-caller-id": "alice"})
        for _ in range(12)
    ])
    ok = sum(r.status_code == 200 for r in results)
    limited = [r for r in results if r.status_code == 429]
    assert ok == 5, ok
    assert len(limited) == 7
    data = limited[0].json()
    assert data["error"] == "rate_limited"
    assert limited[0].headers["x-gateway-reason"] == "rate_limited"


async def test_token_refill_over_time(upstream_factory, gateway_factory):
    u1 = upstream_factory("rl-1")
    client = await gateway_factory(
        [make_route(
            "/api/r", [u1],
            rate_per_second=20.0, burst=2, max_concurrency=100,
            cache_enabled=False,
        )]
    )

    async def req():
        return (
            await client.get("/api/r/item", headers={"x-caller-id": "bob"})
        ).status_code

    assert [await req() for _ in range(2)] == [200, 200]
    assert await req() == 429
    # 20/s：0.15s 后补充约 3 个令牌，又可通过
    await asyncio.sleep(0.15)
    assert await req() == 200


async def test_callers_have_independent_buckets(upstream_factory, gateway_factory):
    u1 = upstream_factory("rl-1")
    client = await gateway_factory(
        [make_route(
            "/api/r", [u1],
            rate_per_second=0.0, burst=2, max_concurrency=100,
            cache_enabled=False,
        )]
    )

    for caller in ("alice", "bob", "carol"):
        codes = [
            (await client.get("/api/r/item", headers={"x-caller-id": caller})).status_code
            for _ in range(3)
        ]
        assert codes == [200, 200, 429], (caller, codes)

    # 匿名调用方有自己的桶
    codes = [
        (await client.get("/api/r/item")).status_code for _ in range(3)
    ]
    assert codes == [200, 200, 429]


async def test_concurrency_limit_distinct_from_rate_limit(upstream_factory, gateway_factory):
    u1 = upstream_factory("hold-1")
    client = await gateway_factory(
        [make_route(
            "/api/c", [u1],
            rate_per_second=1000.0, burst=1000, max_concurrency=2,
            cache_enabled=False,
        )]
    )

    # 5 个并发请求同时打向需要 0.3s 的端点：恰好 2 个放行，其余 concurrency_limited
    results = await asyncio.gather(*[
        client.get("/api/c/job?hold=0.3", headers={"x-caller-id": "dave"})
        for _ in range(5)
    ])
    codes = sorted(r.status_code for r in results)
    assert codes == [200, 200, 429, 429, 429], codes
    rejected = next(r for r in results if r.status_code == 429)
    assert rejected.json()["error"] == "concurrency_limited"

    # 并发槽已归还：之后可再次获得 200
    resp = await client.get("/api/c/job", headers={"x-caller-id": "dave"})
    assert resp.status_code == 200


async def test_concurrency_slots_released_after_upstream_error(upstream_factory, gateway_factory):
    u1 = upstream_factory("flaky-1")
    client = await gateway_factory(
        [make_route(
            "/api/e", [u1],
            rate_per_second=1000.0, burst=1000, max_concurrency=1,
            cache_enabled=False, max_attempts=1, health_cooldown=0.0,
        )]
    )

    # 上游失败也要释放并发槽
    for _ in range(3):
        resp = await client.get("/api/e/cluster-fail", headers={"x-caller-id": "eve"})
        assert resp.status_code == 503
    # 连续 3 次都能拿到槽（若未释放，从第二次起会是 concurrency_limited）
    resp = await client.get("/api/e/ok", headers={"x-caller-id": "eve"})
    assert resp.status_code == 200


async def test_rate_limited_distinguishable_from_upstream_failure(
    upstream_factory, gateway_factory
):
    u1 = upstream_factory("rl-1")
    client = await gateway_factory(
        [make_route(
            "/api/r", [u1],
            rate_per_second=0.0, burst=1, max_concurrency=100,
            cache_enabled=False,
        )]
    )

    ok = await client.get("/api/r/ok", headers={"x-caller-id": "z"})
    assert ok.status_code == 200
    limited = await client.get("/api/r/cluster-fail", headers={"x-caller-id": "z"})
    assert limited.status_code == 429
    assert limited.json()["error"] == "rate_limited"
    # 限流在转发前判定：即使路径会 503，也不会到达上游
    import httpx
    async with httpx.AsyncClient() as diag:
        state = (await diag.get(f"{u1.base_url}/__diag/state")).json()
    assert state["requests_total"] == 1
