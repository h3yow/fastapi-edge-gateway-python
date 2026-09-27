"""路由分发、上游健康剔除/恢复、故障转移与明确拒绝的测试。

日志中可看到：
- "route match" 路由判定依据；
- "upstream select" 轮询选择/半开探测；
- "marked UNHEALTHY" / "recovered HEALTHY" 健康变化。
"""
from __future__ import annotations

import asyncio
import time

import httpx
import pytest

from tests.conftest import make_route

pytestmark = pytest.mark.asyncio


async def test_round_robin_distribution(upstream_factory, gateway_factory):
    u1 = upstream_factory("orders-1")
    u2 = upstream_factory("orders-2")
    client = await gateway_factory(
        [make_route("/api/orders", [u1, u2], cache_enabled=False)]
    )

    upstreams = []
    for _ in range(4):
        resp = await client.get("/api/orders/list", headers={"x-caller-id": "c1"})
        assert resp.status_code == 200
        upstreams.append(resp.headers["x-gateway-upstream"])
    assert upstreams == ["orders-1", "orders-2", "orders-1", "orders-2"], upstreams


async def test_route_missing_is_rejected_distinctly(upstream_factory, gateway_factory):
    u1 = upstream_factory("orders-1")
    client = await gateway_factory([make_route("/api/orders", [u1])])

    resp = await client.get("/unknown/path", headers={"x-caller-id": "c1"})
    assert resp.status_code == 404
    data = resp.json()
    assert data["error"] == "route_not_found"
    assert resp.headers["x-gateway-reason"] == "route_not_found"


async def test_failover_on_5xx_within_one_request(upstream_factory, gateway_factory):
    u1 = upstream_factory("orders-1")
    u2 = upstream_factory("orders-2")
    client = await gateway_factory(
        [make_route("/api/orders", [u1, u2], max_attempts=3, cache_enabled=False)]
    )

    # u1 的 /fail 返回 503 -> 当次请求内故障转移到 u2
    resp = await client.get("/api/orders/orders-1/fail", headers={"x-caller-id": "c1"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["upstream"] == "orders-2"
    assert resp.headers["x-gateway-upstream"] == "orders-2"
    assert int(resp.headers["x-gateway-attempts"]) == 2

    # u1 已被剔除，后续流量全给 u2
    resp2 = await client.get("/api/orders/list", headers={"x-caller-id": "c1"})
    assert resp2.headers["x-gateway-upstream"] == "orders-2"

    # u1 统计：仅收到一次 /fail 请求
    async with httpx.AsyncClient() as diag:
        state = (await diag.get(f"{u1.base_url}/__diag/state")).json()
    assert state["requests_total"] == 1


async def test_timeout_then_failover_and_recovery(upstream_factory, gateway_factory):
    u1 = upstream_factory("slow-1")
    u2 = upstream_factory("fast-1")
    client = await gateway_factory(
        [make_route(
            "/api/jobs", [u1, u2],
            timeout_seconds=0.3, max_attempts=2, health_cooldown=0.3,
            cache_enabled=False,
        )]
    )

    # u1 慢于超时阈值 -> 超时后转移 u2，成功
    t0 = time.monotonic()
    resp = await client.get("/api/jobs/run?slow=2", headers={"x-caller-id": "c1"})
    elapsed = time.monotonic() - t0
    assert resp.status_code == 200
    assert resp.headers["x-gateway-upstream"] == "fast-1"
    assert int(resp.headers["x-gateway-attempts"]) == 2
    assert elapsed < 1.5

    # u1 已剔除 -> 直接走 u2，不等待超时
    t0 = time.monotonic()
    resp2 = await client.get("/api/jobs/run?slow=2", headers={"x-caller-id": "c1"})
    assert time.monotonic() - t0 < 0.5
    assert resp2.headers["x-gateway-upstream"] == "fast-1"

    # 等待冷却：下一次请求对半开的 u1 发起探测，仍然慢 -> 探测失败
    await asyncio.sleep(0.4)
    resp3 = await client.get("/api/jobs/run?slow=2", headers={"x-caller-id": "c1"})
    assert resp3.status_code == 200  # 探测失败后转移 u2 成功
    assert resp3.headers["x-gateway-upstream"] == "fast-1"

    # u1 恢复正常后（不带 slow），探测成功并重新纳入
    await asyncio.sleep(0.4)
    resp4 = await client.get("/api/jobs/run", headers={"x-caller-id": "c1"})
    assert resp4.status_code == 200
    assert resp4.headers["x-gateway-upstream"] == "slow-1"

    # 之后两个上游都重新参与分发
    chosen = {
        (await client.get("/api/jobs/run", headers={"x-caller-id": "c1"})).headers[
            "x-gateway-upstream"
        ]
        for _ in range(4)
    }
    assert chosen == {"slow-1", "fast-1"}, chosen


async def test_all_upstreams_down_distinguishes_reasons(upstream_factory, gateway_factory):
    u1 = upstream_factory("dead-1")
    u2 = upstream_factory("dead-2")
    client = await gateway_factory(
        [make_route("/api/x", [u1, u2], max_attempts=3, cache_enabled=False)]
    )

    # 第一次请求：u1/u2 都 503，尝试耗尽 -> upstream_error（上游失败，已重试过）
    resp = await client.get("/api/x/cluster-fail", headers={"x-caller-id": "c1"})
    assert resp.status_code == 503
    assert resp.json()["error"] == "upstream_error"

    # 两上游都已剔除：立即得到 no_healthy_upstream，与上游错误可区分
    resp2 = await client.get("/api/x/ping", headers={"x-caller-id": "c1"})
    assert resp2.status_code == 503
    data = resp2.json()
    assert data["error"] == "no_healthy_upstream"
    assert resp2.headers["x-gateway-reason"] == "no_healthy_upstream"


async def test_connection_refused_treated_as_upstream_failure(upstream_factory, gateway_factory):
    # 起一个上游然后立刻停掉，模拟连接被拒
    u_gone = upstream_factory("gone")
    client = await gateway_factory(
        [make_route("/api/g", [u_gone], max_attempts=1)]
    )
    u_gone.stop()

    resp = await client.get("/api/g/ping", headers={"x-caller-id": "c1"})
    assert resp.status_code == 502
    assert resp.json()["error"] == "upstream_error"
    assert "连接失败" in resp.json()["detail"]

    resp2 = await client.get("/api/g/ping", headers={"x-caller-id": "c1"})
    assert resp2.json()["error"] == "no_healthy_upstream"


async def test_non_safe_request_not_retried_on_failure(upstream_factory, gateway_factory):
    """POST 命中 503 时不允许重试/故障转移，且不产生重复副作用。"""
    u1 = upstream_factory("pay-1")
    u2 = upstream_factory("pay-2")
    client = await gateway_factory(
        [make_route("/api/payments", [u1, u2], max_attempts=3, cache_enabled=False)]
    )

    # 对 u1 发起 POST /charge/fail -> 503，非安全请求不重试
    resp = await client.post(
        "/api/payments/pay-1/fail",
        content=b'{"amount":100}',
        headers={"x-caller-id": "c1"},
    )
    assert resp.status_code == 503
    assert resp.json()["error"] == "upstream_error"
    assert "非安全方法" in resp.json()["detail"]

    # u1 因 503 被剔除；u2 从未收到该请求 -> 无任何副作用写入
    async with httpx.AsyncClient() as diag:
        s1 = (await diag.get(f"{u1.base_url}/__diag/state")).json()
        s2 = (await diag.get(f"{u2.base_url}/__diag/state")).json()
    assert s1["requests_total"] == 1
    assert s1["side_effects"] == {}
    assert s2["requests_total"] == 0
    assert s2["side_effects"] == {}


async def test_non_safe_request_succeeds_with_one_side_effect(upstream_factory, gateway_factory):
    u1 = upstream_factory("pay-1")
    client = await gateway_factory(
        [make_route("/api/payments", [u1], max_attempts=3, cache_enabled=False)]
    )
    resp = await client.post(
        "/api/payments/charge", content=b'{"amount":1}', headers={"x-caller-id": "c1"}
    )
    assert resp.status_code == 201
    assert resp.json()["side_effect_count"] == 1
    async with httpx.AsyncClient() as diag:
        state = (await diag.get(f"{u1.base_url}/__diag/state")).json()
    assert state["side_effects"]["/api/payments/charge"] == 1
