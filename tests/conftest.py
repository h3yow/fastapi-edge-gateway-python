"""测试夹具：在线程中启动真实 uvicorn 上游实例，网关以 ASGI 方式挂载。"""
from __future__ import annotations

import threading
import time

import httpx
import pytest
import uvicorn

from gateway.app import GatewayRuntime, create_app
from gateway.config import (
    CacheConfig,
    GatewayConfig,
    RateLimitConfig,
    RetryConfig,
    RouteConfig,
    UpstreamConfig,
)
from upstream_app import create_upstream_app


class UpstreamServer:
    """运行在独立线程中的真实 uvicorn 上游。"""

    def __init__(self, name: str) -> None:
        self.name = name
        config = uvicorn.Config(
            create_upstream_app(name), host="127.0.0.1", port=0, log_level="error"
        )
        self.server = uvicorn.Server(config)
        self.thread = threading.Thread(target=self.server.run, daemon=True)
        self.thread.start()
        while not self.server.started:
            time.sleep(0.01)
        self.port = self.server.servers[0].sockets[0].getsockname()[1]
        self.base_url = f"http://127.0.0.1:{self.port}"

    def stop(self) -> None:
        self.server.should_exit = True
        self.thread.join(timeout=3)


@pytest.fixture
def upstream_factory():
    servers: list[UpstreamServer] = []

    def make(name: str) -> UpstreamServer:
        srv = UpstreamServer(name)
        servers.append(srv)
        return srv

    yield make
    for srv in servers:
        srv.stop()


def make_route(
    prefix: str,
    servers: list[UpstreamServer],
    *,
    rate_per_second: float = 100.0,
    burst: int = 100,
    max_concurrency: int = 100,
    cache_enabled: bool = True,
    cache_ttl: float = 60.0,
    max_attempts: int = 3,
    timeout_seconds: float = 1.0,
    health_cooldown: float = 0.2,
) -> RouteConfig:
    return RouteConfig(
        path_prefix=prefix,
        upstreams=[
            UpstreamConfig(name=srv.name, base_url=srv.base_url) for srv in servers
        ],
        rate_limit=RateLimitConfig(
            rate_per_second=rate_per_second,
            burst=burst,
            max_concurrency=max_concurrency,
        ),
        cache=CacheConfig(enabled=cache_enabled, ttl_seconds=cache_ttl),
        retry=RetryConfig(
            max_attempts=max_attempts,
            timeout_seconds=timeout_seconds,
            health_cooldown=health_cooldown,
        ),
    )


@pytest.fixture
async def gateway_factory():
    runtimes: list[GatewayRuntime] = []
    clients: list[httpx.AsyncClient] = []

    async def make(routes: list[RouteConfig]) -> httpx.AsyncClient:
        config = GatewayConfig(routes=routes)
        app = create_app(config)
        # ASGITransport 不触发 lifespan，手动装配运行时
        runtime = GatewayRuntime(config)
        app.state.runtime = runtime
        runtimes.append(runtime)
        client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://edge-gateway"
        )
        clients.append(client)
        return client

    yield make

    for client in clients:
        await client.aclose()
    for runtime in runtimes:
        await runtime.aclose()
