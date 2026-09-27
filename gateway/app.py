"""FastAPI 应用工厂：装配路由表、限流器、缓存与转发引擎。"""
from __future__ import annotations

import json
import logging
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

from .cache import ResponseCache
from .config import GatewayConfig
from .errors import ErrorReason, GatewayError
from .proxy import ProxyEngine
from .ratelimit import RateLimiter
from .router import Route, RouteTable

logger = logging.getLogger("gateway.app")

HOP_BY_HOP = frozenset({
    "host", "content-length", "connection", "keep-alive", "proxy-authenticate",
    "proxy-authorization", "te", "trailers", "transfer-encoding", "upgrade",
    "content-encoding",
})


class GatewayRuntime:
    """运行时组件集合；reload 时重建路由表，限流/缓存状态按路由前缀保留。"""

    def __init__(self, config: GatewayConfig) -> None:
        self.client = httpx.AsyncClient()
        self.engine = ProxyEngine(self.client)
        self._limiters: dict[str, RateLimiter] = {}
        self._caches: dict[str, ResponseCache] = {}
        self.reload(config)

    def reload(self, config: GatewayConfig) -> None:
        self.config = config
        self.table = RouteTable(config.routes)
        for route in config.routes:
            prefix = route.path_prefix
            self._limiters.setdefault(prefix, RateLimiter(route.rate_limit))
            if route.cache.enabled:
                self._caches.setdefault(
                    prefix,
                    ResponseCache(route.cache.ttl_seconds, route.cache.max_entries),
                )
        logger.info("gateway config reloaded: %d routes", len(config.routes))

    def limiter_for(self, route: Route) -> RateLimiter:
        return self._limiters[route.config.path_prefix]

    def cache_for(self, route: Route) -> ResponseCache | None:
        return self._caches.get(route.config.path_prefix)

    async def aclose(self) -> None:
        await self.client.aclose()


def load_config(path: str | Path) -> GatewayConfig:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    return GatewayConfig.model_validate(data)


def create_app(config: GatewayConfig) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        runtime = GatewayRuntime(config)
        app.state.runtime = runtime
        yield
        await runtime.aclose()

    app = FastAPI(title="edge-gateway", lifespan=lifespan)

    @app.exception_handler(GatewayError)
    async def on_gateway_error(_: Request, exc: GatewayError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content={"error": exc.reason.value, "detail": exc.detail},
            headers={"X-Gateway-Reason": exc.reason.value},
        )

    @app.api_route("/__admin/reload", methods=["POST"])
    async def admin_reload(request: Request) -> JSONResponse:
        body = await request.json()
        new_config = GatewayConfig.model_validate(body)
        request.app.state.runtime.reload(new_config)
        return JSONResponse({"status": "reloaded", "routes": len(new_config.routes)})

    @app.get("/__admin/config")
    async def admin_config(request: Request) -> JSONResponse:
        runtime: GatewayRuntime = request.app.state.runtime
        return JSONResponse(json.loads(runtime.config.model_dump_json()))

    @app.api_route(
        "/{full_path:path}",
        methods=["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"],
    )
    async def gateway_entry(request: Request, full_path: str = "") -> Response:
        runtime: GatewayRuntime = request.app.state.runtime
        path = "/" + full_path
        caller = request.headers.get("x-caller-id", "anonymous")

        # 1. 路由匹配：缺失必须明确拒绝
        route = runtime.table.match(path)
        if route is None:
            raise GatewayError(
                ErrorReason.ROUTE_NOT_FOUND,
                404,
                f"无匹配路由: {path}",
            )

        limiter = runtime.limiter_for(route)
        cache = runtime.cache_for(route)

        # 2. 令牌桶限流
        if not limiter.allow(caller):
            raise GatewayError(
                ErrorReason.RATE_LIMITED,
                429,
                f"调用方 {caller} 超出速率限制",
            )

        # 3. 并发约束
        if not await limiter.acquire_concurrency(caller):
            raise GatewayError(
                ErrorReason.CONCURRENCY_LIMITED,
                429,
                f"调用方 {caller} 超出并发限制",
            )

        body = await request.body()
        query = request.url.query
        headers = {
            k: v for k, v in request.headers.items() if k.lower() not in HOP_BY_HOP
        }

        try:
            result = await runtime.engine.forward(
                route=route,
                rate_limiter=limiter,
                cache=cache,
                caller=caller,
                method=request.method,
                path=path,
                query=query,
                headers=headers,
                body=body,
            )
        finally:
            limiter.release_concurrency(caller)

        out_headers = {
            k: v
            for k, v in result.headers.items()
            if k.lower() not in HOP_BY_HOP
        }
        out_headers["X-Gateway-Upstream"] = result.upstream
        out_headers["X-Gateway-Attempts"] = str(result.attempts)
        out_headers["X-Gateway-Cache"] = "HIT" if result.cache_hit else "MISS"
        return Response(
            content=result.body,
            status_code=result.status_code,
            headers=out_headers,
        )

    return app
