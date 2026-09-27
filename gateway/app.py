"""FastAPI edge gateway wiring: routing, rate limiting, cache, forwarding."""
from __future__ import annotations

from dataclasses import dataclass
from contextlib import asynccontextmanager

import anyio
import httpx
from fastapi import FastAPI, Request, Response

from .cache import (CACHEABLE_METHODS, CacheEntry, ResponseCache, now_ms)
from .config import GatewayConfig
from .forwarder import Forwarder
from .health import HealthTracker
from .logging_utils import log_decision
from .ratelimit import RateLimiter, RejectReason
from .router import UpstreamSelector

CALLER_HEADER = "X-Caller-Id"
VARY_HEADERS = ("accept", "accept-encoding", "accept-language")

# Error codes are machine-distinguishable and documented in README.
NO_ROUTE_BODY = b'{"error":"route_not_found"}'
NO_UPSTREAM_BODY = b'{"error":"no_available_upstream"}'
RATE_LIMIT_BODY = b'{"error":"rate_limited"}'
CONCURRENCY_BODY = b'{"error":"concurrency_limited"}'
UPSTREAM_FAIL_BODY = b'{"error":"upstream_unavailable"}'

HOP_BY_HOP = frozenset({
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade", "content-length",
})


@dataclass
class Gateway:
    app: FastAPI
    config: GatewayConfig
    cache: ResponseCache
    health: HealthTracker
    limiter: RateLimiter


def _filter_response_headers(
    headers: tuple[tuple[str, str], ...],
) -> dict[str, str]:
    return {k: v for k, v in headers if k.lower() not in HOP_BY_HOP}


def _forward_headers(request: Request) -> dict[str, str]:
    headers = {
        k: v for k, v in request.headers.items()
        if k.lower() not in HOP_BY_HOP and k.lower() != "host"
    }
    return headers


def build_gateway(config: GatewayConfig,
                  client: httpx.Client | None = None,
                  probe_interval_ms: int = 500) -> Gateway:
    cache = ResponseCache()
    health = HealthTracker(probe_interval_ms=probe_interval_ms)
    selector = UpstreamSelector(config, health)
    limiter = RateLimiter()
    forwarder = Forwarder(health, selector, client)
    owns_client = client is None

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        yield
        if owns_client:
            forwarder.close()

    app = FastAPI(title="edge-gateway", lifespan=lifespan)

    @app.get("/_gateway/health")
    def gateway_health() -> dict[str, object]:
        return {"upstreams": health.snapshot()}

    @app.post("/_gateway/cache/invalidate")
    async def cache_invalidate(request: Request) -> dict[str, object]:
        import json
        try:
            raw = await request.body()
            payload = json.loads(raw.decode())
        except Exception:
            payload = {}
        caller = payload.get("caller")
        route_name = payload.get("route")
        removed = cache.invalidate(caller=caller, route_name=route_name)
        return {"invalidated": removed}

    @app.api_route(
        "/{full_path:path}",
        methods=["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    )
    async def entry(request: Request, full_path: str) -> Response:
        path = "/" + full_path
        caller = request.headers.get(CALLER_HEADER, "anonymous")
        method = request.method
        route = config.route_for(path)
        if route is None:
            log_decision("route_lookup", path=path, verdict="reject",
                         reason="route_not_found", caller=caller)
            return Response(NO_ROUTE_BODY, status_code=404,
                            media_type="application/json",
                            headers={"X-Gateway-Reason": "route_not_found"})
        log_decision("route_lookup", path=path, route=route.name,
                     verdict="match", caller=caller)

        rl_cfg = config.rate_limit_for(caller)
        decision = limiter.try_acquire(caller, rl_cfg)
        if not decision.allowed:
            assert decision.reason is not None
            limited = decision.reason == RejectReason.CONCURRENCY_LIMITED
            return Response(
                CONCURRENCY_BODY if limited else RATE_LIMIT_BODY,
                status_code=429,
                media_type="application/json",
                headers={"X-Gateway-Reason": decision.reason.value},
            )

        try:
            body = await request.body()
            vary = {
                h: request.headers.get(h, "")
                for h in VARY_HEADERS
            }
            cached = None
            cache_key = ""
            if method in CACHEABLE_METHODS and route.cache_ttl_ms > 0:
                cache_key = ResponseCache.make_key(
                    caller, route.name, method, path,
                    {"query": request.url.query, **vary},
                )
                cached = cache.get(cache_key)
            if cached is not None:
                return Response(
                    cached.body,
                    status_code=cached.status_code,
                    headers={
                        **_filter_response_headers(cached.headers),
                        "X-Cache": "HIT",
                        "X-Cache-Key": cache_key[:12],
                        "X-Upstream": cached.upstream_name,
                    },
                )

            # Blocking httpx work is pushed to a worker thread so concurrent
            # requests truly overlap and the concurrency cap is meaningful.
            result = await anyio.to_thread.run_sync(
                lambda: forwarder.forward(
                    route=route,
                    method=method,
                    path=path,
                    headers=_forward_headers(request),
                    content=body,
                    query=request.url.query,
                )
            )

            if result.error is not None:
                # Upstream-originated failure: distinct reason header so the
                # caller can tell it apart from a local rate-limit rejection.
                return Response(
                    UPSTREAM_FAIL_BODY,
                    status_code=503 if result.status_code == 503 else 504,
                    media_type="application/json",
                    headers={
                        "X-Gateway-Reason": result.error,
                        "X-Upstream": result.upstream_name,
                    },
                )

            # A retriable-status terminal response (all upstreams answered
            # 502/503/504) is normalized so the reason stays distinguishable.
            if result.status_code in (502, 503, 504):
                log_decision("terminal_upstream_error",
                             route=route.name,
                             status=result.status_code,
                             upstream=result.upstream_name)
                return Response(
                    UPSTREAM_FAIL_BODY,
                    status_code=503,
                    media_type="application/json",
                    headers={
                        "X-Gateway-Reason": f"status_{result.status_code}",
                        "X-Upstream": result.upstream_name,
                    },
                )

            response_headers = _filter_response_headers(result.headers)
            cacheable = (
                method in CACHEABLE_METHODS
                and route.cache_ttl_ms > 0
                and 200 <= result.status_code < 300
            )
            if cacheable:
                entry = CacheEntry(
                    status_code=result.status_code,
                    body=result.content,
                    headers=tuple(response_headers.items()),
                    stored_ms=now_ms(),
                    ttl_ms=route.cache_ttl_ms,
                    upstream_name=result.upstream_name,
                )
                cache.put(cache_key, entry, caller=caller,
                          route_name=route.name)

            return Response(
                result.content,
                status_code=result.status_code,
                headers={
                    **response_headers,
                    "X-Cache": "MISS",
                    "X-Upstream": result.upstream_name,
                    "X-Upstream-Attempts": str(result.attempts),
                },
            )
        finally:
            limiter.release(caller)

    return Gateway(
        app=app,
        config=config,
        cache=cache,
        health=health,
        limiter=limiter,
    )
