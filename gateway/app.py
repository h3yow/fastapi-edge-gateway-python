"""FastAPI edge gateway wiring: routing, rate limiting, cache, forwarding.

A running gateway pins one immutable configuration *snapshot* per request
(see :class:`gateway.manager.ConfigManager`). The pin is taken once, at the
top of the request, so route lookup, rate-limit decision and cache lookup
for the same request always derive from the same revision — even while a
reload swaps generations underneath concurrent traffic.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from contextlib import asynccontextmanager

import anyio
import httpx
from fastapi import FastAPI, Request, Response

from .cache import (CACHEABLE_METHODS, CacheEntry, ResponseCache, now_ms)
from .config import GatewayConfig
from .configfile import ConfigError, LoadedConfig, load_config_file
from .forwarder import Forwarder
from .health import HealthTracker
from .logging_utils import decision_scope, log_decision
from .manager import ConfigManager, Snapshot
from .ratelimit import RateLimiter, RejectReason

CALLER_HEADER = "X-Caller-Id"
VARY_HEADERS = ("accept", "accept-encoding", "accept-language")

# Error codes are machine-distinguishable and documented in README.
NO_ROUTE_BODY = b'{"error":"route_not_found"}'
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
    manager: ConfigManager
    health: HealthTracker
    limiter: RateLimiter
    config_path: str | None = None

    # Backwards-compatible accessors used by existing tests/tooling. They
    # always reflect the *currently active* generation.
    @property
    def config(self) -> GatewayConfig:
        return self.manager.active().config

    @property
    def cache(self) -> ResponseCache:
        return self.manager.active().cache


def _filter_response_headers(
    headers: tuple[tuple[str, str], ...],
) -> dict[str, str]:
    return {k: v for k, v in headers if k.lower() not in HOP_BY_HOP}


def _forward_headers(request: Request) -> dict[str, str]:
    return {
        k: v for k, v in request.headers.items()
        if k.lower() not in HOP_BY_HOP and k.lower() != "host"
    }


def _config_headers(snapshot: Snapshot) -> dict[str, str]:
    """Headers identifying the exact rule generation serving a response."""
    return {
        "X-Config-Revision": str(snapshot.revision),
        "X-Config-Version": snapshot.version,
    }


def build_gateway(config: GatewayConfig,
                  client: httpx.Client | None = None,
                  probe_interval_ms: int = 500,
                  *,
                  config_path: str | None = None,
                  initial: LoadedConfig | None = None) -> Gateway:
    health = HealthTracker(probe_interval_ms=probe_interval_ms)
    limiter = RateLimiter()
    forwarder = Forwarder(health, client)
    owns_client = client is None

    if initial is not None:
        manager = ConfigManager(
            initial.config, health,
            version=initial.version or "",
            content_sha256=initial.content_sha256,
            source=f"file:{config_path}",
        )
    else:
        manager = ConfigManager(config, health)

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        yield
        if owns_client:
            forwarder.close()

    app = FastAPI(title="edge-gateway", lifespan=lifespan)

    @app.get("/_gateway/health")
    def gateway_health() -> dict[str, object]:
        snapshot = manager.active()
        return {
            "upstreams": health.snapshot(),
            "config_revision": snapshot.revision,
            "config_version": snapshot.version,
        }

    @app.get("/_gateway/config")
    def config_status() -> dict[str, object]:
        """Report which configuration generation is currently serving."""
        info = manager.info()
        snapshot = manager.active()
        return {
            "revision": info.revision,
            "version": info.version,
            "content_sha256": info.content_sha256,
            "source": info.source,
            "loaded_at_monotonic_ms": info.loaded_at_ms,
            "routes": [
                {
                    "name": r.name,
                    "path_prefix": r.path_prefix,
                    "upstreams": list(r.upstream_names),
                    "strategy": r.strategy.value,
                    "timeout_ms": r.timeout_ms,
                    "max_retries": r.max_retries,
                    "cache_ttl_ms": r.cache_ttl_ms,
                }
                for r in snapshot.config.routes
            ],
        }

    async def _reload_from(path: str) -> tuple[int, dict[str, object]]:
        # File I/O and full validation happen off the event loop and finish
        # before the atomic pointer swap; traffic keeps using the old snapshot.
        try:
            loaded = await anyio.to_thread.run_sync(load_config_file, path)
        except ConfigError as exc:
            active = manager.info()
            log_decision("config_reload_rejected",
                         old_revision=active.revision,
                         reason_count=len(exc.reasons),
                         reasons="; ".join(exc.reasons))
            return 422, {
                "error": "config_reload_rejected",
                "rejected": True,
                "reasons": exc.reasons,
                "active_revision": active.revision,
                "active_version": active.version,
            }

        before = manager.info()
        snapshot = manager.reload(
            loaded.config,
            version=loaded.version or "",
            content_sha256=loaded.content_sha256,
            source=f"file:{path}",
        )
        unchanged = snapshot.revision == before.revision
        return 200, {
            "error": None,
            "rejected": False,
            "unchanged": unchanged,
            "previous_revision": before.revision,
            "previous_version": before.version,
            "revision": snapshot.revision,
            "version": snapshot.version,
            "content_sha256": loaded.content_sha256,
            "routes": len(loaded.config.routes),
            "upstreams": len(loaded.config.upstreams),
        }

    @app.post("/_gateway/config/reload")
    async def config_reload(request: Request) -> Response:
        """Re-read and atomically apply the local config file.

        Optional JSON body ``{"path": "/abs/config.json"}`` overrides the
        path the gateway was started with.
        """
        target = config_path
        try:
            raw = await request.body()
            if raw.strip():
                payload = json.loads(raw.decode())
                if isinstance(payload, dict) and isinstance(
                        payload.get("path"), str):
                    target = payload["path"]
        except Exception:
            return Response(
                b'{"error":"invalid_request_body"}',
                status_code=400,
                media_type="application/json",
                headers={"X-Gateway-Reason": "invalid_request_body"},
            )
        if not target:
            return Response(
                b'{"error":"no_config_path"}',
                status_code=400,
                media_type="application/json",
                headers={"X-Gateway-Reason": "no_config_path"},
            )
        status_code, payload = await _reload_from(target)
        body = json.dumps(payload).encode()
        headers = {}
        if status_code == 422:
            headers["X-Gateway-Reason"] = "config_reload_rejected"
        return Response(body, status_code=status_code,
                        media_type="application/json", headers=headers)

    @app.post("/_gateway/cache/invalidate")
    async def cache_invalidate(request: Request) -> dict[str, object]:
        try:
            raw = await request.body()
            payload = json.loads(raw.decode())
        except Exception:
            payload = {}
        caller = payload.get("caller")
        route_name = payload.get("route")
        removed = manager.active().cache.invalidate(
            caller=caller, route_name=route_name)
        return {"invalidated": removed}

    @app.api_route(
        "/{full_path:path}",
        methods=["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    )
    async def entry(request: Request, full_path: str) -> Response:
        # Pin the generation once. Everything below uses this snapshot only.
        snapshot = manager.active()
        config = snapshot.config
        path = "/" + full_path
        caller = request.headers.get(CALLER_HEADER, "anonymous")
        method = request.method
        with decision_scope(rev=snapshot.revision, ver=snapshot.version):
            route = config.route_for(path)
            if route is None:
                log_decision("route_lookup", path=path, verdict="reject",
                             reason="route_not_found", caller=caller)
                return Response(
                    NO_ROUTE_BODY, status_code=404,
                    media_type="application/json",
                    headers={**_config_headers(snapshot),
                             "X-Gateway-Reason": "route_not_found"})
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
                    headers={**_config_headers(snapshot),
                             "X-Gateway-Reason": decision.reason.value},
                )

            try:
                body = await request.body()
                vary = {h: request.headers.get(h, "") for h in VARY_HEADERS}
                cached = None
                cache_key = ""
                # Cache lookup uses the snapshot's own cache, so an entry
                # produced under an older generation can never leak through.
                if method in CACHEABLE_METHODS and route.cache_ttl_ms > 0:
                    cache_key = ResponseCache.make_key(
                        caller, route.name, method, path,
                        {"query": request.url.query, **vary},
                    )
                    cached = snapshot.cache.get(cache_key)
                if cached is not None:
                    return Response(
                        cached.body,
                        status_code=cached.status_code,
                        headers={
                            **_filter_response_headers(cached.headers),
                            **_config_headers(snapshot),
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
                        selector=snapshot.selector,
                    )
                )

                if result.error is not None:
                    return Response(
                        UPSTREAM_FAIL_BODY,
                        status_code=503 if result.status_code == 503 else 504,
                        media_type="application/json",
                        headers={
                            **_config_headers(snapshot),
                            "X-Gateway-Reason": result.error,
                            "X-Upstream": result.upstream_name,
                        },
                    )

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
                            **_config_headers(snapshot),
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
                    cache_entry = CacheEntry(
                        status_code=result.status_code,
                        body=result.content,
                        headers=tuple(response_headers.items()),
                        stored_ms=now_ms(),
                        ttl_ms=route.cache_ttl_ms,
                        upstream_name=result.upstream_name,
                    )
                    snapshot.cache.put(cache_key, cache_entry, caller=caller,
                                       route_name=route.name)

                return Response(
                    result.content,
                    status_code=result.status_code,
                    headers={
                        **response_headers,
                        **_config_headers(snapshot),
                        "X-Cache": "MISS",
                        "X-Upstream": result.upstream_name,
                        "X-Upstream-Attempts": str(result.attempts),
                    },
                )
            finally:
                limiter.release(caller)

    return Gateway(
        app=app,
        manager=manager,
        health=health,
        limiter=limiter,
        config_path=config_path,
    )
