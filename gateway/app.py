"""FastAPI edge gateway wiring: routing, rate limiting, cache, forwarding.

A request captures the active :class:`ConfigRuntime` once at entry; every
later decision in that request (route lookup, rate-limit quota, cache,
forwarding) is evaluated against that same immutable version. A reload swaps
the active runtime atomically, so a request never sees half-old half-new
rules.
"""
from __future__ import annotations

import threading
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
from .logging_utils import log_decision
from .ratelimit import RateLimiter, RejectReason
from .router import UpstreamSelector
from .runtime import ConfigHolder, ConfigRuntime, build_runtime

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
    config: GatewayConfig  # initial config (use holder for current)
    cache: ResponseCache   # initial runtime cache
    health: HealthTracker
    limiter: RateLimiter
    holder: ConfigHolder


def _filter_response_headers(
    headers: tuple[tuple[str, str], ...],
) -> dict[str, str]:
    return {k: v for k, v in headers if k.lower() not in HOP_BY_HOP}


def _forward_headers(request: Request) -> dict[str, str]:
    return {
        k: v for k, v in request.headers.items()
        if k.lower() not in HOP_BY_HOP and k.lower() != "host"
    }


def _runtime_summary(rt: ConfigRuntime) -> dict[str, object]:
    cfg = rt.config
    return {
        "version": rt.version,
        "revision": rt.revision,
        "source_path": rt.source_path,
        "loaded_at_ms": rt.loaded_at_ms,
        "content_hash": rt.content_hash[:12],
        "upstreams": [
            {"name": u.name, "base_url": u.base_url, "weight": u.weight}
            for u in cfg.upstreams
        ],
        "routes": [
            {"name": r.name, "path_prefix": r.path_prefix,
             "strategy": r.strategy.value,
             "upstreams": list(r.upstream_names),
             "timeout_ms": r.timeout_ms, "max_retries": r.max_retries,
             "cache_ttl_ms": r.cache_ttl_ms}
            for r in cfg.routes
        ],
        "rate_limits": [
            {"caller": rl.caller, "rate_per_sec": rl.rate_per_sec,
             "burst": rl.burst, "max_concurrency": rl.max_concurrency}
            for rl in cfg.rate_limits
        ],
        "defaults": {
            "rate_per_sec": cfg.default_rate_per_sec,
            "burst": cfg.default_burst,
            "max_concurrency": cfg.default_max_concurrency,
        },
    }


def build_gateway(config: GatewayConfig,
                  client: httpx.Client | None = None,
                  probe_interval_ms: int = 500,
                  *,
                  version: str = "built-in-default",
                  config_path: str | None = None,
                  sim_urls: dict[str, str] | None = None,
                  content_hash: str = "",
                  loaded_at_ms: int | None = None) -> Gateway:
    health = HealthTracker(probe_interval_ms=probe_interval_ms)
    limiter = RateLimiter()
    owns_client = client is None

    initial = build_runtime(
        config,
        version=version,
        revision=0,
        source_path=config_path or "",
        loaded_at_ms=loaded_at_ms if loaded_at_ms is not None else now_ms(),
        content_hash=content_hash,
        health=health,
        client=client,
    )
    holder = ConfigHolder(initial)

    # Serializes reloads; revision numbering always follows swap order.
    reload_lock = threading.Lock()
    state = {"revision": 0, "config_path": config_path}

    def apply_loaded(loaded: LoadedConfig) -> ConfigRuntime:
        with reload_lock:
            state["revision"] += 1
            runtime = build_runtime(
                loaded.config,
                version=loaded.version,
                revision=state["revision"],
                source_path=loaded.source_path,
                loaded_at_ms=loaded.loaded_at_ms,
                content_hash=loaded.content_hash,
                health=health,
                client=client,
            )
            holder.replace(runtime)
            state["config_path"] = loaded.source_path
        log_decision(
            "config_reload", verdict="applied", version=loaded.version,
            revision=runtime.revision, source=loaded.source_path,
            routes=len(loaded.config.routes),
            upstreams=len(loaded.config.upstreams),
            hash=loaded.content_hash[:12],
        )
        return runtime

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        yield
        if owns_client:
            holder.current().forwarder.close()

    app = FastAPI(title="edge-gateway", lifespan=lifespan)

    @app.get("/_gateway/health")
    def gateway_health() -> dict[str, object]:
        return {"upstreams": health.snapshot()}

    @app.get("/_gateway/config/version")
    def config_version() -> dict[str, object]:
        """Which configuration version is currently serving traffic."""
        return _runtime_summary(holder.current())

    @app.post("/_gateway/config/reload")
    async def config_reload(request: Request) -> Response:
        """Atomically reload the configuration file.

        Validation runs entirely against the new file first; only after it
        parses and validates is the active runtime swapped (and the old cache
        dropped). Any failure leaves the old version serving untouched.
        """
        import json
        try:
            raw = await request.body()
            payload = json.loads(raw.decode()) if raw.strip() else {}
        except Exception:
            payload = {}
        target = payload.get("path") if isinstance(payload, dict) else None
        if not isinstance(target, str) or not target.strip():
            target = state["config_path"]
        if not target:
            return Response(
                b'{"error":"no_config_path","message":'
                b'"gateway started without a config file; pass {\"path\": '
                b'\"...\"} or restart with --config"}',
                status_code=409, media_type="application/json",
                headers={"X-Gateway-Reason": "no_config_path"},
            )
        try:
            loaded = load_config_file(target, sim_urls=sim_urls)
        except ConfigError as exc:
            messages = getattr(exc, "errors", [str(exc)])
            log_decision("config_reload", verdict="reject", source=target,
                         errors=len(messages))
            for msg in messages:
                log_decision("config_reload_error", source=target,
                             detail=msg)
            body = json.dumps(
                {"error": "invalid_config", "rejected_version":
                 _runtime_summary(holder.current())["version"],
                 "active_version": _runtime_summary(holder.current())["version"],
                 "reasons": messages},
                ensure_ascii=False,
            ).encode()
            return Response(
                body, status_code=400, media_type="application/json",
                headers={"X-Gateway-Reason": "invalid_config",
                         "X-Config-Version": holder.current().version},
            )
        runtime = apply_loaded(loaded)
        body = json.dumps(
            {"status": "applied", **_runtime_summary(runtime)},
            ensure_ascii=False,
        ).encode()
        return Response(body, status_code=200,
                        media_type="application/json",
                        headers={"X-Config-Version": runtime.version})

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
        removed = holder.current().cache.invalidate(
            caller=caller, route_name=route_name)
        return {"invalidated": removed}

    @app.api_route(
        "/{full_path:path}",
        methods=["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    )
    async def entry(request: Request, full_path: str) -> Response:
        # Capture the active version once: every decision below uses this
        # same snapshot even if a reload happens mid-request.
        rt = holder.current()
        cfg = rt.config
        ver = rt.version
        path = "/" + full_path
        caller = request.headers.get(CALLER_HEADER, "anonymous")
        method = request.method
        route = cfg.route_for(path)
        if route is None:
            log_decision("route_lookup", path=path, verdict="reject",
                         reason="route_not_found", caller=caller,
                         config_version=ver)
            return Response(NO_ROUTE_BODY, status_code=404,
                            media_type="application/json",
                            headers={"X-Gateway-Reason": "route_not_found",
                                     "X-Config-Version": ver})
        log_decision("route_lookup", path=path, route=route.name,
                     verdict="match", caller=caller, config_version=ver)

        rl_cfg = cfg.rate_limit_for(caller)
        decision = limiter.try_acquire(caller, rl_cfg)
        if not decision.allowed:
            assert decision.reason is not None
            limited = decision.reason == RejectReason.CONCURRENCY_LIMITED
            return Response(
                CONCURRENCY_BODY if limited else RATE_LIMIT_BODY,
                status_code=429,
                media_type="application/json",
                headers={"X-Gateway-Reason": decision.reason.value,
                         "X-Config-Version": ver},
            )

        try:
            body = await request.body()
            vary = {h: request.headers.get(h, "") for h in VARY_HEADERS}
            cached = None
            cache_key = ""
            if method in CACHEABLE_METHODS and route.cache_ttl_ms > 0:
                cache_key = ResponseCache.make_key(
                    caller, route.name, method, path,
                    {"query": request.url.query, **vary},
                )
                cached = rt.cache.get(cache_key)
            if cached is not None:
                return Response(
                    cached.body,
                    status_code=cached.status_code,
                    headers={
                        **_filter_response_headers(cached.headers),
                        "X-Cache": "HIT",
                        "X-Cache-Key": cache_key[:12],
                        "X-Upstream": cached.upstream_name,
                        "X-Config-Version": ver,
                    },
                )

            # Blocking httpx work is pushed to a worker thread so concurrent
            # requests truly overlap and the concurrency cap is meaningful.
            result = await anyio.to_thread.run_sync(
                lambda: rt.forwarder.forward(
                    rt.selector,
                    route=route,
                    method=method,
                    path=path,
                    headers=_forward_headers(request),
                    content=body,
                    query=request.url.query,
                )
            )

            if result.error is not None:
                return Response(
                    UPSTREAM_FAIL_BODY,
                    status_code=503 if result.status_code == 503 else 504,
                    media_type="application/json",
                    headers={
                        "X-Gateway-Reason": result.error,
                        "X-Upstream": result.upstream_name,
                        "X-Config-Version": ver,
                    },
                )

            if result.status_code in (502, 503, 504):
                log_decision("terminal_upstream_error",
                             route=route.name,
                             status=result.status_code,
                             upstream=result.upstream_name,
                             config_version=ver)
                return Response(
                    UPSTREAM_FAIL_BODY,
                    status_code=503,
                    media_type="application/json",
                    headers={
                        "X-Gateway-Reason": f"status_{result.status_code}",
                        "X-Upstream": result.upstream_name,
                        "X-Config-Version": ver,
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
                rt.cache.put(cache_key, cache_entry, caller=caller,
                             route_name=route.name)

            return Response(
                result.content,
                status_code=result.status_code,
                headers={
                    **response_headers,
                    "X-Cache": "MISS",
                    "X-Upstream": result.upstream_name,
                    "X-Upstream-Attempts": str(result.attempts),
                    "X-Config-Version": ver,
                },
            )
        finally:
            limiter.release(caller)

    return Gateway(
        app=app,
        config=config,
        cache=initial.cache,
        health=health,
        limiter=limiter,
        holder=holder,
    )
