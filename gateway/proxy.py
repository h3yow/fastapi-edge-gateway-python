"""转发引擎：超时、重试、故障转移与缓存编排。

入口判定顺序（可解释）：
1. 路由匹配（在 app 层完成）；
2. 调用方令牌桶限流；
3. 调用方并发槽；
4. 缓存查询（仅安全方法，命中不占用上游）；
5. 上游转发：失败按 max_attempts 在（健康）上游间故障转移；
   仅安全方法允许重试，非安全方法只尝试一次，避免重复副作用；
6. 成功的安全响应写入缓存。
"""
from __future__ import annotations

import logging
import time

import httpx

from .cache import CacheEntry, ResponseCache
from .errors import ErrorReason, GatewayError
from .ratelimit import RateLimiter
from .router import Route
from .upstream import Upstream

logger = logging.getLogger("gateway.proxy")

SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})  # 可安全重放
RETRYABLE_STATUS = frozenset({502, 503, 504, 500})


class ProxyResponse:
    def __init__(
        self,
        status_code: int,
        headers: dict[str, str],
        body: bytes,
        upstream: str,
        attempts: int,
        cache_hit: bool,
    ) -> None:
        self.status_code = status_code
        self.headers = headers
        self.body = body
        self.upstream = upstream
        self.attempts = attempts
        self.cache_hit = cache_hit


class ProxyEngine:
    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client = client

    async def forward(
        self,
        route: Route,
        rate_limiter: RateLimiter,
        cache: ResponseCache | None,
        caller: str,
        method: str,
        path: str,
        query: str,
        headers: dict[str, str],
        body: bytes,
    ) -> ProxyResponse:
        cfg = route.config
        is_safe = method.upper() in SAFE_METHODS

        # 4. 缓存查询（仅安全方法）
        cache_key: str | None = None
        if cache is not None and cfg.cache.enabled and is_safe:
            cache_key = ResponseCache.make_key(caller, method, path, query, body)
            entry = cache.get(cache_key)
            if entry is not None:
                return ProxyResponse(
                    entry.status_code,
                    dict(entry.headers),
                    entry.body,
                    upstream="cache",
                    attempts=0,
                    cache_hit=True,
                )

        # 5. 转发 + 故障转移
        attempts = 0
        last: Upstream | None = None
        last_failure: GatewayError | None = None

        while attempts < cfg.retry.max_attempts:
            attempts += 1
            upstream = route.pool.select()
            if upstream is None:
                if last_failure is not None:
                    # 本次请求已实际尝试过上游并失败（重试耗尽）：上游错误
                    logger.warning(
                        "forward abort: retries exhausted, last=%s",
                        last.name if last else "?",
                    )
                    raise last_failure
                # 尚未尝试任何上游即无健康实例：明确的无可用上游
                logger.warning(
                    "forward abort: no healthy upstream (tried=%s)",
                    [u.name for u in route.pool.all()],
                )
                raise GatewayError(
                    ErrorReason.NO_HEALTHY_UPSTREAM,
                    503,
                    f"路由 {cfg.path_prefix} 无可用上游",
                )

            url = upstream.config.base_url.rstrip("/") + path
            if query:
                url = f"{url}?{query}"
            logger.info(
                "forward attempt=%d upstream=%s method=%s url=%s",
                attempts,
                upstream.name,
                method,
                url,
            )

            try:
                resp = await self._client.request(
                    method,
                    url,
                    headers=headers,
                    content=body,
                    timeout=cfg.retry.timeout_seconds,
                )
            except httpx.TimeoutException:
                route.pool.mark_failure(upstream)
                last, last_failure = upstream, GatewayError(
                    ErrorReason.UPSTREAM_TIMEOUT,
                    504,
                    f"上游 {upstream.name} 超时",
                )
                logger.warning("forward upstream=%s timeout", upstream.name)
            except httpx.TransportError as exc:
                route.pool.mark_failure(upstream)
                last, last_failure = upstream, GatewayError(
                    ErrorReason.UPSTREAM_ERROR,
                    502,
                    f"上游 {upstream.name} 连接失败: {exc.__class__.__name__}",
                )
                logger.warning("forward upstream=%s transport error: %s", upstream.name, exc)
            else:
                if resp.status_code in RETRYABLE_STATUS:
                    route.pool.mark_failure(upstream)
                    last, last_failure = upstream, GatewayError(
                        ErrorReason.UPSTREAM_ERROR,
                        resp.status_code,
                        f"上游 {upstream.name} 返回 {resp.status_code}",
                    )
                    logger.warning(
                        "forward upstream=%s status=%d (retryable)",
                        upstream.name,
                        resp.status_code,
                    )
                else:
                    route.pool.mark_success(upstream)
                    body_bytes = resp.content
                    resp_headers = dict(resp.headers)
                    result = ProxyResponse(
                        resp.status_code,
                        resp_headers,
                        body_bytes,
                        upstream=upstream.name,
                        attempts=attempts,
                        cache_hit=False,
                    )
                    # 6. 仅缓存安全方法的成功响应
                    if (
                        cache is not None
                        and cfg.cache.enabled
                        and is_safe
                        and cache_key is not None
                        and 200 <= resp.status_code < 300
                    ):
                        cache.put(
                            cache_key,
                            CacheEntry(
                                status_code=resp.status_code,
                                headers=resp_headers,
                                body=body_bytes,
                                expires_at=time.monotonic() + cfg.cache.ttl_seconds,
                            ),
                        )
                    return result

            # 失败后是否允许再次尝试（安全重放）
            if not is_safe:
                logger.info(
                    "forward stop retry: method=%s is non-replayable, skip failover",
                    method,
                )
                break
            if attempts >= cfg.retry.max_attempts:
                logger.info("forward stop retry: max_attempts=%d exhausted", cfg.retry.max_attempts)

        assert last_failure is not None
        detail = last_failure.detail
        if not is_safe:
            detail += "（非安全方法，未重放/故障转移）"
        raise GatewayError(last_failure.reason, last_failure.status_code, detail)
