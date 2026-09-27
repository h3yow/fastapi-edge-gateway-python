"""按调用方维度的限流（令牌桶）与并发约束。"""
from __future__ import annotations

import asyncio
import logging
import time

from .config import RateLimitConfig

logger = logging.getLogger("gateway.ratelimit")


class _Bucket:
    __slots__ = ("tokens", "last_refill", "lock", "inflight")

    def __init__(self, tokens: float, now: float) -> None:
        self.tokens = tokens
        self.last_refill = now
        self.lock = asyncio.Lock()  # 保护 inflight 的并发判定
        self.inflight = 0


class RateLimiter:
    """每个调用方（caller）独立的令牌桶 + 并发计数。

    - 令牌桶按 rate_per_second 补充、burst 为容量，初始满桶；
    - allow() 为无 await 的同步临界区，事件循环内原子执行，
      突发并发下不会多计或漏计；
    - 并发按 max_concurrency 限制在途请求数（不排队、立即判定）。
    """

    def __init__(self, config: RateLimitConfig) -> None:
        self._config = config
        self._buckets: dict[str, _Bucket] = {}

    def _get(self, caller: str) -> _Bucket:
        bucket = self._buckets.get(caller)
        if bucket is None:
            now = time.monotonic()
            bucket = _Bucket(tokens=float(self._config.burst), now=now)
            self._buckets[caller] = bucket
        return bucket

    def allow(self, caller: str) -> bool:
        now = time.monotonic()
        bucket = self._get(caller)
        # 惰性补充令牌
        elapsed = now - bucket.last_refill
        if elapsed > 0:
            bucket.tokens = min(
                float(self._config.burst),
                bucket.tokens + elapsed * self._config.rate_per_second,
            )
            bucket.last_refill = now
        if bucket.tokens >= 1.0:
            bucket.tokens -= 1.0
            logger.info(
                "rate-limit caller=%s decision=ALLOW remaining=%.2f", caller, bucket.tokens
            )
            return True
        logger.info(
            "rate-limit caller=%s decision=REJECT remaining=%.2f", caller, bucket.tokens
        )
        return False

    async def acquire_concurrency(self, caller: str) -> bool:
        bucket = self._get(caller)
        async with bucket.lock:
            if bucket.inflight < self._config.max_concurrency:
                bucket.inflight += 1
                logger.info(
                    "concurrency caller=%s decision=ALLOW inflight=%d/%d",
                    caller,
                    bucket.inflight,
                    self._config.max_concurrency,
                )
                return True
            logger.info(
                "concurrency caller=%s decision=REJECT inflight=%d/%d",
                caller,
                bucket.inflight,
                self._config.max_concurrency,
            )
            return False

    def release_concurrency(self, caller: str) -> None:
        bucket = self._buckets.get(caller)
        if bucket is not None and bucket.inflight > 0:
            bucket.inflight -= 1
            logger.info(
                "concurrency caller=%s released inflight=%d/%d",
                caller,
                bucket.inflight,
                self._config.max_concurrency,
            )
