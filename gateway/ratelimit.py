"""Per-caller rate limiting (token bucket) and concurrency caps.

Token reservation and concurrency-slot reservation happen under one lock so a
burst of parallel requests can never over-admit or double count.
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from enum import Enum

from .config import RateLimitConfig
from .logging_utils import log_decision


class RejectReason(str, Enum):
    RATE_LIMITED = "rate_limited"
    CONCURRENCY_LIMITED = "concurrency_limited"


@dataclass(frozen=True)
class RateDecision:
    allowed: bool
    reason: RejectReason | None
    tokens_after: float
    in_flight_after: int
    detail: str


class _Bucket:
    __slots__ = ("tokens", "last_ms", "in_flight")

    def __init__(self, tokens: float) -> None:
        self.tokens = tokens
        self.last_ms = time.monotonic_ns() // 1_000_000
        self.in_flight = 0


class RateLimiter:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._buckets: dict[str, _Bucket] = {}

    def _bucket(self, caller: str, cfg: RateLimitConfig) -> _Bucket:
        bucket = self._buckets.get(caller)
        if bucket is None:
            bucket = _Bucket(float(cfg.burst))
            self._buckets[caller] = bucket
        return bucket

    def _refill(self, bucket: _Bucket, cfg: RateLimitConfig, now: int) -> None:
        elapsed_ms = max(0, now - bucket.last_ms)
        bucket.last_ms = now
        bucket.tokens = min(
            float(cfg.burst),
            bucket.tokens + elapsed_ms * cfg.rate_per_sec / 1000.0,
        )

    def try_acquire(self, caller: str,
                    cfg: RateLimitConfig) -> RateDecision:
        with self._lock:
            now = time.monotonic_ns() // 1_000_000
            bucket = self._bucket(caller, cfg)
            self._refill(bucket, cfg, now)

            if bucket.tokens < 1.0:
                decision = RateDecision(
                    allowed=False,
                    reason=RejectReason.RATE_LIMITED,
                    tokens_after=bucket.tokens,
                    in_flight_after=bucket.in_flight,
                    detail=(f"tokens={bucket.tokens:.3f}<1 "
                            f"rate={cfg.rate_per_sec}/s burst={cfg.burst}"),
                )
                log_decision("rate_limit", caller=caller,
                             verdict="reject", reason=decision.reason.value,
                             tokens=f"{bucket.tokens:.3f}",
                             in_flight=bucket.in_flight)
                return decision

            if bucket.in_flight >= cfg.max_concurrency:
                decision = RateDecision(
                    allowed=False,
                    reason=RejectReason.CONCURRENCY_LIMITED,
                    tokens_after=bucket.tokens,
                    in_flight_after=bucket.in_flight,
                    detail=(f"in_flight={bucket.in_flight}>="
                            f"{cfg.max_concurrency}"),
                )
                log_decision("rate_limit", caller=caller,
                             verdict="reject",
                             reason=decision.reason.value,
                             tokens=f"{bucket.tokens:.3f}",
                             in_flight=bucket.in_flight)
                return decision

            bucket.tokens -= 1.0
            bucket.in_flight += 1
            decision = RateDecision(
                allowed=True,
                reason=None,
                tokens_after=bucket.tokens,
                in_flight_after=bucket.in_flight,
                detail="acquired",
            )
            log_decision("rate_limit", caller=caller, verdict="allow",
                         tokens=f"{bucket.tokens:.3f}",
                         in_flight=bucket.in_flight)
            return decision

    def release(self, caller: str) -> None:
        with self._lock:
            bucket = self._buckets.get(caller)
            if bucket is not None and bucket.in_flight > 0:
                bucket.in_flight -= 1
                log_decision("concurrency_release", caller=caller,
                             in_flight=bucket.in_flight)
