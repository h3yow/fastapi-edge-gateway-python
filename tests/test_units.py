"""Unit tests for token-bucket accuracy, concurrency accounting and keys."""
from __future__ import annotations

import threading
import time

from gateway.cache import ResponseCache
from gateway.config import RateLimitConfig
from gateway.ratelimit import RateLimiter, RejectReason


def test_token_bucket_burst_then_refill() -> None:
    rl = RateLimiter()
    cfg = RateLimitConfig("c", rate_per_sec=100, burst=5, max_concurrency=50)
    assert [rl.try_acquire("c", cfg).allowed for _ in range(5)] == [True] * 5
    sixth = rl.try_acquire("c", cfg)
    assert not sixth.allowed and sixth.reason == RejectReason.RATE_LIMITED
    time.sleep(0.03)  # ~3 tokens should have accrued
    assert rl.try_acquire("c", cfg).allowed


def test_parallel_burst_never_over_admits() -> None:
    rl = RateLimiter()
    cfg = RateLimitConfig("c", rate_per_sec=1_000_000, burst=1_000_000,
                          max_concurrency=37)
    admitted: list[int] = []
    lock = threading.Lock()

    def worker() -> None:
        if rl.try_acquire("c", cfg).allowed:
            with lock:
                admitted.append(1)

    threads = [threading.Thread(target=worker) for _ in range(300)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(admitted) == 37


def test_release_returns_slot_and_is_under_counted() -> None:
    rl = RateLimiter()
    cfg = RateLimitConfig("c", rate_per_sec=1_000_000, burst=1_000_000,
                          max_concurrency=2)
    assert rl.try_acquire("c", cfg).allowed
    assert rl.try_acquire("c", cfg).allowed
    denied = rl.try_acquire("c", cfg)
    assert not denied.allowed
    rl.release("c")
    assert rl.try_acquire("c", cfg).allowed


def test_cache_key_distinguishes_variants() -> None:
    base = dict(route_name="r", method="GET", path="/p")
    keys = {
        ResponseCache.make_key(caller="a", vary={"q": "1"}, **base),
        ResponseCache.make_key(caller="b", vary={"q": "1"}, **base),
        ResponseCache.make_key(caller="a", vary={"q": "2"}, **base),
        ResponseCache.make_key(caller="a", method="POST",
                               vary={"q": "1"},
                               route_name="r", path="/p"),
        ResponseCache.make_key(caller="a", vary={"q": "1"}, **base),
    }
    assert len(keys) == 4  # last duplicates the first
