"""Per-upstream health tracking with simple failure counting and probing.

An upstream is marked unhealthy after ``fail_threshold`` consecutive failures.
While unhealthy it is excluded from selection. ``recover_threshold``
consecutive successful probes bring it back into rotation. A probe is allowed
once every ``probe_interval_ms`` (half-open style) so a dead upstream does not
get hammered.
"""
from __future__ import annotations

import threading
import time

from .config import UpstreamConfig
from .logging_utils import log_decision


def now_ms() -> int:
    return time.monotonic_ns() // 1_000_000


class HealthTracker:
    def __init__(self, probe_interval_ms: int = 500) -> None:
        self._lock = threading.Lock()
        self._fails: dict[str, int] = {}
        self._probes_ok: dict[str, int] = {}
        self._healthy: dict[str, bool] = {}
        self._last_probe: dict[str, int] = {}
        self._probe_interval_ms = probe_interval_ms

    def is_healthy(self, name: str) -> bool:
        with self._lock:
            return self._healthy.get(name, True)

    def healthy_names(self, names: list[str]) -> list[str]:
        with self._lock:
            return [n for n in names if self._healthy.get(n, True)]

    def allow_probe(self, name: str, ts_ms: int | None = None) -> bool:
        """Whether an unhealthy upstream may receive one trial request now."""
        ts_ms = now_ms() if ts_ms is None else ts_ms
        with self._lock:
            if self._healthy.get(name, True):
                return True
            last = self._last_probe.get(name, -10**12)
            return ts_ms - last >= self._probe_interval_ms

    def mark_probe_sent(self, name: str) -> None:
        with self._lock:
            self._last_probe[name] = now_ms()

    def record_success(self, upstream: UpstreamConfig) -> None:
        with self._lock:
            was_healthy = self._healthy.get(upstream.name, True)
            self._fails[upstream.name] = 0
            if was_healthy:
                return
            ok = self._probes_ok.get(upstream.name, 0) + 1
            self._probes_ok[upstream.name] = ok
            if ok >= upstream.recover_threshold:
                self._healthy[upstream.name] = True
                self._probes_ok[upstream.name] = 0
                log_decision(
                    "upstream_recovered",
                    upstream=upstream.name,
                    reason=f"probe_success>={upstream.recover_threshold}",
                )

    def record_failure(self, upstream: UpstreamConfig) -> None:
        with self._lock:
            was_healthy = self._healthy.get(upstream.name, True)
            self._probes_ok[upstream.name] = 0
            fails = self._fails.get(upstream.name, 0) + 1
            self._fails[upstream.name] = fails
            if was_healthy and fails >= upstream.fail_threshold:
                self._healthy[upstream.name] = False
                log_decision(
                    "upstream_removed",
                    upstream=upstream.name,
                    reason=f"consecutive_fails>={upstream.fail_threshold}",
                )

    def snapshot(self) -> dict[str, bool]:
        with self._lock:
            return dict(self._healthy)
