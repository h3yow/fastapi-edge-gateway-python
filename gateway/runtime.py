"""Atomic runtime configuration holder.

Every request captures the current :class:`ConfigRuntime` once, so routing,
rate limiting, cache lookup and forwarding for that request are all evaluated
against the same immutable configuration version — even while a reload swaps
in a new version concurrently. In-flight requests keep using the old runtime
(and its cache) to completion; new requests see the new runtime immediately.
"""
from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import TYPE_CHECKING

import httpx

from .cache import ResponseCache
from .config import GatewayConfig
from .forwarder import Forwarder
from .health import HealthTracker
from .router import UpstreamSelector

if TYPE_CHECKING:
    pass


@dataclass
class ConfigRuntime:
    """Immutable bundle of one config version and its per-version components.

    ``cache`` and ``selector`` are version-private: replacing the runtime with
    a fresh cache guarantees an entry stored under old rules can never be
    served after new rules take effect.
    """
    config: GatewayConfig
    version: str
    revision: int
    source_path: str
    loaded_at_ms: int
    content_hash: str
    cache: ResponseCache
    selector: UpstreamSelector
    forwarder: Forwarder


def build_runtime(config: GatewayConfig, *, version: str, revision: int,
                  source_path: str, loaded_at_ms: int, content_hash: str,
                  health: HealthTracker,
                  client: httpx.Client | None) -> ConfigRuntime:
    """Assemble one runtime bundle.

    Selector and cache are fresh per version; the httpx client and health
    tracker are shared across reloads (connection pooling and circuit-breaker
    state survive a reload).
    """
    cache = ResponseCache()
    selector = UpstreamSelector(config, health)
    forwarder = Forwarder(health, client)
    return ConfigRuntime(
        config=config, version=version, revision=revision,
        source_path=source_path, loaded_at_ms=loaded_at_ms,
        content_hash=content_hash, cache=cache, selector=selector,
        forwarder=forwarder,
    )


class ConfigHolder:
    """Holds the active runtime; replacement is an atomic reference swap."""

    def __init__(self, initial: ConfigRuntime) -> None:
        self._lock = threading.Lock()
        self._runtime = initial

    def current(self) -> ConfigRuntime:
        with self._lock:
            return self._runtime

    def replace(self, runtime: ConfigRuntime) -> None:
        with self._lock:
            old = self._runtime
            self._runtime = runtime
        # Logging and old-cache cleanup stay outside the swap critical path;
        # the reference itself was already exchanged atomically.
        old.cache.clear()
