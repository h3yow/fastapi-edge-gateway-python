"""Runtime configuration holder with atomic, rollback-safe reloads.

A :class:`ConfigManager` holds the single active :class:`Snapshot`. Every
request pins the snapshot it starts with, so route lookup, rate-limit
decision and cache lookup for one request always derive from the same
immutable config generation. Reload validates a full new document before
touching the active pointer: either the whole new generation takes effect at
once or the old one keeps serving unchanged.

Each generation owns a fresh response cache and its own upstream selector,
which guarantees that an old cache entry can never be served after the rules
that produced it were replaced. The health tracker and rate-limiter buckets
are shared across generations (they are keyed by upstream name / caller id
and describe runtime state, not routing rules).
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass

from .cache import ResponseCache
from .config import GatewayConfig
from .health import HealthTracker
from .logging_utils import log_decision
from .router import UpstreamSelector


@dataclass(frozen=True)
class Snapshot:
    """One immutable, fully wired configuration generation."""
    config: GatewayConfig
    version: str
    revision: int
    content_sha256: str
    source: str
    cache: ResponseCache
    selector: UpstreamSelector


class ReloadError(Exception):
    """Raised when a reload cannot be applied; active snapshot is untouched."""

    def __init__(self, reasons: list[str]) -> None:
        self._reasons = reasons
        super().__init__("reload rejected: " + "; ".join(reasons))

    @property
    def reasons(self) -> list[str]:
        return list(self._reasons)


@dataclass(frozen=True)
class ActiveInfo:
    version: str
    revision: int
    content_sha256: str
    source: str
    loaded_at_ms: int


def _now_ms() -> int:
    return time.monotonic_ns() // 1_000_000


class ConfigManager:
    """Holds the active generation and swaps it atomically on reload."""

    def __init__(self, config: GatewayConfig, health: HealthTracker,
                 *, version: str = "",
                 content_sha256: str = "",
                 source: str = "code:built-in-defaults") -> None:
        self._health = health
        self._lock = threading.Lock()
        self._loaded_at = _now_ms()
        snapshot = self._make_snapshot(
            config, revision=1,
            version=version or "built-in-r1",
            content_sha256=content_sha256,
            source=source,
        )
        self._active = snapshot
        log_decision(
            "config_loaded", revision=1, version=snapshot.version,
            sha=content_sha256[:12], source=source,
            routes=len(config.routes), upstreams=len(config.upstreams),
        )

    def _make_snapshot(self, config: GatewayConfig, *, revision: int,
                       version: str, content_sha256: str,
                       source: str) -> Snapshot:
        cache = ResponseCache()
        selector = UpstreamSelector(config, self._health)
        return Snapshot(
            config=config,
            version=version,
            revision=revision,
            content_sha256=content_sha256,
            source=source,
            cache=cache,
            selector=selector,
        )

    def active(self) -> Snapshot:
        """Return the currently serving generation (cheap, thread-safe)."""
        return self._active

    def info(self) -> ActiveInfo:
        """Return version metadata of the active generation."""
        snapshot = self._active
        return ActiveInfo(
            version=snapshot.version,
            revision=snapshot.revision,
            content_sha256=snapshot.content_sha256,
            source=snapshot.source,
            loaded_at_ms=self._loaded_at,
        )

    def reload(self, config: GatewayConfig, *, version: str,
               content_sha256: str, source: str) -> Snapshot:
        """Atomically install a validated new generation.

        The document must already have been fully validated; this method only
        wires and swaps. Re-loading byte-identical content is a no-op and
        returns the current snapshot. On any failure the previous generation
        keeps serving and a :class:`ReloadError` is raised.
        """
        with self._lock:
            current = self._active
            if content_sha256 and content_sha256 == current.content_sha256:
                log_decision(
                    "config_reload", revision=current.revision,
                    version=current.version, sha=content_sha256[:12],
                    verdict="unchanged",
                )
                return current

            next_revision = current.revision + 1
            label = version or f"auto-r{next_revision}"
            try:
                new_snapshot = self._make_snapshot(
                    config, revision=next_revision, version=label,
                    content_sha256=content_sha256, source=source,
                )
            except Exception as exc:  # wiring must never half-apply
                log_decision(
                    "config_reload_rejected",
                    old_revision=current.revision, reason=str(exc),
                )
                raise ReloadError([f"failed to wire new config: {exc}"]) \
                    from None
            # Single pointer publish: after this line every newly admitted
            # request sees the entire new generation at once.
            self._active = new_snapshot
            self._loaded_at = _now_ms()
            log_decision(
                "config_reload", old_revision=current.revision,
                revision=next_revision, old_version=current.version,
                version=label, sha=content_sha256[:12], source=source,
                routes=len(config.routes),
                upstreams=len(config.upstreams), verdict="applied",
            )
            return new_snapshot
