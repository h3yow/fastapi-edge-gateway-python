"""Response cache for safe (GET/HEAD) responses.

Cache keys include caller, route, normalized path and the Vary request
variants (query, selected headers, accepted encoding), so two callers or two
variants never share an entry. Only cacheable upstream responses are stored.
"""
from __future__ import annotations

import hashlib
import threading
import time
from dataclasses import dataclass

from .logging_utils import log_decision

CACHEABLE_METHODS = frozenset({"GET", "HEAD"})
SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})


def now_ms() -> int:
    return time.monotonic_ns() // 1_000_000


@dataclass(frozen=True)
class CacheEntry:
    status_code: int
    body: bytes
    headers: tuple[tuple[str, str], ...]
    stored_ms: int
    ttl_ms: int
    upstream_name: str = ""

    def fresh_at(self, ts_ms: int) -> bool:
        return ts_ms - self.stored_ms < self.ttl_ms


class ResponseCache:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._entries: dict[str, CacheEntry] = {}
        self._tags: dict[str, tuple[str, str]] = {}

    @staticmethod
    def make_key(caller: str, route_name: str, method: str, path: str,
                 vary: dict[str, str]) -> str:
        digest = hashlib.sha256()
        digest.update(caller.encode())
        digest.update(b"|")
        digest.update(route_name.encode())
        digest.update(b"|")
        digest.update(method.encode())
        digest.update(b"|")
        digest.update(path.encode())
        digest.update(b"|")
        for name in sorted(vary):
            digest.update(f"{name.lower()}={vary[name]};".encode())
        return digest.hexdigest()

    def get(self, key: str) -> CacheEntry | None:
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                log_decision("cache", key=key[:12], verdict="miss",
                             reason="no_entry")
                return None
            if not entry.fresh_at(now_ms()):
                del self._entries[key]
                self._tags.pop(key, None)
                log_decision("cache", key=key[:12], verdict="expired",
                             ttl_ms=entry.ttl_ms)
                return None
            log_decision("cache", key=key[:12], verdict="hit",
                         ttl_ms=entry.ttl_ms)
            return entry

    def put(self, key: str, entry: CacheEntry, caller: str = "",
            route_name: str = "") -> None:
        with self._lock:
            self._entries[key] = entry
            self._tags[key] = (caller, route_name)
        log_decision("cache", key=key[:12], verdict="store",
                     ttl_ms=entry.ttl_ms, status=entry.status_code)

    def invalidate(self, caller: str | None = None,
                   route_name: str | None = None) -> int:
        with self._lock:
            victims = [
                k for k, (c, r) in self._tags.items()
                if k in self._entries
                and (caller is None or c == caller)
                and (route_name is None or r == route_name)
            ]
            for k in victims:
                self._entries.pop(k, None)
                self._tags.pop(k, None)
        log_decision("cache", verdict="invalidate", removed=len(victims),
                     caller=caller or "*", route=route_name or "*")
        return len(victims)

    def clear(self) -> None:
        with self._lock:
            count = len(self._entries)
            self._entries.clear()
            self._tags.clear()
        log_decision("cache", verdict="clear", removed=count)
