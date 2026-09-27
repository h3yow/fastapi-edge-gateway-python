"""响应缓存：键区分调用方与请求变体，命中带可解释标记。"""
from __future__ import annotations

import hashlib
import logging
import time
from dataclasses import dataclass

logger = logging.getLogger("gateway.cache")


@dataclass
class CacheEntry:
    status_code: int
    headers: dict[str, str]
    body: bytes
    expires_at: float


class ResponseCache:
    """简单内存缓存，TTL 过期 + 容量上限（最简 LRU：命中后放到队尾）。"""

    def __init__(self, ttl_seconds: float, max_entries: int = 1024) -> None:
        self._ttl = ttl_seconds
        self._max = max_entries
        self._store: dict[str, CacheEntry] = {}
        self._order: list[str] = []

    @staticmethod
    def make_key(caller: str, method: str, path: str, query: str, body: bytes) -> str:
        """缓存键 = 调用方 + 方法 + 路径 + 查询串 + 请求体哈希。

        不同调用方的同路径请求互不可见；query/body 任一不同都视为不同变体。
        """
        digest = hashlib.sha256(body).hexdigest()
        key = f"{caller}|{method.upper()}|{path}|{query}|body={digest}"
        logger.info("cache key caller=%s method=%s path=%s query=%s -> %s",
                    caller, method, path, query, key)
        return key

    def get(self, key: str) -> CacheEntry | None:
        entry = self._store.get(key)
        now = time.monotonic()
        if entry is None:
            logger.info("cache decision=MISS key=%s reason=no_entry", key)
            return None
        if now >= entry.expires_at:
            logger.info("cache decision=MISS key=%s reason=expired", key)
            self._delete(key)
            return None
        # 命中，更新 LRU 顺序
        self._order.remove(key)
        self._order.append(key)
        remaining = entry.expires_at - now
        logger.info("cache decision=HIT key=%s ttl_remaining=%.2fs", key, remaining)
        return entry

    def put(self, key: str, entry: CacheEntry) -> None:
        self._store[key] = entry
        if key in self._order:
            self._order.remove(key)
        self._order.append(key)
        while len(self._order) > self._max:
            oldest = self._order.pop(0)
            self._store.pop(oldest, None)
        logger.info("cache decision=STORE key=%s ttl=%.2fs", key, self._ttl)

    def _delete(self, key: str) -> None:
        self._store.pop(key, None)
        if key in self._order:
            self._order.remove(key)
