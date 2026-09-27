"""上游池：健康跟踪与轮询选择。"""
from __future__ import annotations

import enum
import logging
import threading
import time
from dataclasses import dataclass, field

from .config import UpstreamConfig

logger = logging.getLogger("gateway.upstream")

FAILURE_THRESHOLD = 1  # 一次连接失败/超时/可重试 5xx 即剔除
RECOVERY_COOLDOWN = 0.3  # 剔除后冷却秒数，过后允许一次探测请求


class Health(str, enum.Enum):
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"


@dataclass
class Upstream:
    config: UpstreamConfig
    health: Health = Health.HEALTHY
    consecutive_failures: int = 0
    opened_at: float = 0.0  # 被剔除的时刻（熔断打开）

    @property
    def name(self) -> str:
        return self.config.name


class UpstreamPool:
    """一条路由对应的上游集合，按轮询在健康实例间分发。

    健康状态变化：
    - 连续失败达到阈值 -> 剔除（UNHEALTHY / 熔断打开），不再参与常规分发；
    - 冷却期过后允许放行一次探测（半开）：成功则恢复并重新纳入，
      失败则重新进入冷却。
    """

    def __init__(self, configs: list[UpstreamConfig], cooldown: float = RECOVERY_COOLDOWN) -> None:
        self._upstreams: list[Upstream] = [Upstream(c) for c in configs]
        self._cursor = 0
        self._lock = threading.Lock()
        self._cooldown = cooldown
        self._probing: set[str] = set()  # 正在半开探测中的上游

    def select(self) -> Upstream | None:
        """轮询选择下一个健康上游；冷却到期则优先放行一个恢复探测。"""
        with self._lock:
            now = time.monotonic()
            # 半开探测：为一个冷却到期的不健康上游放行一次
            for u in self._upstreams:
                if (
                    u.health is Health.UNHEALTHY
                    and u.name not in self._probing
                    and now - u.opened_at >= self._cooldown
                ):
                    self._probing.add(u.name)
                    logger.info(
                        "upstream select: probing recovering upstream=%s (half-open)",
                        u.name,
                    )
                    return u

            healthy = [u for u in self._upstreams if u.health is Health.HEALTHY]
            if not healthy:
                logger.info("upstream select: no healthy upstream available")
                return None
            chosen = healthy[self._cursor % len(healthy)]
            self._cursor += 1
            logger.info(
                "upstream select: chose=%s healthy=%s",
                chosen.name,
                [u.name for u in healthy],
            )
            return chosen

    def mark_failure(self, upstream: Upstream) -> None:
        with self._lock:
            was_probing = upstream.name in self._probing
            self._probing.discard(upstream.name)
            upstream.consecutive_failures += 1
            if (
                upstream.health is Health.HEALTHY
                and upstream.consecutive_failures >= FAILURE_THRESHOLD
            ):
                upstream.health = Health.UNHEALTHY
                upstream.opened_at = time.monotonic()
                logger.warning(
                    "upstream %s marked UNHEALTHY after %d consecutive failures",
                    upstream.name,
                    upstream.consecutive_failures,
                )
            elif was_probing:
                # 半开探测失败，重新冷却
                upstream.opened_at = time.monotonic()
                logger.warning("upstream %s probe failed, re-entering cooldown", upstream.name)

    def mark_success(self, upstream: Upstream) -> None:
        with self._lock:
            was_probing = upstream.name in self._probing
            self._probing.discard(upstream.name)
            if upstream.health is not Health.HEALTHY or was_probing:
                logger.warning(
                    "upstream %s probe succeeded -> recovered HEALTHY", upstream.name
                )
            upstream.consecutive_failures = 0
            upstream.health = Health.HEALTHY

    def healthy(self) -> list[Upstream]:
        return [u for u in self._upstreams if u.health is Health.HEALTHY]

    def all(self) -> list[Upstream]:
        return list(self._upstreams)
