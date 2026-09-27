"""路由表：按路径前缀最长匹配路由。"""
from __future__ import annotations

import logging

from .config import RouteConfig
from .upstream import UpstreamPool

logger = logging.getLogger("gateway.router")


class Route:
    def __init__(self, config: RouteConfig) -> None:
        self.config = config
        self.pool = UpstreamPool(config.upstreams, cooldown=config.retry.health_cooldown)


class RouteTable:
    def __init__(self, routes: list[RouteConfig]) -> None:
        # 长前缀优先，保证 /api/orders 比 /api 更先匹配
        self._routes: list[Route] = sorted(
            (Route(r) for r in routes), key=lambda r: len(r.config.path_prefix), reverse=True
        )

    def match(self, path: str) -> Route | None:
        for route in self._routes:
            prefix = route.config.path_prefix
            if path == prefix or path.startswith(prefix.rstrip("/") + "/"):
                logger.info("route match: path=%s -> %s", path, prefix)
                return route
        logger.info("route match: path=%s -> NOT FOUND", path)
        return None
