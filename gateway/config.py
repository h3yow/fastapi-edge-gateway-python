"""Configuration model for the edge gateway: routes, upstreams, rate limits."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class BalanceStrategy(str, Enum):
    ROUND_ROBIN = "round_robin"
    RANDOM = "random"


def prefix_matches(path: str, prefix: str) -> bool:
    """Whether ``path`` falls under ``prefix`` at a path-segment boundary.

    A prefix matches the exact path, or a longer path whose next character
    after the prefix (ignoring a single trailing ``/``) is ``/``. Hence
    ``/v1/report`` matches ``/v1/report`` and ``/v1/report/2026`` but never
    the sibling ``/v1/reporter``; a directory prefix ``/api/`` still matches
    ``/api/echo``. A prefix of ``/`` matches every absolute path.
    """
    base = prefix.rstrip("/")
    return path == base or path.startswith(base + "/")


@dataclass(frozen=True)
class UpstreamConfig:
    name: str
    base_url: str
    weight: int = 1
    fail_threshold: int = 1
    recover_threshold: int = 1


@dataclass(frozen=True)
class RouteConfig:
    name: str
    path_prefix: str
    upstream_names: tuple[str, ...]
    strategy: BalanceStrategy = BalanceStrategy.ROUND_ROBIN
    strip_prefix: str | None = None
    cache_ttl_ms: int = 0
    timeout_ms: int = 1000
    max_retries: int = 0  # retries only on safe methods / idempotent failures


@dataclass(frozen=True)
class RateLimitConfig:
    caller: str
    rate_per_sec: int
    burst: int
    max_concurrency: int


@dataclass(frozen=True)
class GatewayConfig:
    upstreams: tuple[UpstreamConfig, ...] = ()
    routes: tuple[RouteConfig, ...] = ()
    rate_limits: tuple[RateLimitConfig, ...] = ()
    default_rate_per_sec: int = 100
    default_burst: int = 100
    default_max_concurrency: int = 100

    def upstream(self, name: str) -> UpstreamConfig | None:
        for u in self.upstreams:
            if u.name == name:
                return u
        return None

    def route_for(self, path: str) -> RouteConfig | None:
        # Longest prefix wins so that more specific routes take precedence.
        # Matching is path-segment aware: "/v1/report" must not swallow the
        # sibling resource "/v1/reporter"; only an exact prefix match or a
        # match ending at a "/" segment boundary counts.
        best: RouteConfig | None = None
        for route in self.routes:
            if prefix_matches(path, route.path_prefix):
                if best is None or len(route.path_prefix) > len(best.path_prefix):
                    best = route
        return best

    def rate_limit_for(self, caller: str) -> RateLimitConfig:
        for rl in self.rate_limits:
            if rl.caller == caller:
                return rl
        return RateLimitConfig(
            caller=caller,
            rate_per_sec=self.default_rate_per_sec,
            burst=self.default_burst,
            max_concurrency=self.default_max_concurrency,
        )
