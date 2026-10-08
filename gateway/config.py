"""Configuration model for the edge gateway: routes, upstreams, rate limits."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


def _normalize_prefix(prefix: str) -> str:
    """Canonical route-prefix form: no trailing slash except for root."""
    if len(prefix) > 1:
        return prefix.rstrip("/") or "/"
    return prefix


def _prefix_matches(prefix: str, path: str) -> bool:
    """Segment-aware prefix match.

    ``/v1/report`` matches ``/v1/report`` and ``/v1/report/x`` but not
    ``/v1/reporter``: the next path character after the match must be a
    segment separator, not another word character.
    """
    if prefix == "/":
        return True
    if path == prefix:
        return True
    return path.startswith(prefix + "/")


class BalanceStrategy(str, Enum):
    ROUND_ROBIN = "round_robin"
    RANDOM = "random"


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
        # Longest *segment-aware* prefix wins so that more specific routes take
        # precedence. A prefix only matches on a path-segment boundary:
        # "/v1/report" matches "/v1/report" and "/v1/report/x" but never
        # "/v1/reporter" (adjacent spelling, different resource).
        best: RouteConfig | None = None
        best_len = -1
        for route in self.routes:
            prefix = _normalize_prefix(route.path_prefix)
            if not _prefix_matches(prefix, path):
                continue
            if len(prefix) > best_len:
                best, best_len = route, len(prefix)
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
