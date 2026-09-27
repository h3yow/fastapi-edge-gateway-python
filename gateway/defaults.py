"""Default gateway configuration bound to a running upstream cluster."""
from __future__ import annotations

from .config import (
    BalanceStrategy, GatewayConfig, RateLimitConfig, RouteConfig,
    UpstreamConfig,
)


def build_default_config(base_urls: dict[str, str],
                         echo_cache_ttl_ms: int = 2000) -> GatewayConfig:
    upstreams = tuple(
        UpstreamConfig(
            name=name,
            base_url=url,
            fail_threshold=1,
            recover_threshold=1,
        )
        for name, url in base_urls.items()
    )
    names = tuple(base_urls)
    routes = (
        RouteConfig(
            name="echo",
            path_prefix="/api/",
            upstream_names=names,
            strategy=BalanceStrategy.ROUND_ROBIN,
            strip_prefix="/api",
            cache_ttl_ms=echo_cache_ttl_ms,
            timeout_ms=800,
            max_retries=2,
        ),
        RouteConfig(
            name="effects",
            path_prefix="/effects",
            upstream_names=names,
            strategy=BalanceStrategy.ROUND_ROBIN,
            strip_prefix="/effects",
            cache_ttl_ms=0,
            timeout_ms=800,
            max_retries=2,
        ),
        RouteConfig(
            name="slow",
            path_prefix="/slow",
            upstream_names=names,
            strategy=BalanceStrategy.ROUND_ROBIN,
            strip_prefix="",
            cache_ttl_ms=0,
            timeout_ms=300,
            max_retries=1,
        ),
        RouteConfig(
            name="hold",
            path_prefix="/hold",
            upstream_names=names,
            strategy=BalanceStrategy.ROUND_ROBIN,
            strip_prefix="",
            cache_ttl_ms=0,
            timeout_ms=1500,
            max_retries=0,
        ),
        RouteConfig(
            name="fail",
            path_prefix="/fail",
            upstream_names=names,
            strategy=BalanceStrategy.ROUND_ROBIN,
            strip_prefix="",
            cache_ttl_ms=0,
            timeout_ms=800,
            max_retries=2,
        ),
    )
    rate_limits = (
        RateLimitConfig("premium", rate_per_sec=100, burst=100,
                        max_concurrency=20),
        RateLimitConfig("burst-demo", rate_per_sec=2, burst=5,
                        max_concurrency=3),
    )
    return GatewayConfig(
        upstreams=upstreams,
        routes=routes,
        rate_limits=rate_limits,
        default_rate_per_sec=20,
        default_burst=20,
        default_max_concurrency=10,
    )
