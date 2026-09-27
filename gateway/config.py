"""网关配置模型与加载。"""
from __future__ import annotations

from pydantic import BaseModel, Field


class UpstreamConfig(BaseModel):
    name: str
    base_url: str  # 例如 http://127.0.0.1:9001


class RateLimitConfig(BaseModel):
    rate_per_second: float = 10.0   # 令牌桶速率
    burst: int = 20                 # 桶容量
    max_concurrency: int = 10       # 每调用方最大并发


class CacheConfig(BaseModel):
    enabled: bool = True
    ttl_seconds: float = 30.0
    max_entries: int = 1024


class RetryConfig(BaseModel):
    max_attempts: int = 2           # 仅对安全（可重放）请求生效
    timeout_seconds: float = 2.0
    health_cooldown: float = 0.3    # 不健康上游的恢复探测冷却（秒）


class RouteConfig(BaseModel):
    path_prefix: str                # 例如 /api/orders
    upstreams: list[UpstreamConfig] = Field(default_factory=list)
    rate_limit: RateLimitConfig = Field(default_factory=RateLimitConfig)
    cache: CacheConfig = Field(default_factory=CacheConfig)
    retry: RetryConfig = Field(default_factory=RetryConfig)


class GatewayConfig(BaseModel):
    routes: list[RouteConfig] = Field(default_factory=list)
