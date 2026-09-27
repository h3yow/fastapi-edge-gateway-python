"""网关错误类型：每种拒绝/失败都有可区分的原因码。"""
from __future__ import annotations

from enum import Enum


class ErrorReason(str, Enum):
    ROUTE_NOT_FOUND = "route_not_found"          # 路由缺失
    NO_HEALTHY_UPSTREAM = "no_healthy_upstream"  # 全部上游不可用
    RATE_LIMITED = "rate_limited"                # 限流拒绝
    CONCURRENCY_LIMITED = "concurrency_limited"  # 并发超限拒绝
    UPSTREAM_TIMEOUT = "upstream_timeout"        # 上游超时（重试耗尽）
    UPSTREAM_ERROR = "upstream_error"            # 上游返回错误（重试耗尽）
    NON_REPLAYABLE = "non_replayable"            # 非安全请求不允许重试


class GatewayError(Exception):
    """网关层拒绝/失败，携带可区分原因与 HTTP 状态码。"""

    def __init__(self, reason: ErrorReason, status_code: int, detail: str = "") -> None:
        self.reason = reason
        self.status_code = status_code
        self.detail = detail or reason.value
        super().__init__(f"{reason.value}: {self.detail}")
