"""HTTP forwarding with timeout, safe-only retries and failover.

Retry rules (designed to avoid duplicate side effects):
* Only safe, replayable methods (GET/HEAD/OPTIONS) are ever retried.
* Connect errors / timeouts / HTTP 502/503/504 trigger another attempt.
* The first attempt targets the selected upstream; subsequent attempts
  fail over to a *different* healthy upstream.
* Non-safe methods (POST/PUT/PATCH/DELETE) get exactly one attempt.
"""
from __future__ import annotations

from dataclasses import dataclass

import httpx

from .cache import SAFE_METHODS
from .config import RouteConfig
from .health import HealthTracker
from .logging_utils import log_decision
from .router import UpstreamPick, UpstreamSelector

RETRIABLE_STATUSES = frozenset({502, 503, 504})


@dataclass(frozen=True)
class ForwardResult:
    status_code: int
    content: bytes
    headers: tuple[tuple[str, str], ...]
    upstream_name: str
    attempts: int
    error: str | None = None  # set when no upstream could serve the request


class Forwarder:
    def __init__(self, health: HealthTracker,
                 client: httpx.Client | None = None) -> None:
        self._health = health
        self._client = client or httpx.Client()
        self._owns_client = client is None

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def forward(self, selector: UpstreamSelector, route: RouteConfig,
                method: str, path: str, headers: dict[str, str],
                content: bytes, query: str = "") -> ForwardResult:
        method = method.upper()
        attempts_limit = (
            route.max_retries + 1
            if method in SAFE_METHODS and route.max_retries > 0
            else 1
        )
        tried: set[str] = set()
        last_error = "no_attempt"
        last_failure: ForwardResult | None = None

        for attempt in range(1, attempts_limit + 1):
            # Only a replayable request may be used as a recovery probe when
            # every known upstream is unhealthy. Already-tried upstreams are
            # excluded so a retry can never replay against the same node.
            pick = selector.pick(
                route,
                allow_probe=method in SAFE_METHODS,
                exclude=frozenset(tried),
            )
            if pick.upstream is None:
                last_error = pick.reason
                log_decision("forward", route=route.name,
                             verdict="no_upstream", reason=pick.reason,
                             attempt=attempt)
                if last_failure is not None:
                    # We already reached an upstream at least once; surface
                    # that concrete failure rather than a generic 503.
                    return last_failure
                break

            tried.add(pick.upstream.name)
            result = self._attempt_once(
                route, method, path, headers, content, query, pick
            )
            retriable = method in SAFE_METHODS and (
                result.error is not None
                or result.status_code in RETRIABLE_STATUSES
            )
            if not retriable:
                return result
            last_failure = result
            last_error = result.error or f"status_{result.status_code}"
            if attempt >= attempts_limit:
                return result
            log_decision("failover", route=route.name,
                         failed=pick.upstream.name,
                         reason=last_error, next_attempt=attempt + 1)

        return ForwardResult(503, b"", (), upstream_name="-", attempts=0,
                             error=last_error)

    def _rewrite_path(self, route: RouteConfig, path: str) -> str:
        strip = route.strip_prefix
        if strip and path.startswith(strip):
            rewritten = path[len(strip):] or "/"
            return rewritten if rewritten.startswith("/") else "/" + rewritten
        return path

    def _attempt_once(self, route: RouteConfig, method: str, path: str,
                      headers: dict[str, str], content: bytes,
                      query: str, pick: UpstreamPick) -> ForwardResult:
        assert pick.upstream is not None
        upstream = pick.upstream
        url = upstream.base_url.rstrip("/") + self._rewrite_path(route, path)
        if query:
            url += "?" + query
        log_decision("forward", route=route.name, upstream=upstream.name,
                     method=method, url=url,
                     probe=int(pick.probe_trial))
        try:
            response = self._client.request(
                method=method,
                url=url,
                headers=headers,
                content=content,
                timeout=httpx.Timeout(route.timeout_ms / 1000.0),
            )
        except (httpx.TimeoutException, httpx.ConnectError,
                httpx.NetworkError) as exc:
            self._health.record_failure(upstream)
            kind = type(exc).__name__
            log_decision("upstream_failure", upstream=upstream.name,
                         reason=kind)
            return ForwardResult(504, str(exc).encode(), (),
                                 upstream_name=upstream.name, attempts=1,
                                 error=kind)

        if response.status_code in RETRIABLE_STATUSES:
            self._health.record_failure(upstream)
        else:
            self._health.record_success(upstream)
        return ForwardResult(
            status_code=response.status_code,
            content=response.content,
            headers=tuple(response.headers.items()),
            upstream_name=upstream.name,
            attempts=1,
        )
