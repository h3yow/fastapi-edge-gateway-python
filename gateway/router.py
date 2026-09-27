"""Health-aware upstream selection for a route (round-robin / random)."""
from __future__ import annotations

import itertools
import random
import threading
from dataclasses import dataclass

from .config import BalanceStrategy, GatewayConfig, RouteConfig, UpstreamConfig
from .health import HealthTracker
from .logging_utils import log_decision


@dataclass(frozen=True)
class UpstreamPick:
    upstream: UpstreamConfig | None
    probe_trial: bool  # True when picked only as a half-open recovery probe
    reason: str


class UpstreamSelector:
    def __init__(self, config: GatewayConfig, health: HealthTracker,
                 rng: random.Random | None = None) -> None:
        self._config = config
        self._health = health
        self._lock = threading.Lock()
        self._counters: dict[str, itertools.count] = {}
        self._rng = rng or random.Random()

    def _counter(self, route_name: str) -> itertools.count:
        counter = self._counters.get(route_name)
        if counter is None:
            counter = itertools.count()
            self._counters[route_name] = counter
        return counter

    def pick(self, route: RouteConfig, allow_probe: bool = True,
             exclude: frozenset[str] = frozenset()) -> UpstreamPick:
        configured = [self._config.upstream(n) for n in route.upstream_names]
        configured = [u for u in configured if u is not None]
        if not configured:
            return UpstreamPick(None, False, "route_has_no_upstream")

        healthy_names = self._health.healthy_names(
            [u.name for u in configured]
        )
        healthy_names = [n for n in healthy_names if n not in exclude]

        # Opportunistic recovery: when at least one healthy node remains (so a
        # failed probe cannot break the request), a replayable request may be
        # steered to an unhealthy node whose probe cooldown has elapsed.
        if healthy_names and allow_probe:
            unhealthy = [
                u for u in configured
                if u.name not in healthy_names and u.name not in exclude
            ]
            for candidate in unhealthy:
                if self._health.allow_probe(candidate.name):
                    self._health.mark_probe_sent(candidate.name)
                    log_decision(
                        "upstream_select",
                        route=route.name,
                        picked=candidate.name,
                        healthy=",".join(healthy_names),
                        strategy=route.strategy.value,
                        reason="staggered_recovery_probe",
                    )
                    return UpstreamPick(candidate, True, "recovery_probe")

        if healthy_names:
            picked = self._pick_healthy(
                route,
                [u for u in configured if u.name in healthy_names],
            )
            log_decision(
                "upstream_select",
                route=route.name,
                picked=picked.name,
                healthy=",".join(healthy_names),
                strategy=route.strategy.value,
                reason="healthy_candidate",
            )
            return UpstreamPick(picked, False, "healthy_candidate")

        # No healthy upstream: for replayable requests, allow one half-open
        # trial probe at a time; other requests are rejected outright so an
        # unsafe request never creates an ambiguous side effect.
        if allow_probe:
            return self._pick_probe(route, exclude)
        log_decision(
            "upstream_select",
            route=route.name,
            picked="-",
            reason="all_upstreams_unhealthy",
        )
        return UpstreamPick(None, False, "all_upstreams_unhealthy")

    def _pick_healthy(self, route: RouteConfig,
                      healthy: list[UpstreamConfig]) -> UpstreamConfig:
        if route.strategy == BalanceStrategy.RANDOM:
            total = sum(max(u.weight, 1) for u in healthy)
            point = self._rng.uniform(0, total)
            upto = 0.0
            for u in healthy:
                upto += max(u.weight, 1)
                if point <= upto:
                    return u
            return healthy[-1]
        with self._lock:
            step = next(self._counter(route.name))
        return healthy[step % len(healthy)]

    def _pick_probe(self, route: RouteConfig,
                    exclude: frozenset[str] = frozenset()) -> UpstreamPick:
        for name in route.upstream_names:
            if name in exclude:
                continue
            upstream = self._config.upstream(name)
            if upstream is None:
                continue
            if self._health.allow_probe(name):
                self._health.mark_probe_sent(name)
                log_decision(
                    "upstream_select",
                    route=route.name,
                    picked=name,
                    strategy=route.strategy.value,
                    reason="recovery_probe_all_unhealthy",
                )
                return UpstreamPick(upstream, True, "recovery_probe")
        log_decision(
            "upstream_select",
            route=route.name,
            picked="-",
            reason="all_upstreams_unhealthy",
        )
        return UpstreamPick(None, False, "all_upstreams_unhealthy")
