import asyncio
import logging
import random
import time
from dataclasses import dataclass

import httpx

from ..config import Settings
from .breaker import CircuitBreaker
from .limiter import TokenBucket
from .providers import Provider, ProviderError

log = logging.getLogger("gateway.router")


class AllProvidersFailed(Exception):
    def __init__(self, errors: list[dict]):
        super().__init__("all providers failed")
        self.errors = errors


@dataclass
class RouteResult:
    data: dict
    provider: str
    attempts: int


def build_providers(s: Settings, a_limiter=None) -> list[Provider]:
    def breaker() -> CircuitBreaker:
        return CircuitBreaker(s.breaker_window, s.breaker_min_calls,
                              s.breaker_threshold, s.breaker_open_seconds)

    return [
        Provider("a", s.provider_a_url, s.a_timeout, 0, s.a_max_concurrency, breaker(),
                 limiter=a_limiter or TokenBucket(s.a_rate, s.a_burst), ewma_latency=0.7),
        Provider("b", s.provider_b_url, s.bc_timeout, 1, s.bc_max_concurrency, breaker(),
                 ewma_latency=5.0),
        Provider("c", s.provider_c_url, s.bc_timeout, 1, s.bc_max_concurrency, breaker(),
                 ewma_latency=5.0),
    ]


class Router:
    def __init__(self, client: httpx.AsyncClient, providers: list[Provider], s: Settings, metrics=None):
        self.client = client
        self.providers = providers
        self.s = s
        self.m = metrics

    def _count(self, provider: str, result: str, seconds: float | None = None) -> None:
        if self.m:
            self.m.attempts.labels(provider, result).inc()
            if seconds is not None:
                self.m.provider_seconds.labels(provider, result).observe(seconds)

    async def _pick(self, exclude: set[str]) -> Provider | None:
        # Preferred tier first (A); inside a tier, least-loaded / fastest first.
        ordered = sorted(self.providers,
                         key=lambda p: (p.priority, p.ewma_latency * (1 + p.inflight / 50)))
        for p in ordered:
            if p.name not in exclude and await p.try_enter():
                return p
        return None

    async def process(self, payload, trace_id: str) -> RouteResult:
        s = self.s
        deadline = time.monotonic() + s.request_deadline
        tried: set[str] = set()
        errors: list[dict] = []

        for attempt in range(1, s.max_attempts + 1):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            p = await self._pick(tried) or await self._pick(set())
            if p is None:
                errors.append({"error": "no_provider_available"})
                self._count("none", "no_provider_available")
                break  # everything saturated / open / rate-limited -> shed load fast

            started = time.perf_counter()
            try:
                data = await p.call(self.client, payload, trace_id, min(p.timeout, remaining))
                took = time.perf_counter() - started
                p.breaker.record(True)
                p.observe(took)
                self._count(p.name, "success", took)
                return RouteResult(data, p.name, attempt)
            except ProviderError as e:
                took = time.perf_counter() - started
                errors.append({"provider": p.name, "error": e.kind, "status": e.status})
                self._count(p.name, e.kind, took)
                log.warning("attempt_failed", extra={"provider": p.name, "kind": e.kind,
                                                     "attempt": attempt})
                if e.counts_against_health:
                    p.breaker.record(False)
                    p.observe(p.timeout)
                else:
                    p.breaker.neutral()
                if e.kind == "rate_limited" and p.limiter is not None:
                    await p.limiter.penalty(e.retry_after or 1.0)
                if not e.retryable:
                    break
                tried.add(p.name)
                if attempt < s.max_attempts:
                    await asyncio.sleep(random.uniform(0, s.backoff_base * 2 ** (attempt - 1)))
            except asyncio.CancelledError:
                p.breaker.neutral()
                raise
            finally:
                p.inflight -= 1

        raise AllProvidersFailed(errors)

    def snapshot(self) -> list[dict]:
        return [{"name": p.name, "breaker": p.breaker.state, "inflight": p.inflight,
                 "ewma_latency_s": round(p.ewma_latency, 3)} for p in self.providers]
