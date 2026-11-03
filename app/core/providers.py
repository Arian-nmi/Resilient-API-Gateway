from dataclasses import dataclass

import httpx

from .breaker import CircuitBreaker


class ProviderError(Exception):
    """kind: timeout | network | http_5xx | rate_limited | bad_response | client_error"""

    def __init__(self, kind: str, *, status: int | None = None,
                 retry_after: float | None = None, retryable: bool = True):
        super().__init__(kind)
        self.kind = kind
        self.status = status
        self.retry_after = retry_after
        self.retryable = retryable

    @property
    def counts_against_health(self) -> bool:
        return self.kind in ("timeout", "network", "http_5xx", "bad_response")


@dataclass
class Provider:
    name: str
    url: str
    timeout: float
    priority: int                # lower = preferred
    max_concurrency: int
    breaker: CircuitBreaker
    limiter: object | None = None    # TokenBucket or RedisTokenBucket (async acquire/penalty)
    ewma_latency: float = 1.0
    inflight: int = 0

    async def try_enter(self) -> bool:
        """Non-blocking admission: concurrency cap, circuit breaker, rate limit.
        The slot is reserved *before* awaiting Redis so concurrent tasks can't overshoot the cap."""
        if self.inflight >= self.max_concurrency:
            return False
        self.inflight += 1
        if not self.breaker.allow():
            self.inflight -= 1
            return False
        if self.limiter is not None and not await self.limiter.acquire():
            self.breaker.neutral()  # release a half-open probe slot we did not use
            self.inflight -= 1
            return False
        return True

    def observe(self, seconds: float) -> None:
        self.ewma_latency = 0.8 * self.ewma_latency + 0.2 * seconds

    async def call(self, client: httpx.AsyncClient, payload, trace_id: str, timeout: float) -> dict:
        body = {} if payload is None else {"payload": payload}
        try:
            r = await client.post(self.url, json=body, headers={"X-Trace-Id": trace_id},
                                  timeout=timeout)
        except httpx.TimeoutException:
            raise ProviderError("timeout")
        except httpx.HTTPError:
            raise ProviderError("network")

        if r.status_code == 200:
            try:
                return r.json()
            except ValueError:
                raise ProviderError("bad_response", status=200)
        if r.status_code == 429:
            try:
                ra = float(r.headers.get("retry-after", ""))
            except ValueError:
                ra = None
            raise ProviderError("rate_limited", status=429, retry_after=ra)
        if r.status_code >= 500:
            raise ProviderError("http_5xx", status=r.status_code)
        raise ProviderError("client_error", status=r.status_code, retryable=False)
