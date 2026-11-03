import logging
import time

log = logging.getLogger("gateway.limiter")


class TokenBucket:
    """In-process token bucket. Used for our own ingress limit and as the fallback
    when Redis is unavailable."""

    def __init__(self, rate: float, burst: int):
        self.rate = rate
        self.burst = burst
        self.tokens = float(burst)
        self.ts = time.monotonic()

    def _refill(self) -> None:
        now = time.monotonic()
        self.tokens = min(self.burst, self.tokens + (now - self.ts) * self.rate)
        self.ts = now

    def try_take(self) -> bool:
        self._refill()
        if self.tokens >= 1:
            self.tokens -= 1
            return True
        return False

    def penalize(self, seconds: float) -> None:
        self._refill()
        self.tokens = min(self.tokens, -self.rate * seconds)

    async def acquire(self) -> bool:
        return self.try_take()

    async def penalty(self, seconds: float) -> None:
        self.penalize(seconds)


# Atomic refill+take inside Redis; uses Redis' own clock so replicas can't disagree about time.
_ACQUIRE = """
local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local d = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(d[1])
local ts = tonumber(d[2])
if tokens == nil then tokens = burst; ts = now end
tokens = math.min(burst, tokens + (now - ts) * rate / 1000)
local allowed = 0
if tokens >= 1 then tokens = tokens - 1; allowed = 1 end
redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts', now)
redis.call('PEXPIRE', KEYS[1], math.ceil(burst / rate * 1000) * 2 + 1000)
return allowed
"""

# Provider answered 429: push the bucket negative so every replica backs off together.
_PENALTY = """
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
redis.call('HSET', KEYS[1], 'tokens', tostring(-tonumber(ARGV[1]) * tonumber(ARGV[2])), 'ts', now)
redis.call('PEXPIRE', KEYS[1], 60000)
return 1
"""


class RedisTokenBucket:
    """Distributed token bucket shared by all gateway replicas.

    Redis failure never fails a request: we drop to a local bucket for a few seconds
    (limits become per-replica instead of global, which is the lesser evil)."""

    def __init__(self, redis, key: str, rate: float, burst: int, *,
                 retry_after_failure: float = 5.0, on_fallback=None):
        self.redis = redis
        self.key = key
        self.rate = rate
        self.burst = burst
        self.fallback = TokenBucket(rate, burst)
        self._acquire = redis.register_script(_ACQUIRE)
        self._penalty = redis.register_script(_PENALTY)
        self._retry_after_failure = retry_after_failure
        self._on_fallback = on_fallback
        self._down_until = 0.0
        self._denied_until = 0.0  # local negative cache: don't hit Redis again before a token can exist

    @property
    def degraded(self) -> bool:
        return time.monotonic() < self._down_until

    async def acquire(self) -> bool:
        now = time.monotonic()
        if now < self._denied_until:
            return False
        if now < self._down_until:
            return self.fallback.try_take()
        try:
            allowed = await self._acquire(keys=[self.key], args=[self.rate, self.burst])
        except Exception as e:  # noqa: BLE001 - any Redis problem => degrade, never fail the request
            self._down_until = now + self._retry_after_failure
            if self._on_fallback:
                self._on_fallback()
            log.warning("redis_limiter_unavailable", extra={"error": repr(e)})
            return self.fallback.try_take()
        if not allowed:
            self._denied_until = now + 1.0 / self.rate
            return False
        return True

    async def penalty(self, seconds: float) -> None:
        self._denied_until = time.monotonic() + seconds
        self.fallback.penalize(seconds)
        try:
            await self._penalty(keys=[self.key], args=[self.rate, seconds])
        except Exception:  # noqa: BLE001
            pass
