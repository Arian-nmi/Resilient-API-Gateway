import asyncio
import time

import httpx

from app.config import Settings
from app.core.breaker import CircuitBreaker
from app.core.limiter import TokenBucket
from app.core.router import AllProvidersFailed, Router, build_providers
from app.observability.metrics import Metrics


def run(coro):
    return asyncio.run(coro)


def make_router(handler, **kw):
    s = Settings(backoff_base=0.001, **kw)
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return Router(client, build_providers(s), s, Metrics())


def ok(provider):
    return httpx.Response(200, json={"id": "txn_1", "status": "approved", "provider": provider})


def test_breaker_opens_then_half_opens_then_closes():
    b = CircuitBreaker(window=10, min_calls=4, threshold=0.5, open_seconds=0.05)
    for _ in range(4):
        b.record(False)
    assert b.state == "open" and not b.allow()
    time.sleep(0.06)
    assert b.allow() and b.state == "half_open"
    assert not b.allow()    # only one probe at a time
    b.record(True)
    assert b.state == "closed"


def test_token_bucket_burst_and_penalty():
    t = TokenBucket(rate=10, burst=3)
    assert [t.try_take() for _ in range(4)] == [True, True, True, False]
    t.penalize(5)
    time.sleep(0.2)
    assert not t.try_take()


def test_prefers_provider_a():
    r = make_router(lambda req: ok("a"))
    res = run(r.process({"x": 1}, "t1"))
    assert (res.provider, res.attempts) == ("a", 1)


def test_failover_when_a_and_b_fail():
    def handler(req):
        return ok("c") if req.url.port == 8003 else httpx.Response(503, json={})
    res = run(make_router(handler).process(None, "t2"))
    assert res.provider == "c" and res.attempts == 3


def test_429_from_a_stops_traffic_to_a():
    hits = {"a": 0}

    def handler(req):
        if req.url.port == 8001:
            hits["a"] += 1
            return httpx.Response(429, headers={"retry-after": "5"}, json={})
        return ok("b")

    async def scenario():
        r = make_router(handler)
        first = await r.process({}, "t3")
        second = await r.process({}, "t4")
        return first, second

    first, second = run(scenario())
    assert first.provider == "b" and second.provider == "b"
    assert hits["a"] == 1   # penalised after the first 429, not retried


def test_all_fail_raises_with_errors():
    r = make_router(lambda req: httpx.Response(503, json={}))
    try:
        run(r.process({}, "t5"))
        assert False
    except AllProvidersFailed as e:
        assert len(e.errors) == 3