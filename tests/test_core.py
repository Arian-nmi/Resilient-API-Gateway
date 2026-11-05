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
    b = CircuitBreaker(window=10, min_calls=4, threshold=0.5, open_seconds=0.05, half_open_probes=2)
    for _ in range(4):
        b.record(False)
    assert b.state == "open" and not b.allow()
    time.sleep(0.06)
    assert b.allow() and b.state == "half_open"
    assert b.allow()              # second concurrent probe allowed
    assert not b.allow()          # third one is refused
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
    assert hits["a"] == 1            # penalised after the first 429, not retried


def test_all_fail_raises_with_errors():
    r = make_router(lambda req: httpx.Response(503, json={}))
    try:
        run(r.process({}, "t5"))
        assert False
    except AllProvidersFailed as e:
        assert len(e.errors) == 3


def test_no_provider_reports_why_each_provider_refused():
    r = make_router(lambda req: ok("a"))
    a, b, c = r.providers
    a.limiter.tokens = 0                      # A: our own budget is empty
    b.inflight = b.max_concurrency            # B: concurrency cap reached
    c.breaker.state, c.breaker.opened_at = "open", time.monotonic()
    c.panic_inflight = c.panic_concurrency    # C: breaker open and panic budget used up
    try:
        run(r.process({}, "t6"))
        assert False
    except AllProvidersFailed as e:
        assert e.errors == [{"error": "no_provider_available"}]
    m = r.m.no_provider
    assert m.labels("a", "rate_limit")._value.get() == 1
    assert m.labels("b", "concurrency")._value.get() == 1
    assert m.labels("c", "panic_cap")._value.get() == 1


def test_open_breakers_are_a_last_resort_not_a_ban():
    def handler(req):
        return ok("b") if req.url.port == 8002 else httpx.Response(503, json={})
    r = make_router(handler)
    a, b, c = r.providers
    a.limiter.tokens = 0                      # A has no budget left
    for p in (b, c):                          # both breakers open
        p.breaker.state, p.breaker.opened_at = "open", time.monotonic()
    res = run(r.process({}, "t7"))
    assert res.provider == "b"                # served through the open breaker
    assert r.m.panic_admissions.labels("b")._value.get() >= 1
    assert b.panic_inflight == 0 and b.inflight == 0   # slots released


def test_healthy_provider_is_preferred_over_panic_path():
    r = make_router(lambda req: ok(str(req.url.port)))
    a, b, c = r.providers
    a.limiter.tokens = 0
    b.breaker.state, b.breaker.opened_at = "open", time.monotonic()
    res = run(r.process({}, "t8"))
    assert res.provider == "c"                # closed breaker wins; open one untouched
    assert r.m.panic_admissions.labels("b")._value.get() == 0


def test_breaker_transitions_are_counted():
    def handler(req):
        return httpx.Response(503, json={}) if req.url.port != 8001 else ok("a")
    r = make_router(handler, breaker_min_calls=3, breaker_window=5)
    b_prov = r.providers[1]
    for _ in range(3):
        b_prov.breaker.record(False)
    assert r.m.breaker_transitions.labels("b", "open")._value.get() == 1
