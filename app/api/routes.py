import time
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Body, Request, Response
from fastapi.responses import JSONResponse

from ..core.router import AllProvidersFailed

router = APIRouter()


def _reject(request: Request, status: int, error: str, outcome: str) -> JSONResponse:
    request.state.outcome = outcome
    return JSONResponse({"error": error, "trace_id": request.state.trace_id},
                        status_code=status, headers={"Retry-After": "1"})


@router.post("/v1/process")
async def process(request: Request, body: Any = Body(default=None)):
    st = request.app.state
    m = st.metrics
    trace_id = request.state.trace_id

    # 1) Protect ourselves: per-instance rate limit, then a hard cap on concurrent work.
    if not st.ingress.try_take():
        m.ingress_rejected.inc()
        m.requests.labels("rate_limited", "none").inc()
        return _reject(request, 429, "rate_limited", "rate_limited")
    if st.in_flight >= st.settings.max_in_flight:
        m.requests.labels("shed", "none").inc()
        return _reject(request, 503, "overloaded", "shed")

    st.in_flight += 1
    m.in_flight.inc()
    created_at = datetime.now(timezone.utc)
    started = time.perf_counter()
    result = errors = None
    try:
        result = await st.router.process(body, trace_id)
    except AllProvidersFailed as e:
        errors = e.errors
    finally:
        st.in_flight -= 1
        m.in_flight.dec()

    elapsed = time.perf_counter() - started
    m.request_seconds.observe(elapsed)

    if result is not None:
        status_code, outcome, provider, attempts = 200, "success", result.provider, result.attempts
        response_body = {**result.data, "trace_id": trace_id,
                         "provider_used": provider, "attempts": attempts}
        txn = result.data.get("id")
    else:
        status_code, outcome, provider, attempts = 503, "failed", None, len(errors)
        response_body = {"error": "all_providers_unavailable", "trace_id": trace_id,
                         "attempts": attempts, "errors": errors}
        txn = None

    m.requests.labels(outcome, provider or "none").inc()
    request.state.outcome, request.state.provider, request.state.attempts = outcome, provider, attempts

    # 2) Audit record: fire-and-forget into the batch writer queue (never blocks the response).
    if st.writer:
        st.writer.submit((trace_id, created_at, outcome, provider, attempts, status_code,
                          round(elapsed * 1000, 2), txn, body,
                          result.data if result else None, errors))

    return JSONResponse(response_body, status_code=status_code,
                        headers=None if result else {"Retry-After": "1"})


@router.get("/health")
async def health(request: Request):
    st = request.app.state
    lim = st.router.providers[0].limiter
    return {"status": "ok", "in_flight": st.in_flight,
            "providers": st.router.snapshot(),
            "limiter_degraded": bool(getattr(lim, "degraded", False)),
            "persistence": {"enabled": st.writer is not None,
                            "connected": bool(st.writer and st.writer.connected),
                            "queue_depth": st.writer.depth if st.writer else 0}}


@router.get("/metrics")
async def metrics(request: Request):
    st = request.app.state
    return Response(st.metrics.render(st.router, st.writer),
                    media_type="text/plain; version=0.0.4")
