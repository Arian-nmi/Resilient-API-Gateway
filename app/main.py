import asyncio
import logging
from contextlib import asynccontextmanager

import httpx
import redis.asyncio as aioredis
from fastapi import FastAPI

from .api.routes import router as api_router
from .config import settings
from .core.limiter import RedisTokenBucket, TokenBucket
from .core.router import Router, build_providers
from .observability.logging import setup_logging
from .observability.metrics import Metrics
from .observability.middleware import TraceIdMiddleware
from .storage.writer import Writer

setup_logging(settings.log_level)
log = logging.getLogger("gateway")


@asynccontextmanager
async def lifespan(app: FastAPI):
    s = settings
    metrics = Metrics()
    client = httpx.AsyncClient(limits=httpx.Limits(max_connections=1500, max_keepalive_connections=300))
    redis = aioredis.Redis.from_url(s.redis_url, socket_timeout=s.redis_timeout, socket_connect_timeout=s.redis_timeout)
    a_limiter = RedisTokenBucket(redis, "gw:limiter:provider_a", s.a_rate, s.a_burst,
                                 retry_after_failure=s.redis_retry_after_failure,
                                 on_fallback=metrics.redis_fallback.inc)
    try:  # warm the connection pool so a cold-start burst doesn't hit connect timeouts
        await asyncio.gather(*[redis.ping() for _ in range(20)])
        await a_limiter.acquire()
    except Exception as e:  # noqa: BLE001 - Redis down at boot is fine: fallback limiter takes over
        log.warning("redis_unavailable_at_start", extra={"error": repr(e)})
    writer = Writer(s.database_url, s.writer_queue_size, s.writer_batch_size,
                    s.writer_flush_interval, metrics) if s.writer_enabled else None

    app.state.settings = s
    app.state.metrics = metrics
    app.state.in_flight = 0
    app.state.ingress = TokenBucket(s.ingress_rate, s.ingress_burst)
    app.state.router = Router(client, build_providers(s, a_limiter), s, metrics)
    app.state.writer = writer
    
    if writer:
        await writer.start()
    log.info("started")
    yield
    if writer:
        await writer.stop()   # flush what is still queued
    await redis.aclose()
    await client.aclose()


app = FastAPI(title="Provider Gateway", lifespan=lifespan)
app.add_middleware(TraceIdMiddleware)
app.include_router(api_router)
