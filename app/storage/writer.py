"""Audit log persistence, kept completely off the request path.

Request handlers call `submit()` (non-blocking, O(1)). A single background task drains the
queue and writes rows with COPY in batches, which is the fastest way to ingest into Postgres
(one round trip and one transaction per ~1000 rows instead of per row).

Failure model: if Postgres is slow or down, the queue fills up and *new records are dropped
and counted* rather than slowing down or crashing request handling. Memory stays bounded.
"""
import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone

import asyncpg

log = logging.getLogger("gateway.writer")

COLUMNS = ["trace_id", "created_at", "status", "provider", "attempts", "http_status",
           "latency_ms", "provider_txn_id", "request_payload", "response", "errors"]

SCHEMA = """
CREATE TABLE IF NOT EXISTS requests (
    trace_id        text        NOT NULL,
    created_at      timestamptz NOT NULL,
    status          text        NOT NULL,   -- success | failed
    provider        text,                   -- provider that produced the final answer
    attempts        smallint    NOT NULL,
    http_status     smallint    NOT NULL,   -- what we returned to the caller
    latency_ms      real        NOT NULL,
    provider_txn_id text,
    request_payload jsonb,
    response        jsonb,
    errors          jsonb                   -- per-attempt failures (provider, kind, status)
) PARTITION BY RANGE (created_at);
CREATE TABLE IF NOT EXISTS requests_default PARTITION OF requests DEFAULT;
CREATE INDEX IF NOT EXISTS requests_trace_id_idx ON requests (trace_id);
CREATE INDEX IF NOT EXISTS requests_created_at_brin ON requests USING brin (created_at);
"""


def _j(v):
    return None if v is None else json.dumps(v)


class Writer:
    def __init__(self, dsn: str, queue_size: int, batch_size: int, flush_interval: float,
                 metrics=None):
        self.dsn = dsn
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.m = metrics
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=queue_size)
        self.pool: asyncpg.Pool | None = None
        self._task: asyncio.Task | None = None
        self._stopping = False
        self._last_partition_check = 0.0

    @property
    def depth(self) -> int:
        return self.queue.qsize()

    @property
    def connected(self) -> bool:
        return self.pool is not None

    def submit(self, record: tuple) -> bool:
        try:
            self.queue.put_nowait(record)
            return True
        except asyncio.QueueFull:
            if self.m:
                self.m.persist_dropped.inc()
            return False

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="audit-writer")

    async def stop(self) -> None:
        self._stopping = True
        if self._task:
            try:
                await asyncio.wait_for(self._task, timeout=15)
            except asyncio.TimeoutError:
                self._task.cancel()
        if self.pool:
            await self.pool.close()

    async def _ensure_pool(self) -> None:
        delay = 0.5
        while self.pool is None and not self._stopping:
            try:
                pool = await asyncpg.create_pool(self.dsn, min_size=1, max_size=4, command_timeout=30)
                async with pool.acquire() as conn:
                    await conn.execute(SCHEMA)
                    await self._ensure_partitions(conn)
                self.pool = pool
                log.info("postgres_ready")
            except Exception as e:  # noqa: BLE001
                log.warning("postgres_unavailable", extra={"error": repr(e), "retry_in_s": delay})
                await asyncio.sleep(delay)
                delay = min(delay * 2, 10)

    async def _ensure_partitions(self, conn, days_ahead: int = 3) -> None:
        """Daily partitions: cheap retention (DROP PARTITION) and small, hot indexes."""
        today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        for i in range(days_ahead + 1):
            start = today + timedelta(days=i)
            end = start + timedelta(days=1)
            name = f"requests_{start:%Y%m%d}"
            try:
                await conn.execute(
                    f"CREATE TABLE IF NOT EXISTS {name} PARTITION OF requests "
                    f"FOR VALUES FROM ('{start.isoformat()}') TO ('{end.isoformat()}')")
            except asyncpg.PostgresError as e:  # e.g. overlaps rows already in the default partition
                log.warning("partition_create_failed", extra={"partition": name, "error": repr(e)})

    async def _run(self) -> None:
        await self._ensure_pool()
        loop = asyncio.get_running_loop()
        while True:
            if self._stopping and self.queue.empty():
                return
            try:
                first = await asyncio.wait_for(self.queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                await self._maybe_roll_partitions(loop.time())
                continue
            batch = [first]
            deadline = loop.time() + self.flush_interval
            while len(batch) < self.batch_size:
                try:
                    batch.append(self.queue.get_nowait())
                except asyncio.QueueEmpty:
                    if self._stopping or loop.time() >= deadline:
                        break
                    await asyncio.sleep(0.01)
            await self._flush(batch)
            await self._maybe_roll_partitions(loop.time())

    async def _maybe_roll_partitions(self, now: float) -> None:
        if self.pool and now - self._last_partition_check > 3600:
            self._last_partition_check = now
            try:
                async with self.pool.acquire() as conn:
                    await self._ensure_partitions(conn)
            except Exception as e:  # noqa: BLE001
                log.warning("partition_roll_failed", extra={"error": repr(e)})

    async def _flush(self, batch: list[tuple]) -> None:
        rows = [(r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], _j(r[8]), _j(r[9]), _j(r[10]))
                for r in batch]
        for attempt in range(1, 4):
            try:
                await self._ensure_pool()
                async with self.pool.acquire() as conn:
                    await conn.copy_records_to_table("requests", records=rows, columns=COLUMNS)
                if self.m:
                    self.m.persist_written.inc(len(rows))
                    self.m.persist_batch.observe(len(rows))
                return
            except Exception as e:  # noqa: BLE001
                log.warning("flush_failed", extra={"attempt": attempt, "rows": len(rows),
                                                   "error": repr(e)})
                if isinstance(e, (asyncpg.PostgresConnectionError, OSError)) and self.pool:
                    await self.pool.close()
                    self.pool = None
                await asyncio.sleep(0.5 * attempt)
        if self.m:
            self.m.persist_dropped.inc(len(rows))
        log.error("batch_dropped", extra={"rows": len(rows)})
