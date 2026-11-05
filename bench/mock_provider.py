"""Local stand-in for the real providers, for offline testing.
NAME=a LMIN=0.3 LMAX=1 ERR=0 RATE=50 uvicorn bench.mock_provider:app --port 9001
"""
import asyncio
import os
import random
import time
from collections import deque

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

NAME = os.getenv("NAME", "a")
LMIN, LMAX = float(os.getenv("LMIN", "0.3")), float(os.getenv("LMAX", "1"))
ERR = float(os.getenv("ERR", "0"))
RATE = int(os.getenv("RATE", "0"))  # max req/s, 0 = unlimited
HANG = float(os.getenv("HANG", "0"))  # probability a request hangs 30 s (client-side timeout)

app = FastAPI()
hits: deque[float] = deque()


@app.post("/v1/process")
async def process(request: Request):
    now = time.monotonic()
    if RATE:
        while hits and now - hits[0] > 1:
            hits.popleft()
        if len(hits) >= RATE:
            return JSONResponse({"error": "rate_limited"}, status_code=429,
                                headers={"Retry-After": "1"})
        hits.append(now)
    if random.random() < HANG:
        await asyncio.sleep(30)
    delay = random.uniform(LMIN, LMAX)
    await asyncio.sleep(delay)
    if random.random() < ERR:
        return JSONResponse({"error": "upstream_unavailable", "provider": NAME}, status_code=503)
    try:
        body = await request.json()
    except Exception:
        body = {}
    return {"id": f"txn_{random.getrandbits(48):x}", "status": "approved", "provider": NAME,
            "echo": body.get("payload", {}), "latency_ms": round(delay * 1000, 2)}
