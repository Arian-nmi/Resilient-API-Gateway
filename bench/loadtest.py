#!/usr/bin/env python3
"""Load generator for the gateway.

Closed loop (find max throughput):   python bench/loadtest.py --concurrency 200 --duration 60
Open loop (fixed arrival rate):      python bench/loadtest.py --rps 500 --duration 60

Open loop is the honest way to measure latency: requests are *scheduled* at a fixed rate, so a slow
gateway can't hide its own slowness by slowing down the load generator (coordinated omission).
Run it from a different machine than the gateway if you can, and keep rps per process below ~2000.
"""
import argparse
import asyncio
import json
import statistics
import time
from collections import Counter

import httpx


def pct(sorted_vals: list[float], p: float) -> float:
    if not sorted_vals:
        return float("nan")
    k = min(len(sorted_vals) - 1, max(0, int(round(p / 100 * len(sorted_vals) + 0.5)) - 1))
    return sorted_vals[k]


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://localhost:8000/v1/process")
    ap.add_argument("--duration", type=float, default=30, help="seconds of measured load")
    ap.add_argument("--warmup", type=float, default=3, help="seconds excluded from results")
    ap.add_argument("--concurrency", type=int, default=100, help="closed loop: parallel workers")
    ap.add_argument("--rps", type=float, default=0, help="open loop: target arrivals per second")
    ap.add_argument("--max-inflight", type=int, default=20000, help="open loop: client-side cap")
    ap.add_argument("--timeout", type=float, default=40)
    ap.add_argument("--out", default="", help="write full results JSON here")
    a = ap.parse_args()

    samples: list[tuple[float, str, str | None, int]] = []  # (latency_s, status, provider, attempts)
    t_start = time.perf_counter()
    t_measure = t_start + a.warmup
    t_end = t_measure + a.duration
    skipped = 0

    limits = httpx.Limits(max_connections=a.max_inflight if a.rps else a.concurrency,
                          max_keepalive_connections=2000)
    async with httpx.AsyncClient(limits=limits, timeout=a.timeout) as client:

        async def one(i: int) -> None:
            body = {"order_id": i, "amount": 100, "currency": "USD"}
            t0 = time.perf_counter()
            try:
                r = await client.post(a.url, json=body)
                dt = time.perf_counter() - t0
                try:
                    j = r.json()
                except ValueError:
                    j = {}
                res = (dt, str(r.status_code), j.get("provider_used"), int(j.get("attempts") or 0))
            except httpx.TimeoutException as e:
                res = (time.perf_counter() - t0, f"client_timeout:{type(e).__name__}", None, 0)
            except httpx.HTTPError as e:
                res = (time.perf_counter() - t0, f"client_error:{type(e).__name__}", None, 0)
            if t0 >= t_measure:
                samples.append(res)

        if a.rps:
            sem = asyncio.Semaphore(a.max_inflight)
            tasks: set[asyncio.Task] = set()
            interval = 1.0 / a.rps
            nxt, i = t_start, 0
            while nxt < t_end:
                delay = nxt - time.perf_counter()
                if delay > 0:
                    await asyncio.sleep(delay)
                if sem.locked():
                    skipped += 1  # client saturated: counted, not hidden
                else:
                    await sem.acquire()
                    t = asyncio.create_task(one(i))
                    tasks.add(t)
                    t.add_done_callback(lambda tt: (tasks.discard(tt), sem.release()))
                i += 1
                nxt += interval
            if tasks:
                await asyncio.gather(*tasks)
        else:
            counter = iter(range(10**9))

            async def worker() -> None:
                while time.perf_counter() < t_end:
                    await one(next(counter))

            await asyncio.gather(*[worker() for _ in range(a.concurrency)])

    n = len(samples)
    if not n:
        print("no samples collected")
        return
    lat = sorted(s[0] for s in samples)
    ok = sorted(s[0] for s in samples if s[1] == "200")
    status = Counter(s[1] for s in samples)
    providers = Counter(s[2] for s in samples if s[1] == "200")
    attempts = [s[3] for s in samples if s[1] == "200"]
    rps = n / a.duration
    result = {
        "mode": f"open-loop {a.rps} rps" if a.rps else f"closed-loop {a.concurrency} workers",
        "duration_s": a.duration, "requests": n, "throughput_rps": round(rps, 1),
        "success_rate_pct": round(100 * status["200"] / n, 2),
        "error_rate_pct": round(100 * (n - status["200"]) / n, 2),
        "status_breakdown": dict(status), "client_skipped_arrivals": skipped,
        "latency_all_ms": {"avg": round(statistics.mean(lat) * 1000, 1), "p50": round(pct(lat, 50) * 1000, 1),
                           "p95": round(pct(lat, 95) * 1000, 1), "p99": round(pct(lat, 99) * 1000, 1),
                           "max": round(lat[-1] * 1000, 1)},
        "latency_success_ms": {"avg": round(statistics.mean(ok) * 1000, 1) if ok else None,
                               "p50": round(pct(ok, 50) * 1000, 1) if ok else None,
                               "p95": round(pct(ok, 95) * 1000, 1) if ok else None,
                               "p99": round(pct(ok, 99) * 1000, 1) if ok else None},
        "provider_share_pct": {k: round(100 * v / max(1, sum(providers.values())), 1)
                               for k, v in sorted(providers.items(), key=lambda x: str(x[0]))},
        "avg_attempts": round(statistics.mean(attempts), 3) if attempts else None,
    }
    print(json.dumps(result, indent=2))
    if a.out:
        with open(a.out, "w") as f:
            json.dump(result, f, indent=2)
    L = result["latency_all_ms"]
    print(f"\n| mode | RPS | avg ms | p95 ms | p99 ms | error % |\n|---|---|---|---|---|---|\n"
          f"| {result['mode']} | {result['throughput_rps']} | {L['avg']} | {L['p95']} | {L['p99']} | "
          f"{result['error_rate_pct']} |")


if __name__ == "__main__":
    asyncio.run(main())
