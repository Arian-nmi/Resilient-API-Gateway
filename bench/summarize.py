#!/usr/bin/env python3
"""Turn bench/results/*.json into a markdown table for the README."""
import glob
import json
import os

rows = []
for f in sorted(glob.glob(os.path.join(os.path.dirname(__file__), "results", "*.json"))):
    r = json.load(open(f))
    L, S = r["latency_all_ms"], r["latency_success_ms"]
    rows.append((r["mode"], r["throughput_rps"], L["avg"], L["p95"], L["p99"], S["p95"],
                 r["success_rate_pct"], r["error_rate_pct"], r["provider_share_pct"], r["avg_attempts"]))
print("| Scenario | RPS | avg ms | p95 ms | p99 ms | p95 ok-only ms | success % | error % | provider share % | avg attempts |")
print("|---|---|---|---|---|---|---|---|---|---|")
for x in rows:
    print("| " + " | ".join(str(v) for v in x) + " |")
