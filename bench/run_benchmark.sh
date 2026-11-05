#!/usr/bin/env bash
# Runs a ladder of scenarios against a running gateway and saves JSON results.
#   docker compose up -d --build && ./bench/run_benchmark.sh
# Env: URL (default http://localhost:8000/v1/process), DURATION (default 60)
set -euo pipefail
cd "$(dirname "$0")/.."
URL="${URL:-http://localhost:8000/v1/process}"
DURATION="${DURATION:-60}"
mkdir -p bench/results
for rps in 20 50 100 200 400; do
  echo "== open loop ${rps} rps"
  python bench/loadtest.py --url "$URL" --rps "$rps" --duration "$DURATION" --out "bench/results/open_${rps}rps.json" | tail -4
done
for c in 200 600; do
  echo "== closed loop ${c} workers (saturation)"
  python bench/loadtest.py --url "$URL" --concurrency "$c" --duration "$DURATION" --out "bench/results/closed_${c}.json" | tail -4
done
python bench/summarize.py
