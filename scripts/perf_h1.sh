#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
PATH="/mnt/data/rust/bin:$PATH" cargo build --release -p h1-queue-engine
BIN=target/release/h1-queue-engine
EVENTS=cycles,instructions,branches,branch-misses,cache-references,cache-misses,LLC-loads,LLC-load-misses
for workload in deliver_ack random_ack nack_retry_ack; do
  for variant in per hybrid; do
    echo "=== workload=$workload variant=$variant ==="
    taskset -c "${CPU:-2}" perf stat -e "$EVENTS" "$BIN" --messages=4194304 --batch=128 --workload="$workload" --variant="$variant"
    echo
  done
 done
