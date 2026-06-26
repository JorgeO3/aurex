#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
PATH="/mnt/data/rust/bin:$PATH" cargo build --release -p h1-queue-engine
for workload in deliver_ack random_ack nack_retry_ack; do
  echo "=== $workload ==="
  PATH="/mnt/data/rust/bin:$PATH" cargo run --release -p h1-queue-engine -- --messages=4194304 --batch=128 --workload="$workload" --variant=both
  echo
 done
