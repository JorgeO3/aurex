#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
PATH="/mnt/data/rust/bin:$PATH" cargo check --workspace --all-targets
