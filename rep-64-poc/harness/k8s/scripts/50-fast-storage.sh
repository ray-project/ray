#!/usr/bin/env bash
# 50-fast-storage.sh — fast-NVMe pipelined throughput sweep (placeholder stub).
#
# This script is a structured stub: it always writes a result envelope so
# run-all.sh and aggregate.py see a coherent record, but it doesn't yet run
# the actual sweep.  Two skip paths:
#
#   1. FAST_NVME_CLASS unset → "not configured for this tier"
#      (k3d.env intentionally leaves this empty; only set on tiers with a
#      verified NVMe-backed StorageClass).
#
#   2. FAST_NVME_CLASS set    → "implementation deferred"
#      The full sweep — running storage_microbench across
#      PIPELINE_BENCH_IO_POOL_SIZES against an NVMe-backed PVC and
#      identifying the io-pool-size inflection — is planned post-harness-
#      ship, since k3d (Mariner cgroup-v1) has no production-shaped NVMe
#      class to gate it on.
#
# When the full version lands it will replace this file in place; the
# skip-when-unset behaviour stays as-is.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck disable=SC1091
source "$HERE/../lib.sh"
load_env

START=$(date +%s)

if [[ -z "${FAST_NVME_CLASS:-}" ]]; then
  log "FAST_NVME_CLASS unset — skipping (not configured for this tier)"
  write_result "50-fast-storage" skipped 0 '{}' \
    "FAST_NVME_CLASS not configured for this tier"
  exit 0
fi

log "FAST_NVME_CLASS=$FAST_NVME_CLASS but implementation deferred"
write_result "50-fast-storage" skipped $(( $(date +%s) - START )) \
  "$(jq -n --arg c "$FAST_NVME_CLASS" \
    --arg sizes "${PIPELINE_BENCH_IO_POOL_SIZES:-}" \
    '{configured_class: $c, planned_io_pool_sizes: $sizes}')" \
  "implementation deferred (pipelined throughput sweep planned post-ship)"
log "50-fast-storage skipped"
