#!/usr/bin/env bash
# Grid runner for the first full benchmark matrix — §5.3 of
# NON_RDT_ARROW_PROTOTYPE.md (2026-06-17).
#
# Sweeps {placement} x {consumer-mode} x 3 transports x sizes, serial:
#   1 actor pair, concurrency 1  => max-in-flight 1 => latency-bound.
# The 3 modes run back-to-back within each (placement, consumer-mode) cell for
# easy comparison. No `set -e`: a failing cell (e.g. cross-node with <2 worker
# nodes) is skipped without aborting the rest.
#
# Run target: Anyscale / a Linux node with Ray built from THIS branch
# (verify `from ray._raylet import vm_scatter_write`), passwordless sudo so the
# actors' _enable_ptrace() can set ptrace_scope=0 for same-node process_vm, and
# >=2 worker nodes for the cross-node half. NEVER run on macOS.
#
# How it was launched on the Anyscale workspace (detached so it survives the
# `anyscale workspace_v2 run_command` session; then polled for ALL_DONE):
#   setsid bash run_2x2_bench.sh > run_bench.nohup 2>&1 < /dev/null &
#
# For the buffer-pool re-run, bump --duration (15-20 for stable p99 at 100 MB)
# and use --concurrency > 1 so native can pipeline its fetch (see §3.9 nuance 2).

set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH="$HERE/bench_flight_store.py"
LOG="${LOG:-$HERE/bench_2x2.log}"

: > "$LOG"
echo "START $(date -u +%H:%M:%S)" | tee -a "$LOG"
for placement in same-node cross-node; do
  for cmode in read-only modify; do
    for mode in ray arrow-native arrow-rdt; do
      echo "==== mode=$mode placement=$placement consumer=$cmode ====" | tee -a "$LOG"
      python "$BENCH" \
        --mode "$mode" --placement "$placement" --consumer-mode "$cmode" \
        --num-actor-pairs 1 --concurrency 1 --duration 5 \
        --sizes-mb 1 10 100 >> "$LOG" 2>&1
      echo "" >> "$LOG"
    done
  done
done
echo "ALL_DONE $(date -u +%H:%M:%S)" | tee -a "$LOG"
echo "results in $LOG"
