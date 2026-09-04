#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# ONE command for a fresh Linux box: replicate every A/B #5 >1.15 regression
# that public data allows, single-node, both readers (user ask 2026-08-28;
# the P0 ledger is arrow_rs_docs/2026-08-27.md §11, the replication map §12).
#
#   ARROW_RS_S3_BUCKET=s3://arrowrs-bench-xxxx \
#     bash release/nightly_tests/dataset/arrow_rs_probe/run_release_regressions.sh
#
# Same skeleton as run_replication.sh (known-good on these boxes):
#   1. setup.sh          venv + commit-matched wheel + crate build (skipped when
#                        env.sh already imports; FORCE_SETUP=1 after a git pull
#                        that touches the crate, or you benchmark a stale .so)
#   2. tpch leg          tpch_probe.py over the P0/addendum queries
#                        (q2,q3,q4,q6,q9,q10,q11,q13,q14,q17,q18,q22 — q6 rides
#                        for the sustained-wUSS 5.8x row, q9 for the T27 spill
#                        history) x both shuffle strategies x both readers,
#                        sf ${TPCH_SF:-10} (release: 1000)
#   3. non-tpch leg      release_regression_probe.py: iter_batches_pyarrow
#                        (exact replica), write_parquet (sf100 for sf1000),
#                        read_parquet_binned (public-data approximation),
#                        map_groups hash/hashv2/sort over col02+14 AND
#                        col08+13+14 (the T-spills-more shapes), aggregate
#                        positive controls (M47), joins
#   4. rlp analog        NOT run here — run_loss_triage.sh's auto_rg S3 shape
#                        is the read_large_parquet stand-in (internal bucket is
#                        ACCESS_DENIED); wide_schema_objects has NO analog.
#
# Knobs (env): TPCH_SF, WRITE_SF, GROUPBY_SF, JOINS_SF (downsizes),
#   JOIN_TYPES=right_outer[,inner,...], REPEAT, ONLY (non-tpch cell filter),
#   SKIP_TPCH=1 / SKIP_PROBE=1, FORCE_SETUP=1, DRY_RUN=1,
#   ARMS=pa,rs,rseos (add rstrim; 2026-09-04 allocator arms), CPUS=24,48,96
#   (non-tpch cells once per local num_cpus — the M107 col02 sweep),
#   MONITOR_INTERVAL=1.0 (node-mem sampler seconds; 0.1 for the q6/wide rows)
# ---------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"
OUT_ROOT="${OUT_ROOT:-$HOME/arrow_rs_regression_runs/$(date +%Y%m%d_%H%M%S)}"
mkdir -p "$OUT_ROOT"

say() { printf '\n\033[1m== %s ==\033[0m\n' "$*"; }

say "1/3 environment (setup.sh; FORCE_SETUP=${FORCE_SETUP:-0})"
if [ "${FORCE_SETUP:-0}" = "1" ] || ! bash -c "source '$SCRIPT_DIR/env.sh' 2>/dev/null \
    && python -c 'import ray_data_arrow_rs'" 2>/dev/null; then
  bash "$SCRIPT_DIR/setup.sh"
fi
# shellcheck disable=SC1091
source "$SCRIPT_DIR/env.sh"

DRY_FLAG=""
[ "${DRY_RUN:-0}" = "1" ] && DRY_FLAG="--dry-run"

if [ "${SKIP_TPCH:-0}" != "1" ]; then
  # P0-scoped matrix, per strategy: most P0 rows are hash_shuffle_v2; v1 runs
  # only where a P0 row names v1 (q4,q14,q22,q17) + q6 (in both: the 5.8x
  # sustained-wUSS row). The full 12x2 matrix is infeasible on one box: v1
  # RPC-shuffles 8-60M rows at ~13k rows/s (q2 v1 pa: >20 min, timed out at
  # sf10). q9 (T27) rides in the additions pass. Override via TPCH_QUERIES_*.
  say "2/3 tpch leg, hash_shuffle_v2 (sf ${TPCH_SF:-10}; $OUT_ROOT/tpch_v2)"
  python "$SCRIPT_DIR/tpch_probe.py" --outdir "$OUT_ROOT/tpch_v2" \
    --sf "${TPCH_SF:-10}" --repeat "${REPEAT:-1}" $DRY_FLAG \
    --cell-timeout "${TPCH_CELL_TIMEOUT:-3600}" \
    --arms "${ARMS:-pa,rs,rseos}" --monitor-interval "${MONITOR_INTERVAL:-1.0}" \
    --queries "${TPCH_QUERIES_V2:-tpch_q2,tpch_q3,tpch_q6,tpch_q10,tpch_q11,tpch_q13,tpch_q17,tpch_q18}" \
    --strategies hash_shuffle_v2 2>&1 | tee "$OUT_ROOT/tpch_v2.out"
  say "2/3 tpch leg, hash_shuffle v1 (sf ${TPCH_SF:-10}; $OUT_ROOT/tpch_v1)"
  python "$SCRIPT_DIR/tpch_probe.py" --outdir "$OUT_ROOT/tpch_v1" \
    --sf "${TPCH_SF:-10}" --repeat "${REPEAT:-1}" $DRY_FLAG \
    --cell-timeout "${TPCH_CELL_TIMEOUT:-3600}" \
    --arms "${ARMS:-pa,rs,rseos}" --monitor-interval "${MONITOR_INTERVAL:-1.0}" \
    --queries "${TPCH_QUERIES_V1:-tpch_q4,tpch_q6,tpch_q14,tpch_q17,tpch_q22}" \
    --strategies hash_shuffle 2>&1 | tee "$OUT_ROOT/tpch_v1.out"
fi

if [ "${SKIP_PROBE:-0}" != "1" ]; then
  say "3/3 non-tpch leg (logs under $OUT_ROOT/probe)"
  python "$SCRIPT_DIR/release_regression_probe.py" --outdir "$OUT_ROOT/probe" \
    --repeat "${REPEAT:-1}" $DRY_FLAG \
    --cell-timeout "${PROBE_CELL_TIMEOUT:-3600}" \
    --write-sf "${WRITE_SF:-100}" --groupby-sf "${GROUPBY_SF:-10}" \
    --joins-sf "${JOINS_SF:-10}" --join-types "${JOIN_TYPES:-right_outer}" \
    --arms "${ARMS:-pa,rs,rseos}" --monitor-interval "${MONITOR_INTERVAL:-1.0}" \
    ${CPUS:+--cpus "$CPUS"} ${ONLY:+--only "$ONLY"} 2>&1 | tee "$OUT_ROOT/probe.out"
fi

say "done — tables in $OUT_ROOT/{tpch_v2,tpch_v1,probe}.out; per-cell benchmark JSONs beside them"
echo "reminder: the read_large_parquet analog is run_loss_triage.sh (auto_rg, S3);"
echo "wide_schema_objects cannot be replicated (internal bucket, no fixture)."
