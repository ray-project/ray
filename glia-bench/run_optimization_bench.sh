#!/usr/bin/env bash
# Drives the N=5 pristine-vs-M6 perf sweep for the optimization report.
#
# Output: results/optimization_perf.jsonl — one line per (config, workload, rep)
# with fields: config, workload, rep, wall_time_sec, throughput_blocks_per_sec,
# driver_cpu_per_wall, efficiency_blocks_per_core_sec, output_hash.
#
# Environment:
#   PRISTINE_TREE  required — absolute path to a pristine ray-2.55.0
#                  `python/ray` directory (check out
#                  https://github.com/ray-project/ray at tag ray-2.55.0
#                  and point this at <that-clone>/python/ray).
#   M6_TREE        optional — absolute path to the M6 `python/ray`
#                  directory. Defaults to <this-repo>/python/ray.
#   FINDER         optional — absolute path to the setuptools editable-
#                  install finder. Auto-detected from the active Python's
#                  site-packages when unset.
#
# Usage:
#   PRISTINE_TREE=/path/to/pristine/python/ray \
#     ./run_optimization_bench.sh                 # all 4 workloads × 5 reps
#   PRISTINE_TREE=... ./run_optimization_bench.sh mixed_pipeline   # single workload
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
# shellcheck source=_tree_switch.sh
source "$SCRIPT_DIR/_tree_switch.sh"

OUT="${SCRIPT_DIR}/results/optimization_perf.jsonl"

WORKLOAD="${1:-all}"
N_RUNS="${2:-5}"
if [ "$WORKLOAD" = "all" ]; then
    WORKLOADS=(synthetic mixed_pipeline medium_tasks long_tasks actor_backpressure)
else
    WORKLOADS=("$WORKLOAD")
fi

export RAY_OBJECT_STORE_ALLOW_SLOW_STORAGE="${RAY_OBJECT_STORE_ALLOW_SLOW_STORAGE:-1}"
export RAY_DATA_DISABLE_PROGRESS_BARS="${RAY_DATA_DISABLE_PROGRESS_BARS:-1}"

run_one() {
    local config="$1" workload="$2" rep="$3"
    local raw clean stderr_log stdout_log rc
    # cwd matters for import resolution — avoid any dir that has a `ray/`
    # subdir. SCRIPT_DIR itself is safe.
    clean=$(mktemp -d)
    stderr_log=$(mktemp --suffix=.stderr.log)
    stdout_log=$(mktemp --suffix=.stdout.log)
    # Temporarily disable `set -e` / pipefail so a python crash surfaces
    # as a non-zero $? instead of terminating the script silently.
    set +e
    (cd "$clean" && python3 "$SCRIPT_DIR/benchmark_scheduler.py" \
        --workload "$workload" \
        --artifact-dir "$SCRIPT_DIR" \
        --config workload_config.json \
        --validate \
        >"$stdout_log" 2>"$stderr_log")
    rc=$?
    set -e
    rmdir "$clean" 2>/dev/null || true
    raw=$(tail -1 "$stdout_log")
    if [ "$rc" -ne 0 ] || [ -z "$raw" ] || [[ "$raw" != {* ]]; then
        echo "ERROR: benchmark_scheduler.py failed for [${config}] ${workload} rep=${rep}" >&2
        echo "       exit code: $rc" >&2
        echo "       stdout tail:" >&2
        tail -20 "$stdout_log" | sed 's/^/         /' >&2
        echo "       stderr tail:" >&2
        tail -40 "$stderr_log" | sed 's/^/         /' >&2
        exit 1
    fi
    rm -f "$stderr_log" "$stdout_log"
    python3 -c "
import json, sys
o = json.loads('''$raw''')
w = o['result']['workloads'][0]
rec = {
    'config': '$config',
    'workload': '$workload',
    'rep': $rep,
    'wall_time_sec': w['wall_time_sec'],
    'throughput_blocks_per_sec': w['throughput_blocks_per_sec'],
    'throughput_rows_per_sec': w['throughput_rows_per_sec'],
    'driver_cpu_per_wall': w['driver_cpu_per_wall'],
    'efficiency_blocks_per_core_sec': w['efficiency_blocks_per_core_sec'],
    'output_hash': w['output_hash'],
    'peak_op_obj_store_over_alloc_ratio': w.get('peak_op_obj_store_over_alloc_ratio'),
    'peak_op_obj_store_used_mb': w.get('peak_op_obj_store_used_mb'),
    'peak_op_obj_store_alloc_mb': w.get('peak_op_obj_store_alloc_mb'),
    'num_sampler_reads': w.get('num_sampler_reads'),
}
print(json.dumps(rec))
" >> "$OUT"
}

mkdir -p "$(dirname "$OUT")"
: > "$OUT"
echo "writing to $OUT"

for config in pristine m6; do
    case "$config" in
        pristine) set_tree "$PRISTINE_TREE" ;;
        m6)       set_tree "$M6_TREE" ;;
    esac
    active_path=$(cd /tmp && python -c "import $FINDER_MODULE as F; print(F.MAPPING['ray'])")
    echo "=== config=$config tree=$active_path ==="
    for wl in "${WORKLOADS[@]}"; do
        for i in $(seq 1 "$N_RUNS"); do
            echo "  [${config}] ${wl} rep=${i}/${N_RUNS}"
            run_one "$config" "$wl" "$i"
        done
    done
done

# Leave the finder pointing at M6 for subsequent gate runs.
set_tree "$M6_TREE"
echo "done. $(wc -l < "$OUT") runs recorded to $OUT"
