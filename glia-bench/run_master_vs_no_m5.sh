#!/usr/bin/env bash
# Master-vs-no_m5 perf sweep for the M5-revert checkpoint.
#
# Same harness pattern as run_four_tree_sweep.sh: a shared /opt/venv with
# matched library versions, with MAPPING swapped between the master tree
# and the no_m5 tree on each config switch.
#
# Configs:
#   master   -- upstream/master @ a1ce262eff
#   no_m5    -- this branch (v5-rebase with M5 reverted)
#
# Output: results/optimization_perf_master_vs_no_m5.jsonl
#
# Usage:
#   ./run_master_vs_no_m5.sh             # 5 workloads × 2 configs × 1 rep = 10 runs
#   ./run_master_vs_no_m5.sh all 3       # 3 reps each
#   ./run_master_vs_no_m5.sh synthetic 1
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT="${SCRIPT_DIR}/results/optimization_perf_master_vs_no_m5.jsonl"
mkdir -p "${SCRIPT_DIR}/results"
> "$OUT"

WORKLOAD="${1:-all}"
N_RUNS="${2:-1}"
if [ "$WORKLOAD" = "all" ]; then
    WORKLOADS=(synthetic mixed_pipeline medium_tasks long_tasks actor_backpressure)
else
    WORKLOADS=("$WORKLOAD")
fi

PY=/opt/venv/bin/python
FINDER=/opt/venv/lib/python3.12/site-packages/__editable___ray_2_55_0_finder.py
declare -A TREES=(
  [master]=/tmp/ray-master-tree/python/ray
  [no_m5]=/tmp/ray-v5-no-m5-tree/python/ray
)
TREE_ORDER=(master no_m5)

swap_mapping() {
  local tree="$1"
  sed -i "s|MAPPING: dict\[str, str\] = {'ray': '[^']*'}|MAPPING: dict[str, str] = {'ray': '${tree}'}|" "$FINDER"
  rm -f "$(dirname "$FINDER")/__pycache__/$(basename "$FINDER" .py).cpython-"*.pyc
}

export RAY_OBJECT_STORE_ALLOW_SLOW_STORAGE="${RAY_OBJECT_STORE_ALLOW_SLOW_STORAGE:-1}"
export RAY_DATA_DISABLE_PROGRESS_BARS="${RAY_DATA_DISABLE_PROGRESS_BARS:-1}"

echo "writing to $OUT"
echo "workloads: ${WORKLOADS[@]}"
echo "n_reps: $N_RUNS"

START=$(date +%s)
for wl in "${WORKLOADS[@]}"; do
  for cfg in "${TREE_ORDER[@]}"; do
    swap_mapping "${TREES[$cfg]}"
    echo "=== config=$cfg workload=$wl ==="
    for ((rep=1; rep<=N_RUNS; rep++)); do
      echo "  [$cfg] $wl rep=$rep/$N_RUNS"
      clean=$(mktemp -d)
      stderr_log=$(mktemp --suffix=.stderr.log)
      stdout_log=$(mktemp --suffix=.stdout.log)
      (cd "$clean" && "$PY" "$SCRIPT_DIR/benchmark_scheduler.py" \
        --workload "$wl" \
        --artifact-dir "$SCRIPT_DIR" \
        --config "$SCRIPT_DIR/workload_config.json" \
        --validate \
        > "$stdout_log" 2> "$stderr_log")
      rc=$?
      if [ $rc -ne 0 ]; then
        echo "    EXIT $rc — see $stderr_log"
        tail -5 "$stderr_log" | sed 's/^/      /'
        continue
      fi
      "$PY" -c "
import json, sys
text = open('$stdout_log').read()
last_line = next((l for l in reversed(text.splitlines()) if l.strip().startswith('{')), None)
if last_line is None:
    sys.exit(2)
d = json.loads(last_line)
w = d['result']['workloads'][0]
w['config'] = '$cfg'
w['rep'] = $rep
print(json.dumps(w))
" >> "$OUT"
      tail -1 "$OUT" | "$PY" -c "
import json, sys
d = json.loads(sys.stdin.read())
print(f\"    wall={d['wall_time_sec']:.2f}s tps={d['throughput_blocks_per_sec']:.2f} hash={d['output_hash'][:12]}\")
"
      rm -rf "$clean" "$stderr_log" "$stdout_log"
    done
  done
done
END=$(date +%s)
echo "done. $(wc -l < $OUT) runs recorded to $OUT (total $((END-START))s)"
