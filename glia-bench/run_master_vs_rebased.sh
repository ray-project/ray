#!/usr/bin/env bash
# Drives the master-vs-rebased perf sweep for the rebased OPTIMIZATION_REPORT.
#
# Replaces the pristine 2.55.0 baseline with `master` (upstream Ray master at
# the time of the rebase) so the comparison reflects "Δ from upgrading our
# fork onto current upstream" rather than "Δ from 2.55.0 to our fork".
#
# Output: results/optimization_perf_master_vs_rebased.jsonl
#
# Configs:
#   master    -- /opt/venv-master   (master nightly wheel; no M-series)
#   rebased   -- /opt/venv          (rebased branch via editable MAPPING; M-series)
#
# Both venvs have matched library versions (pyarrow 24.0.0, pandas 3.0.2,
# numpy 2.4.4) so the comparison isolates Ray Data code-level changes.
#
# Usage:
#   ./run_master_vs_rebased.sh                        # default: 4 workloads × 3 reps
#   ./run_master_vs_rebased.sh actor_backpressure 5   # one workload × N reps
#   ./run_master_vs_rebased.sh all 5                  # all 5 workloads × N reps
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT="${SCRIPT_DIR}/results/optimization_perf_master_vs_rebased.jsonl"
mkdir -p "${SCRIPT_DIR}/results"
> "$OUT"

WORKLOAD="${1:-default}"
N_RUNS="${2:-3}"
if [ "$WORKLOAD" = "all" ]; then
    WORKLOADS=(synthetic mixed_pipeline medium_tasks long_tasks actor_backpressure)
elif [ "$WORKLOAD" = "default" ]; then
    # Skip long_tasks (control) by default — it's expected to show no change
    # and takes ~30 min per rep at 24-CPU.
    WORKLOADS=(synthetic mixed_pipeline medium_tasks actor_backpressure)
else
    WORKLOADS=("$WORKLOAD")
fi

declare -A PYTHONS=(
  [master]=/opt/venv-master/bin/python
  [rebased]=/opt/venv/bin/python
)

export RAY_OBJECT_STORE_ALLOW_SLOW_STORAGE="${RAY_OBJECT_STORE_ALLOW_SLOW_STORAGE:-1}"
export RAY_DATA_DISABLE_PROGRESS_BARS="${RAY_DATA_DISABLE_PROGRESS_BARS:-1}"

echo "writing to $OUT"
echo "workloads: ${WORKLOADS[@]}"
echo "n_reps: $N_RUNS"

START=$(date +%s)
for wl in "${WORKLOADS[@]}"; do
  for cfg in master rebased; do
    PY="${PYTHONS[$cfg]}"
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
      # Last line of stdout is JSON; extract the workload result and add config/rep tags
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
      # Print one-liner
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
