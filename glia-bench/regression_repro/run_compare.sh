#!/usr/bin/env bash
# Run each regression-repro bench against m6_fixed and master.
# Sequential; data is generated once (cached) and reused across trees.
set -uo pipefail

REPRO_DIR="$(cd "$(dirname "$0")" && pwd)"
OUT_DIR="${OUT_DIR:-/tmp/regression_repro_results}"
mkdir -p "$OUT_DIR"

# CSV-like manifest of (bench, suspected_master_fix)
BENCHES=(
  "b4_wide_schema_read|read+batcher chunk combining (#62579, #62720)"
  "b1_map_batches_fusion|Read→MapBatches operator fusion (master code path)"
  "b2_inference_actor_pool|actor-pool task selection heap (#62309, #62574)"
  "b3_tpch_q3_autoscale|op resource reservation under multi-op CPU pressure (#62592)"
  "b5_tpch_q7_join_order|logical-op refactor reorder (#62321/#62400/#63090)"
)

declare -A PYTHONS=(
  [m6_fixed]=/opt/venv/bin/python
  [master]=/opt/venv-master/bin/python
)

START=$(date +%s)

for entry in "${BENCHES[@]}"; do
  bench="${entry%%|*}"
  hint="${entry##*|}"
  for tree in m6_fixed master; do
    PY="${PYTHONS[$tree]}"
    LOG="$OUT_DIR/${bench}__${tree}.log"
    JSON="$OUT_DIR/${bench}__${tree}.json"
    echo "===== $bench / $tree -> $LOG ====="
    cd "$REPRO_DIR/$bench"
    BENCH_OUT="$JSON" timeout 1800 "$PY" run.py > "$LOG" 2>&1
    rc=$?
    if [ $rc -eq 0 ]; then
      grep -E "wall_mean|exec_plan:|wrote " "$LOG" | head -5
    elif [ $rc -eq 124 ]; then
      echo "  TIMEOUT after 1800s"
    else
      echo "  EXIT $rc — see $LOG"
      tail -10 "$LOG" | sed 's/^/    /'
    fi
  done
done

END=$(date +%s)
echo
echo "===== TOTAL WALL: $((END-START)) s ====="
echo

echo "===== SUMMARY ====="
printf '%-32s  %-9s  %-12s  %-12s  %-7s  %s\n' "bench" "tree" "wall_mean" "wall_min" "Δ%" "exec_plan"
for entry in "${BENCHES[@]}"; do
  bench="${entry%%|*}"
  M6_JSON="$OUT_DIR/${bench}__m6_fixed.json"
  MASTER_JSON="$OUT_DIR/${bench}__master.json"
  for tree in m6_fixed master; do
    JSON="$OUT_DIR/${bench}__${tree}.json"
    if [ ! -f "$JSON" ]; then
      printf '%-32s  %-9s  %s\n' "$bench" "$tree" "NO RESULT"
      continue
    fi
    wall=$(/opt/venv/bin/python -c "import json; d=json.load(open('$JSON')); print(f\"{d['wall_seconds_mean']:.2f}\")")
    walmin=$(/opt/venv/bin/python -c "import json; d=json.load(open('$JSON')); print(f\"{d['wall_seconds_min']:.2f}\")")
    plan=$(/opt/venv/bin/python -c "import json; p=json.load(open('$JSON')).get('exec_plan'); print((p or '')[:80])")
    delta="—"
    if [ -f "$M6_JSON" ] && [ -f "$MASTER_JSON" ]; then
      delta=$(/opt/venv/bin/python -c "
import json
g=json.load(open('$M6_JSON'))['wall_seconds_mean']
m=json.load(open('$MASTER_JSON'))['wall_seconds_mean']
ratio = (g-m)/m*100
print(f'{ratio:+.1f}%')
")
      [ "$tree" != "m6_fixed" ] && delta="—"
    fi
    printf '%-32s  %-9s  %-12s  %-12s  %-7s  %s\n' "$bench" "$tree" "${wall}s" "${walmin}s" "$delta" "$plan"
  done
done
