#!/bin/bash
# One measurement round: deploy MODE, warm up, run the steady-state load points.
# Usage: round.sh MODE OUTDIR [EXTRA_ENV_JSON]
set -euo pipefail
MODE=$1
OUT=$2
EXTRA=${3:-"{}"}
PY=/mnt/local_storage/sm/sm_env/bin/python
cd /home/ray/default/sm_work
# Results are large; keep them out of the snapshotted workspace.
case "$OUT" in /*) ;; *) OUT=${RUNS_ROOT:-/mnt/local_storage/sm/runs}/${OUT#runs/} ;; esac
mkdir -p "$OUT"
$PY deploy.py --mode "$MODE" --out "$OUT" --extra-env "$EXTRA" > "$OUT/deploy.log" 2>&1
# Warm up at the highest load point; results are discarded from comparisons.
$PY bench.py run --out "$OUT/warmup" --conc 256 --rate 36 --num 768 --clients 32 --seed 7 > "$OUT/warmup.txt" 2>&1
for point in "6 64 360 8" "18 128 1080 16" "36 256 2048 32"; do
  # shellcheck disable=SC2086  # split the point into its four fields
  set -- $point
  $PY bench.py run --out "$OUT/r$1_c$2" --rate "$1" --conc "$2" --num "$3" --clients "$4" --seed 1000 \
    > "$OUT/r$1_c$2.txt" 2>&1
done
echo "ROUND DONE $MODE $OUT"
