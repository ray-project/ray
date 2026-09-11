#!/usr/bin/env bash
set -euo pipefail

CELL_DIR="$1"
DAG_FILE="$2"
# $3 is the seed DAG path, which affinity calibration does not use.
ROUTER="$4"
ROOTS="$5"
[[ "$ROUTER" == "session-affinity" ]] || { echo "calibration requires session-affinity" >&2; exit 2; }
if ! [[ "$ROOTS" =~ ^[0-9]+$ ]] || (( ROOTS <= 0 || ROOTS % 8 != 0 )); then
  echo "rollout count must be a positive multiple of eight" >&2
  exit 2
fi
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WORKLOAD_DIR="$(cd "$(dirname "$DAG_FILE")" && pwd)"
"$AIPERF_PYTHON" "$SCRIPT_DIR/calibrate_session_affinity_skew.py" \
  --cell-dir "$CELL_DIR" --workload-dir "$WORKLOAD_DIR" \
  --expected-replicas "${AGENTIC_EXPECTED_REPLICAS:-4}" \
  --candidates-per-straggler 48 --probe-concurrency 32
