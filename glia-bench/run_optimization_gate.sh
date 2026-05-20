#!/usr/bin/env bash
# Drives the pristine-vs-M6 correctness gate.
#
# Phase 1: record a baseline by running the curated 25-file test list
#          against the pristine ray-2.55.0 tree. Writes per-test pass/flake
#          rates to results/optimization_gate_baseline.json.
# Phase 2: run the same tests against the M6 tree and diff against the
#          baseline. Writes a JSON with ``regressed`` / ``fixed`` lists to
#          results/optimization_gate_m6.json.
#
# Environment:
#   PRISTINE_TREE  required — absolute path to a pristine ray-2.55.0
#                  `python/ray` directory (check out
#                  https://github.com/ray-project/ray at tag ray-2.55.0
#                  and point this at <that-clone>/python/ray).
#   M6_TREE        optional — absolute path to the M6 `python/ray`
#                  directory. Defaults to <this-repo>/python/ray.
#   FINDER         optional — absolute path to the setuptools editable-
#                  install finder. Auto-detected when unset.
#   TIMEOUT        optional — per-test timeout in seconds (default 180).
#
# Usage:
#   PRISTINE_TREE=/path/to/pristine/python/ray ./run_optimization_gate.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
# shellcheck source=_tree_switch.sh
source "$SCRIPT_DIR/_tree_switch.sh"

TIMEOUT="${TIMEOUT:-180}"
RESULTS_DIR="$SCRIPT_DIR/results"
BASELINE="$RESULTS_DIR/optimization_gate_baseline.json"
M6_OUT="$RESULTS_DIR/optimization_gate_m6.json"

mkdir -p "$RESULTS_DIR"

echo "=== phase 1: record baseline (pristine ray-2.55.0) ==="
set_tree "$PRISTINE_TREE"
python "$SCRIPT_DIR/run_gated_tests.py" \
    --mode record \
    --timeout "$TIMEOUT" \
    --artifact-dir "$REPO_ROOT" \
    --baseline "$BASELINE"

echo
echo "=== phase 2: gate (M6) ==="
set_tree "$M6_TREE"
python "$SCRIPT_DIR/run_gated_tests.py" \
    --mode gate \
    --timeout "$TIMEOUT" \
    --artifact-dir "$REPO_ROOT" \
    --baseline "$BASELINE" \
    > "$M6_OUT"

echo
echo "baseline: $BASELINE"
echo "m6 diff:  $M6_OUT"
