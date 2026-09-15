#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT=""

usage() {
  echo "Usage: ./reproduce.sh --out DIRECTORY" >&2
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --out)
      [[ $# -ge 2 && -n "${2:-}" ]] || { usage; exit 2; }
      OUT="$2"
      shift 2
      ;;
    -h|--help) usage; exit 0 ;;
    *) usage; exit 2 ;;
  esac
done
[[ -n "$OUT" ]] || { usage; exit 2; }
[[ "$OUT" = /* ]] || OUT="$PWD/$OUT"
[[ ! -e "$OUT" ]] || { echo "refusing to overwrite: $OUT" >&2; exit 2; }

source "$ROOT/scripts/env.sh"
[[ -x "$AGENTIC_RUNTIME_PYTHON" ]] || { echo "missing runtime Python" >&2; exit 2; }

"$ROOT/scripts/prepare.sh" --out "$OUT"
source "$OUT/run.env"

DEPLOYED=0
cleanup() {
  if [[ "$DEPLOYED" -eq 1 ]]; then
    python "$ROOT/scripts/deploy.py" --shutdown || true
  fi
}
trap cleanup EXIT

run_variant() {
  local variant="$1"
  local cell="$CAMPAIGN_DIR/cells/$variant/c16"
  mkdir -p "$cell/routing"

  echo "--- deploy $variant ---"
  python "$ROOT/scripts/deploy.py" \
    --router "$variant" --replicas 4 --tp 2 --gpu-memory-utilization 0.85 \
    --routing-log-dir "$cell/routing" --meta-out "$cell/meta.json"
  DEPLOYED=1

  echo "--- client traffic $variant ---"
  local extra=()
  if [[ "$variant" == "session-affinity" ]]; then
    extra=(--pre-profile-hook "$ROOT/scripts/prepare_session_affinity_skew.sh")
  fi
  "$ROOT/scripts/run_cell.sh" \
    --router "$variant" --conc 16 --trial 20260813 --cell-dir "$cell" \
    --dag-file "$WORKLOAD_DIR/async_rl_rollouts.dag.jsonl" \
    --seed-file "$WORKLOAD_DIR/global_seed.dag.jsonl" \
    --client-python "$AIPERF_PYTHON" --replicas 4 --roots 80 "${extra[@]}"

  echo "--- teardown $variant ---"
  python "$ROOT/scripts/deploy.py" --shutdown
  DEPLOYED=0
}

run_variant session-affinity
run_variant pure-kv-cache
run_variant kv-token-aware

echo "--- generate results ---"
"$ROOT/scripts/generate_results.sh" \
  --campaign "$CAMPAIGN_DIR" --client-python "$AIPERF_PYTHON" \
  --out "$OUT/async_rl_rollout_router_comparison.png"
