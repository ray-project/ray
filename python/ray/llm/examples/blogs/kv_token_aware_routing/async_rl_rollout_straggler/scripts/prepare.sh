#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/env.sh"
AIPERF_ROOT="${AIPERF_ROOT:-$(cd "$AIPERF_SRC/.." && pwd)}"
AIPERF_COMMIT=c2f5e9d459005d362457716bbd865d247232fa30
DYNAMO_COMMIT=dfc15c35d9cecffd909e8b10ab6ec62d4fa3d844

OUT=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --out) OUT="${2:-}"; shift 2 ;;
    *) echo "Usage: $0 --out DIRECTORY" >&2; exit 2 ;;
  esac
done
[[ -n "$OUT" ]] || { echo "Usage: $0 --out DIRECTORY" >&2; exit 2; }
[[ "$OUT" = /* ]] || OUT="$PWD/$OUT"
[[ ! -e "$OUT" ]] || { echo "refusing to overwrite: $OUT" >&2; exit 2; }
command -v uv >/dev/null || { echo "uv is required" >&2; exit 2; }
[[ -f "$AIPERF_ROOT/pyproject.toml" ]] || { echo "missing AIPerf source: $AIPERF_ROOT" >&2; exit 2; }
[[ -d "$DYNAMO_SOURCE/.git" ]] || { echo "missing Dynamo source: $DYNAMO_SOURCE" >&2; exit 2; }
[[ -x "$AGENTIC_RUNTIME_PYTHON" ]] || { echo "missing runtime Python" >&2; exit 2; }

mkdir -p "$OUT"
snapshot_source() {
  local source="$1"
  local destination="$2"
  local commit="$3"
  git -C "$source" cat-file -e "$commit^{commit}"
  git clone --shared --no-checkout "$source" "$destination"
  git -C "$destination" checkout --detach "$commit"
}

AIPERF_SNAPSHOT="$OUT/sources/aiperf"
DYNAMO_SNAPSHOT="$OUT/sources/dynamo"
mkdir -p "$OUT/sources"
snapshot_source "$AIPERF_ROOT" "$AIPERF_SNAPSHOT" "$AIPERF_COMMIT"
snapshot_source "$DYNAMO_SOURCE" "$DYNAMO_SNAPSHOT" "$DYNAMO_COMMIT"
"$SCRIPT_DIR/apply_aiperf_patch.sh" --source "$AIPERF_SNAPSHOT"

CLIENT_VENV="$OUT/aiperf-venv"
uv venv --python "$AIPERF_BASE_PYTHON" "$CLIENT_VENV"
CLIENT_PYTHON="$CLIENT_VENV/bin/python"
uv pip install --python "$CLIENT_PYTHON" "$AIPERF_SNAPSHOT"

"$CLIENT_PYTHON" - <<'PY'
from aiperf.credit.callback_handler import _closed_loop_turn_concurrency_enabled
from aiperf.dataset.loader.dag_jsonl_models import DagTurn

assert callable(_closed_loop_turn_concurrency_enabled)
assert "timestamp" in DagTurn.model_fields
PY

"$SCRIPT_DIR/build_dynamo_cache_affinity_wheel.sh" \
  --source "$DYNAMO_SNAPSHOT" --wheel-dir "$OUT/dynamo-wheel"
"$CLIENT_PYTHON" "$SCRIPT_DIR/generate_workload.py" \
  --out "$OUT/workload" \
  --follower-gap-ms 2200 --step-stagger-ms 250 --regular-start-lag-ms 3200 \
  --intermediate-output-tokens 1024 --regular-terminal-output-tokens 1024 \
  --straggler-terminal-output-tokens 8192

{
  printf 'export AIPERF_PYTHON=%q\n' "$CLIENT_PYTHON"
  printf 'export WORKLOAD_DIR=%q\n' "$OUT/workload"
  printf 'export CAMPAIGN_DIR=%q\n' "$OUT/campaign"
  printf 'export AGENTIC_SESSION_PREFIX=%q\n' "rl-rollout"
  printf 'export AGENTIC_PROFILE_REQUESTS_TOTAL=%q\n' "800"
  printf 'export AGENTIC_REQUIRED_OUTPUT_TIERS=%q\n' "1024:780,8192:20"
  printf 'export AGENTIC_EXPECTED_REPLICAS=%q\n' "4"
  printf 'export AGENTIC_WORKLOAD_KIND=%q\n' "async_rl_rollout_v1"
} > "$OUT/run.env"
echo "Prepared $OUT"
