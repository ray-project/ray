#!/usr/bin/env bash
# Deploy one clean cell, replay the trace workload, then tear it down.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/env.sh"

ROUTER_VARIANT=""
CONCURRENCY=""
CELL_DIR=""
DURATION="${BENCHMARK_DURATION:-900}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --router-variant) ROUTER_VARIANT="$2"; shift 2 ;;
    --concurrency) CONCURRENCY="$2"; shift 2 ;;
    --cell-dir) CELL_DIR="$2"; shift 2 ;;
    --duration) DURATION="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

[[ -n "${ROUTER_VARIANT}" && -n "${CONCURRENCY}" && -n "${CELL_DIR}" ]] || {
  echo "need --router-variant, --concurrency, and --cell-dir" >&2
  exit 2
}
[[ ! -e "${CELL_DIR}" ]] || {
  echo "refusing to overwrite existing cell: ${CELL_DIR}" >&2
  exit 2
}
[[ -d "${CC_TRACE_DIR:?CC_TRACE_DIR is required}" ]] || {
  echo "missing trace directory: ${CC_TRACE_DIR}" >&2
  exit 2
}

mkdir -p "${CELL_DIR}/routing" "${CELL_DIR}/aiperf_artifacts"
exec > >(tee -a "${CELL_DIR}/cell.log") 2>&1

cleanup() {
  python "${SCRIPT_DIR}/deploy.py" --shutdown || true
}
trap cleanup EXIT

python "${SCRIPT_DIR}/deploy.py" \
  --router-variant "${ROUTER_VARIANT}" \
  --replicas 4 \
  --tp 2 \
  --max-model-len 131072 \
  --gpu-memory-utilization 0.50 \
  --routing-log-dir "${CELL_DIR}/routing" \
  --meta-out "${CELL_DIR}/meta.json"

ARGS=(
  profile --scenario inferencex-agentx-mvp --unsafe-override
  --url http://localhost:8000 --endpoint /v1/chat/completions
  --endpoint-type chat --streaming --model "${MODEL}"
  --concurrency "${CONCURRENCY}" --benchmark-duration "${DURATION}"
  --request-timeout-seconds 600 --random-seed 42
  --failed-request-threshold 0.10
  --trajectory-start-min-ratio 0.25 --trajectory-start-max-ratio 0.75
  --use-server-token-count --cache-bust first_turn_prefix
  --no-gpu-telemetry --no-server-metrics --tokenizer-trust-remote-code
  --max-context-length 120000 --num-dataset-entries 71
  --slice-duration 1.0 --stats-interval 30
  --custom-dataset-type weka_trace --input-file "${CC_TRACE_DIR}"
  --output-artifact-dir "${CELL_DIR}/aiperf_artifacts"
)

printf '%q ' aiperf "${ARGS[@]}" > "${CELL_DIR}/aiperf_command.txt"
printf '\n' >> "${CELL_DIR}/aiperf_command.txt"
set +e
aiperf "${ARGS[@]}"
AIPERF_RC=$?
set -e

python - "${CELL_DIR}" "${AIPERF_RC}" "${CONCURRENCY}" "${ROUTER_VARIANT}" <<'PY'
import json
import sys
from pathlib import Path

cell_dir = Path(sys.argv[1])
meta_path = cell_dir / "meta.json"
metadata = json.loads(meta_path.read_text())
metadata.update(
    {
        "aiperf_rc": int(sys.argv[2]),
        "concurrency": int(sys.argv[3]),
        "router_variant": sys.argv[4],
        "dataset_max_context_length": 120000,
        "dataset_entries_requested": 71,
    }
)
meta_path.write_text(json.dumps(metadata, indent=2) + "\n")
PY

exit "${AIPERF_RC}"
