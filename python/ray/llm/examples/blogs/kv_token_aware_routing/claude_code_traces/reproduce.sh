#!/usr/bin/env bash
# Run the CC router sweep and write its two figures.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT_DIR=""

usage() {
  echo "Usage: $0 --out /path/to/artifact" >&2
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --out) OUT_DIR="${2:-}"; shift 2 ;;
    --help|-h) usage; exit 0 ;;
    *) usage; exit 2 ;;
  esac
done
[[ -n "${OUT_DIR}" ]] || { usage; exit 2; }
[[ ! -e "${OUT_DIR}" ]] || {
  echo "--out must name a new directory for a clean run: ${OUT_DIR}" >&2
  exit 2
}
mkdir -p "${OUT_DIR}"
OUT_DIR="$(cd "${OUT_DIR}" && pwd)"

export PYTHONDONTWRITEBYTECODE=1
export CC_TRACE_DIR="${OUT_DIR}/weka_traces"
export AIPERF_DATASET_MMAP_BASE_PATH="${OUT_DIR}/mmap_runs"
export AIPERF_DATASET_MMAP_CACHE_DIR="${OUT_DIR}/mmap_cache"
export BENCHMARK_DURATION="${BENCHMARK_DURATION:-900}"
export CELL_ATTEMPTS="${CELL_ATTEMPTS:-2}"
mkdir -p "${OUT_DIR}/cells" "${AIPERF_DATASET_MMAP_BASE_PATH}" "${AIPERF_DATASET_MMAP_CACHE_DIR}"

source "${ROOT_DIR}/scripts/env.sh"
command -v aiperf >/dev/null || {
  echo "aiperf is not installed in the active virtual environment" >&2
  exit 1
}
python -c 'import aiperf, datasets, ray' || {
  echo "the active virtual environment must expose aiperf, datasets, and ray" >&2
  exit 1
}
python "${ROOT_DIR}/scripts/download_weka_traces.py" \
  --manifest "${ROOT_DIR}/data/weka_trace_selection.json" --out "${CC_TRACE_DIR}"
python "${ROOT_DIR}/scripts/verify_weka_inputs.py" --data-dir "${CC_TRACE_DIR}"

run_cell() {
  local router_variant="$1"
  local concurrency="$2"
  local cell="${OUT_DIR}/cells/${router_variant}/c${concurrency}"
  local attempt
  for ((attempt = 1; attempt <= CELL_ATTEMPTS; attempt++)); do
    echo "[reproduce] ${router_variant} concurrency=${concurrency} attempt=${attempt}/${CELL_ATTEMPTS}"
    if "${ROOT_DIR}/scripts/run_cell.sh" \
      --router-variant "${router_variant}" --concurrency "${concurrency}" --cell-dir "${cell}" \
      --duration "${BENCHMARK_DURATION}" && \
      python "${ROOT_DIR}/scripts/validate.py" \
        --cell "${cell}" --router-variant "${router_variant}"; then
      return 0
    fi
    if (( attempt == CELL_ATTEMPTS )); then
      return 1
    fi
    mv "${cell}" "${cell}.attempt${attempt}.failed"
  done
}

for concurrency in 8 16 24 32 40; do
  for router_variant in session-affinity kv-token-aware-balanced kv-token-aware-kv-biased; do
    run_cell "${router_variant}" "${concurrency}"
  done
done

python "${ROOT_DIR}/scripts/analyze.py" \
  --cells-dir "${OUT_DIR}/cells" --out "${OUT_DIR}/cells.csv"

mapfile -t mmap_indexes < <(find "${AIPERF_DATASET_MMAP_CACHE_DIR}" -type f -name index.dat -print)
[[ ${#mmap_indexes[@]} -eq 1 ]] || {
  echo "expected one reconstructed AIPerf mmap cache, found ${#mmap_indexes[@]}" >&2
  exit 1
}
python "${ROOT_DIR}/scripts/plot_cc_traces_distribution.py" \
  --data-dir "${CC_TRACE_DIR}" \
  --mmap-cache "$(dirname "${mmap_indexes[0]}")" \
  --out "${OUT_DIR}/cc_traces_distribution.png"
python "${ROOT_DIR}/scripts/plot_router_comparison.py" \
  --cells "${OUT_DIR}/cells.csv" \
  --out "${OUT_DIR}/cc_traces_router_comparison.png"

echo "[reproduce] wrote ${OUT_DIR}/cc_traces_distribution.png"
echo "[reproduce] wrote ${OUT_DIR}/cc_traces_router_comparison.png"
