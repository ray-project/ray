#!/usr/bin/env bash
# Run all 4 scheduler-stress workloads against the currently installed Ray.
# Writes one JSON line per workload to stdout.
#
# Prereq: `pip install -e python/` at the repo root, so `import ray` resolves
# to this working tree. Verify with:
#   python3 -c "import ray; print(ray.__file__)"
# which must point inside this repo.
#
# Usage:
#   ./run_bench.sh                    # all 4 workloads, one run each
#   ./run_bench.sh synthetic          # one specific workload
#   ./run_bench.sh all 5              # all 4 workloads, N=5 runs each

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKLOAD="${1:-all}"
N_RUNS="${2:-1}"

export RAY_OBJECT_STORE_ALLOW_SLOW_STORAGE="${RAY_OBJECT_STORE_ALLOW_SLOW_STORAGE:-1}"
export RAY_DATA_DISABLE_PROGRESS_BARS="${RAY_DATA_DISABLE_PROGRESS_BARS:-1}"

if [ "$WORKLOAD" = "all" ]; then
    WORKLOADS=(synthetic mixed_pipeline medium_tasks long_tasks)
else
    WORKLOADS=("$WORKLOAD")
fi

for wl in "${WORKLOADS[@]}"; do
    for i in $(seq 1 "$N_RUNS"); do
        python3 "$SCRIPT_DIR/benchmark_scheduler.py" \
            --workload "$wl" \
            --artifact-dir "$SCRIPT_DIR" \
            --config workload_config.json \
            --validate \
            2>/dev/null | tail -1
    done
done
