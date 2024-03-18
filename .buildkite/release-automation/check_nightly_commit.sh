#!/bin/bash

set -euo pipefail


get_ray_commit() {
    python -u -c "import ray; print(ray.__commit__)"
}

check_commit() {
    RAY_COMMIT=$(get_commit)
    if [[ "$RAY_COMMIT" != "$BUILDKITE_COMMIT" ]]; then
        echo "ray.__commit__ is not correct:"
        echo "Expected: $BUILDKITE_COMMIT"
        echo "Actual: $RAY_COMMIT"
        exit 1
    fi
}

check_commit