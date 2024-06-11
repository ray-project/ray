#!/bin/bash

set -euo pipefail

set -x

export PYTHON_VERSION="${PYTHON_VERSION}"
if [[ -z "${RAY_VERSION}" ]]; then
    echo "RAY_VERSION is not set"
    exit 1
fi

if [[ "${RAY_COMMIT:-}" == "" ]]; then
    if [[ "${BUILDKITE_COMMIT:-}" == "" ]]; then
        echo "neither BUILDKITE_COMMIT nor RAY_COMMIT is set"
        exit 1
    fi
    RAY_COMMIT="${BUILDKITE_COMMIT:-}"
fi

export PATH="/usr/local/bin/miniconda3/bin:${PATH}"
source "/usr/local/bin/miniconda3/etc/profile.d/conda.sh"

conda create -n rayio python="${PYTHON_VERSION}" -y

conda activate rayio

pip install \
    --index-url https://test.pypi.org/simple/ \
    --extra-index-url https://pypi.org/simple \
    "ray[cpp]==${RAY_VERSION}"

(
    cd release/util
    python sanity_check.py --ray_version="${RAY_VERSION}" --ray_commit="${RAY_COMMIT}"
)

(
    cd release/util
    bash sanity_check_cpp.sh
)
