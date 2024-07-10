#!/bin/bash

set -euo pipefail

set -x

PYTHON_VERSIONS=("3.9" "3.10" "3.11" "3.12")
export RAY_VERSION="${RAY_VERSION:-2.32.0}"
export RAY_HASH="${RAY_HASH:-607f2f30f5f21543b6a5568ee77ea779eeba30a8}"

run_sanity_check() {
    local PYTHON_VERSION="$1"
    conda create -n "rayio_${PYTHON_VERSION}" python="${PYTHON_VERSION}" -y
    conda activate "rayio_${PYTHON_VERSION}"
    pip install \
        --index-url https://test.pypi.org/simple/ \
        --extra-index-url https://pypi.org/simple \
        "ray[cpp]==${RAY_VERSION}"
    (
        cd release/util
        which python
        python --version
        python sanity_check.py
    )
    conda deactivate
    conda env remove -n "rayio_${PYTHON_VERSION}" -y
}

source /c/Miniconda3/etc/profile.d/conda.sh

# Install Ray & run sanity checks for each python version
for PYTHON_VERSION in "${PYTHON_VERSIONS[@]}"; do
    run_sanity_check "${PYTHON_VERSION}"
done
