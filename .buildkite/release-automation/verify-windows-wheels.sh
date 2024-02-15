#!/bin/bash

set -euo pipefail

set -x

PYTHON_VERSIONS=("3.8" "3.9" "3.10" "3.11")
export RAYCI_CHECKOUT_DIR="$(pwd)"
export RAY_VERSION="${RAY_VERSION:-2.9.1}"
export RAY_HASH="${RAY_HASH:-cfbf98c315cfb2710c56039a3c96477d196de049}"

run_sanity_check() {
    local python_version="$1"
    conda create -n "rayio_${python_version}" python="${python_version}" -y
    conda activate "rayio_${python_version}"
    pip install \
        --index-url https://test.pypi.org/simple/ \
        --extra-index-url https://pypi.org/simple \
        "ray[cpp]==$RAY_VERSION"
    (
        cd release/util
        python sanity_check.py
    )
    conda deactivate
    conda env remove -n "rayio_${python_version}" -y
}

source /c/Miniconda3/etc/profile.d/conda.sh

# Install Ray & run sanity checks for each python version
for python_version in "${PYTHON_VERSIONS[@]}"; do
    run_sanity_check "$python_version"
done
