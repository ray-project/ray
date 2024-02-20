#!/bin/bash

set -euo pipefail

set -x

export PYTHON_VERSION="${PYTHON_VERSION:-3.8}"
export RAY_VERSION=$(buildkite-agent meta-data get ray-version)
export RAY_HASH=$(buildkite-agent meta-data get ray-hash)

export PATH="/root/miniconda3/bin:$PATH"

conda create -n rayio python="${PYTHON_VERSION}" -y

conda activate rayio

pip install \
    --index-url https://test.pypi.org/simple/ \
    --extra-index-url https://pypi.org/simple \
    "ray[cpp]==$RAY_VERSION"

(
    cd release/util
    python sanity_check.py
)

(
    cd release/util
    bash sanity_check_cpp.sh
)
