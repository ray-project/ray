#!/bin/bash

set -euo pipefail

set -x

# Check for arguments
if [ "$#" -ne 1 ]; then
    echo "Missing argument to specify machine architecture."
    echo "Use: x86_64 or arm64"
    exit 1
fi

mac_architecture=$1 # First argument is the architecture of the machine, e.g. x86_64, arm64

# Install dependencies (Bazel, Miniconda3, etc.)
"ci/env/install-dependencies.sh"

export PYTHON_VERSION="${PYTHON_VERSION:-3.8}"
export RAY_VERSION="${RAY_VERSION:-2.9.1}"
export RAY_HASH="${RAY_HASH:-cfbf98c315cfb2710c56039a3c96477d196de049}"

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
