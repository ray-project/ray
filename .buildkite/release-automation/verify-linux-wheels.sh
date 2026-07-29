#!/bin/bash

set -euo pipefail

set -x

# Sets RAY_COMMIT and RAY_VERSION
source .buildkite/release-automation/set-ray-version.sh

if [[ "${PYTHON_VERSION}" == "" ]]; then
	echo "Python version not set" >/dev/stderr
	exit 1
fi

export PYTHON_VERSION

export PATH="/usr/local/bin/miniforge3/bin:${PATH}"
source "/usr/local/bin/miniforge3/etc/profile.d/conda.sh"

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
