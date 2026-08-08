#!/bin/bash

set -euo pipefail

set -x

# Sets RAY_COMMIT and RAY_VERSION
source .buildkite/release-automation/set-ray-version.sh

if [[ "${PYTHON_VERSION:-}" == "" ]]; then
    echo "Python version not set" >/dev/stderr
    exit 1
fi

# The Windows agents only carry a fixed system Python; use uv to provision
# the Python version under test, same as python/build-wheel-windows.sh.
pip install uv

VENV_DIR="$(mktemp -d)/rayio_${PYTHON_VERSION}"

_clean_up() {
    rm -rf "$(dirname "${VENV_DIR}")"
}
trap _clean_up EXIT

uv venv --seed --python "${PYTHON_VERSION}" "${VENV_DIR}"
source "${VENV_DIR}/Scripts/activate"

pip install \
    --index-url https://test.pypi.org/simple/ \
    --extra-index-url https://pypi.org/simple \
    "ray[cpp]==${RAY_VERSION}"

(
    cd release/util
    python sanity_check.py --ray_version="${RAY_VERSION}" --ray_commit="${RAY_COMMIT}"
)

# Unlike Linux and macOS, sanity_check_cpp.sh is skipped: building the C++
# example requires a bazel + MSVC toolchain that is not available on the
# Windows runner agents. Installing ray[cpp] above still verifies that the
# ray_cpp win_amd64 wheel is published and installable.
