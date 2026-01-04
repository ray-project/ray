#!/bin/bash

set -euo pipefail

set -x

# TODO(#54047): Python 3.13 is skipped due to the bug
# we should re-enable it when the bug is fixed.

PYTHON_VERSIONS=("3.10" "3.11" "3.12")
BAZELISK_VERSION="v1.16.0"

export USE_BAZEL_VERSION="${USE_BAZEL_VERSION:-6.5.0}"

# Sets RAY_VERSION and RAY_COMMIT
source .buildkite/release-automation/set-ray-version.sh

install_bazel() {
    URL="https://github.com/bazelbuild/bazelisk/releases/download/${BAZELISK_VERSION}/bazelisk-darwin-arm64"

    TARGET="$TMP_DIR/bin/bazel"
    curl -sfL -R -o "${TARGET}" "${URL}"
    chmod +x "${TARGET}"
}

install_miniforge() {
    # Install miniforge3 based on the architecture used
    mkdir -p "$TMP_DIR/miniforge3"
    curl -sfL https://github.com/conda-forge/miniforge/releases/download/25.3.0-1/Miniforge3-25.3.0-1-MacOSX-arm64.sh -o "$TMP_DIR/miniforge3/miniforge.sh"
    bash "$TMP_DIR/miniforge3/miniforge.sh" -b -u -p "$TMP_DIR/miniforge3"
    rm -rf "$TMP_DIR/miniforge3/miniforge.sh"

    # Initialize conda. This replaces calling `conda init bash`.
    # Conda init command requires a shell restart which should not be done on BK.
    source "$TMP_DIR/miniforge3/etc/profile.d/conda.sh"
}

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
        python sanity_check.py --ray_version="${RAY_VERSION}" --ray_commit="${RAY_COMMIT}"
        bash sanity_check_cpp.sh
    )
    conda deactivate
}

_clean_up() {
    bazel clean || true
    rm -rf "$TMP_DIR"
}

# Create tmp directory unique for the run
TMP_DIR="$(mktemp -d "$HOME/tmp.XXXXXXXXXX")"
mkdir -p "$TMP_DIR/bin"
export PATH="$TMP_DIR/bin:$PATH"

trap _clean_up EXIT

install_miniforge
install_bazel

# Install Ray & run sanity checks for each python version
for V in "${PYTHON_VERSIONS[@]}"; do
    run_sanity_check "$V"
done
