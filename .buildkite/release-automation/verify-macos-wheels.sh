#!/bin/bash

set -euo pipefail

set -x

PYTHON_VERSIONS=("3.9" "3.10" "3.11" "3.12")
BAZELISK_VERSION="v1.16.0"

# Check arguments
if [[ $# -ne 1 ]]; then
    echo "Missing argument to specify machine architecture." >/dev/stderr
    echo "Use: x86_64 or arm64" >/dev/stderr
    exit 1
fi

MAC_ARCH="$1" # First argument is the architecture of the machine, e.g. x86_64, arm64
export USE_BAZEL_VERSION="${USE_BAZEL_VERSION:-6.5.0}"

# Sets RAY_VERSION and RAY_COMMIT
source .buildkite/release-automation/set-ray-version.sh

install_bazel() {
    if [[ "${MAC_ARCH}" == "arm64" ]]; then
        URL="https://github.com/bazelbuild/bazelisk/releases/download/${BAZELISK_VERSION}/bazelisk-darwin-arm64"
    elif [[ "${MAC_ARCH}" == "x86_64" ]]; then
        URL="https://github.com/bazelbuild/bazelisk/releases/download/${BAZELISK_VERSION}/bazelisk-darwin-amd64"
    else
        echo "Could not find matching bazelisk URL for Mac ${MAC_ARCH}" >/dev/stderr
        exit 1
    fi

    TARGET="$TMP_DIR/bin/bazel"
    curl -sfL -R -o "${TARGET}" "${URL}"
    chmod +x "${TARGET}"
}

install_miniconda() {
    # Install miniconda3 based on the architecture used
    mkdir -p "$TMP_DIR/miniconda3"
    curl -sfL https://repo.anaconda.com/miniconda/Miniconda3-py311_24.4.0-0-MacOSX-"$MAC_ARCH".sh -o "$TMP_DIR/miniconda3/miniconda.sh"
    bash "$TMP_DIR/miniconda3/miniconda.sh" -b -u -p "$TMP_DIR/miniconda3"
    rm -rf "$TMP_DIR/miniconda3/miniconda.sh"

    # Initialize conda. This replaces calling `conda init bash`.
    # Conda init command requires a shell restart which should not be done on BK.
    source "$TMP_DIR/miniconda3/etc/profile.d/conda.sh"
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

install_miniconda
install_bazel

# Install Ray & run sanity checks for each python version
for V in "${PYTHON_VERSIONS[@]}"; do
    run_sanity_check "$V"
done
