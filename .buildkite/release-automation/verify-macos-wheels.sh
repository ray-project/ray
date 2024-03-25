#!/bin/bash

set -euo pipefail

set -x

PYTHON_VERSIONS=("3.9" "3.10" "3.11")
BAZELISK_VERSION="v1.16.0"

# Check arguments
if [[ $# -ne 1 ]]; then
    echo "Missing argument to specify machine architecture."
    echo "Use: x86_64 or arm64"
    exit 1
fi

mac_architecture=$1 # First argument is the architecture of the machine, e.g. x86_64, arm64
export USE_BAZEL_VERSION="${USE_BAZEL_VERSION:-5.4.1}"

install_bazel() {
    if [[ "${mac_architecture}" = "arm64" ]]; then
        URL="https://github.com/bazelbuild/bazelisk/releases/download/${BAZELISK_VERSION}/bazelisk-darwin-arm64"
    elif [[ "${mac_architecture}" = "x86_64" ]]; then
        URL="https://github.com/bazelbuild/bazelisk/releases/download/${BAZELISK_VERSION}/bazelisk-darwin-amd64"
    else
        echo "Could not find matching bazelisk URL for Mac ${mac_architecture}"
        exit 1
    fi

    TARGET="$TMP_DIR/bin/bazel"
    curl -f -s -L -R -o "${TARGET}" "${URL}"
    chmod +x "${TARGET}"
}

install_miniconda() {
    # Install miniconda3 based on the architecture used
    mkdir -p "$TMP_DIR/miniconda3"
    curl https://repo.anaconda.com/miniconda/Miniconda3-py38_23.1.0-1-MacOSX-"$mac_architecture".sh -o "$TMP_DIR/miniconda3/miniconda.sh"
    bash "$TMP_DIR/miniconda3/miniconda.sh" -b -u -p "$TMP_DIR/miniconda3"
    rm -rf "$TMP_DIR/miniconda3/miniconda.sh"

    # Add miniconda3 to the PATH to use `conda` command.
    export PATH="$TMP_DIR/miniconda3/bin:$PATH"

    # Initialize conda. This replaces calling `conda init bash`.
    # Conda init command requires a shell restart which should not be done on BK.
    source "$TMP_DIR/miniconda3/etc/profile.d/conda.sh"
}

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
        python sanity_check.py --ray_version="$RAY_VERSION" --ray_commit="$BUILDKITE_COMMIT"
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
# Add bin dir to the path.
export PATH="$TMP_DIR/bin:$PATH"

trap _clean_up EXIT

install_miniconda
install_bazel

# Install Ray & run sanity checks for each python version
for python_version in "${PYTHON_VERSIONS[@]}"; do
    run_sanity_check "$python_version"
done
