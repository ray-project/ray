#!/bin/bash

set -euo pipefail

set -x

PYTHON_VERSIONS=("3.8" "3.9" "3.10" "3.11")

# Check arguments
if [[ $# -ne 1 ]]; then
    echo "Missing argument to specify machine architecture."
    echo "Use: x86_64 or arm64"
    exit 1
fi

mac_architecture=$1 # First argument is the architecture of the machine, e.g. x86_64, arm64
export RAY_VERSION="${RAY_VERSION:-2.9.1}"
export RAY_HASH="${RAY_HASH:-cfbf98c315cfb2710c56039a3c96477d196de049}"

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
        python sanity_check.py
    )
    conda deactivate
}

_clean_up() {
    rm -rf "$TMP_DIR"
}

# Create tmp directory unique for the run
TMP_DIR=$(mktemp -d "$HOME/tmp.XXXXXXXXXX")
trap _clean_up EXIT

install_miniconda

# Install Ray & run sanity checks for each python version
for python_version in "${PYTHON_VERSIONS[@]}"; do
    run_sanity_check "$python_version"
done

