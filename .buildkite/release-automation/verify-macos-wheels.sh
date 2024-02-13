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
. ./ci/env/install-bazel.sh
echo $PATH
bazel --version

# Install miniconda3 based on the architecture used
mkdir -p ~/miniconda3
curl https://repo.anaconda.com/miniconda/Miniconda3-py38_23.1.0-1-MacOSX-"$mac_architecture".sh -o ~/miniconda3/miniconda.sh
bash ~/miniconda3/miniconda.sh -b -u -p ~/miniconda3
rm -rf ~/miniconda3/miniconda.sh

# Add miniconda3 to the PATH to use `conda` command.
export PATH="$HOME/miniconda3/bin:$PATH"

# Initialize conda. This replaces calling `conda init bash`.
# Conda init command requires a shell restart which should not be done on BK.
source "$HOME/miniconda3/etc/profile.d/conda.sh"

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
