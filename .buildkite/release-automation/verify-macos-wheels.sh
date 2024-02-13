#!/bin/bash

set -euo pipefail

set -x
echo "Architecture: "$1""
export PYTHON_VERSION="${PYTHON_VERSION:-3.8}"
export RAY_VERSION="${RAY_VERSION:-2.9.1}"
export RAY_HASH="${RAY_HASH:-cfbf98c315cfb2710c56039a3c96477d196de049}"

mkdir -p ~/miniconda3
curl https://repo.anaconda.com/miniconda/Miniconda3-py38_23.1.0-1-MacOSX-"$1".sh -o ~/miniconda3/miniconda.sh
bash ~/miniconda3/miniconda.sh -b -u -p ~/miniconda3
rm -rf ~/miniconda3/miniconda.sh
~/miniconda3/bin/conda init bash

export PATH="$HOME/miniconda3/bin:$PATH"
source $HOME/miniconda3/etc/profile.d/conda.sh # Initialize conda. Conda init requires a shell restart which should not be done on BK.

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
