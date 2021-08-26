#!/usr/bin/env bash

ROOT_DIR=$(builtin cd "$(dirname "${BASH_SOURCE:-$0}")" || exit; pwd)
WORKSPACE_DIR="${ROOT_DIR}/../.."

# Installs conda and python 3.7
MINIMAL_INSTALL=1 PYTHON=3.7 "${WORKSPACE_DIR}/ci/travis/install-dependencies.sh"

# Create new conda env
source /opt/miniconda/etc/profile.d/conda.sh
conda create -y -n minimal python=3.7
conda activate minimal

# Install Ray wheels
python -m pip install -e "${WORKSPACE_DIR}/python/"

# Re-install psutil
CC=gcc python -m pip install -U psutil setproctitle==1.2.2 colorama --target="${WORKSPACE_DIR}/python/ray/thirdparty_files"

# Install test requirements
CC=gcc python -m pip install -U \
  pytest==5.4.3 \
  numpy
