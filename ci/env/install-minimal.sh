#!/usr/bin/env bash

ROOT_DIR=$(builtin cd "$(dirname "${BASH_SOURCE:-$0}")" || exit; pwd)
WORKSPACE_DIR="${ROOT_DIR}/../.."

# Installs conda and python 3.7
MINIMAL_INSTALL=1 PYTHON=${PYTHON-3.7} "${WORKSPACE_DIR}/ci/env/install-dependencies.sh"

# Re-install Ray wheels
rm -rf "${WORKSPACE_DIR}/python/ray/thirdparty_files"
rm -rf "${WORKSPACE_DIR}/python/ray/pickle5_files"
eval "${WORKSPACE_DIR}/ci/ci.sh build"

# Install test requirements
python -m pip install -U \
  pytest==5.4.3 \
  numpy
