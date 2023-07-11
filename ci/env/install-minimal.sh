#!/usr/bin/env bash

set -xe

# Python version can be specified as 3.7, 3.8, 3.9, etc..
if [ -z "$1" ]; then
    PYTHON_VERSION=${PYTHON-3.7}
else
    if [ "$1" = "3.7" ]; then
        PYTHON_VERSION="3.7"
    elif [ "$1" = "3.8" ]; then
        PYTHON_VERSION="3.8"
    elif [ "$1" = "3.9" ]; then
        PYTHON_VERSION="3.9"
    elif [ "$1" = "3.10" ]; then
        PYTHON_VERSION="3.10"
    elif [ "$1" = "3.11" ]; then
        PYTHON_VERSION="3.11"
    else
        echo "Unsupported Python version."
        exit 1
    fi
fi
echo "Python version is ${PYTHON_VERSION}"


ROOT_DIR=$(cd "$(dirname "$0")/$(dirname "$(test -L "$0" && readlink "$0" || echo "/")")" || exit; pwd)
WORKSPACE_DIR="${ROOT_DIR}/../.."

# Installs conda and the specified python version
MINIMAL_INSTALL=1 PYTHON=${PYTHON_VERSION} "${WORKSPACE_DIR}/ci/env/install-dependencies.sh"

# Re-install Ray wheels
rm -rf "${WORKSPACE_DIR}/python/ray/thirdparty_files"
rm -rf "${WORKSPACE_DIR}/python/ray/pickle5_files"
eval "${WORKSPACE_DIR}/ci/ci.sh build"

# Install test requirements
python -m pip install -U \
  pytest==7.0.1 \
  numpy

# Train requirements.
# TODO: make this dynamic
if [ "${TRAIN_MINIMAL_INSTALL-}" = 1 ]; then
    python -m pip install -U "ray[tune]"
fi
