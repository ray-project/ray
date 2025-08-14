#!/bin/bash

PYTHON_VERSION=$1

if [ -z "$PYTHON_VERSION" ]; then
    echo "Usage: $0 <python-version>"
    exit 1
fi

python -m venv "venv_${PYTHON_VERSION}"
source "venv_${PYTHON_VERSION}/bin/activate"

export SKIP_BAZEL_BUILD=1
export RAY_DISABLE_EXTRA_CPP=1
export RAY_PLACEHOLDER_WHEEL=1
pip wheel python/ --no-deps --wheel-dir dist/
