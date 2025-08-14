#!/bin/bash

PYTHON_VERSION=$1

if [ -z "$PYTHON_VERSION" ]; then
    echo "Usage: $0 <python-version>"
    exit 1
fi

echo "PYTHON_VERSION: $PYTHON_VERSION"

which python

mkdir -p .whl

export SKIP_BAZEL_BUILD=1
export RAY_DISABLE_EXTRA_CPP=1
export RAY_PLACEHOLDER_WHEEL=1

"/opt/python/${PYTHON_VERSION}/bin/python" -m pip wheel python/ --no-deps --wheel-dir .whl/
