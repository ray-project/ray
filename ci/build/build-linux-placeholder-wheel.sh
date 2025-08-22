#!/bin/bash
set -euo pipefail
PYTHON_VERSION=$1
if [ -z "$PYTHON_VERSION" ]; then
    echo "Usage: $0 <python-version>"
    exit 1
fi

which python
which python3
mkdir -p dist
export SKIP_BAZEL_BUILD=1
export RAY_DISABLE_EXTRA_CPP=1
export RAY_PLACEHOLDER_WHEEL=1
PYTHON_VERSION_TAG="cp${PYTHON_VERSION//.}"
PIP_CMD="/opt/python/${PYTHON_VERSION_TAG}-${PYTHON_VERSION_TAG}/bin/pip"
"$PIP_CMD" wheel python/ --no-deps --wheel-dir .whl/
