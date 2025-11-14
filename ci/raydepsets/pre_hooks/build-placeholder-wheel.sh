#!/bin/bash

set -exuo pipefail

if [[ -z "$1" ]]; then
  echo "Usage: $0 <python_version>" >&2
  exit 1
fi
PYTHON_VERSION=$1

export RAY_DEBUG_BUILD=deps-only

UV_BUILD_COMMAND="uv build --wheel --directory python/ -o ../.whl/ --python \"$PYTHON_VERSION\""

if [[ "$OSTYPE" == "darwin"* ]]; then
  docker run --rm \
  -v "$PWD":/ray -w /ray --platform linux/x86_64 \
  quay.io/pypa/manylinux2014_x86_64 \
  bash -lc '
  set -e
  export RAY_DEBUG_BUILD=deps-only
  eval "$UV_BUILD_COMMAND"'
else
  eval "$UV_BUILD_COMMAND"
fi
