#!/bin/bash

set -exuo pipefail

if [[ -z "$1" ]]; then
  echo "Usage: $0 <python_version>" >&2
  exit 1
fi
PYTHON_VERSION=$1

export RAY_DEBUG_BUILD=deps-only


if [[ "$OSTYPE" == "darwin"* ]]; then
  docker run --rm \
  -v "$PWD":/ray -w /ray --platform linux/x86_64 \
  quay.io/pypa/manylinux2014_x86_64 \
  bash -lc '
  set -e
  export RAY_DEBUG_BUILD=deps-only
  uv build --wheel --directory python/ -o ../.whl/ --python "$PYTHON_VERSION"'
else
  uv build --wheel --directory python/ -o ../.whl/ --python "$PYTHON_VERSION"
fi
