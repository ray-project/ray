#!/bin/bash

set -exuo pipefail

if [[ -z "$1" ]]; then
  echo "Usage: $0 <python_version>" >&2
  exit 1
fi
PYTHON_VERSION=$1

export RAY_DEBUG_BUILD=deps-only

uv build --wheel --directory python/ -o ../.whl/ --python "$PYTHON_VERSION"
