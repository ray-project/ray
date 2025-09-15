#!/bin/bash

set -exuo pipefail

export RAY_DEBUG_BUILD=deps-only

PYTHON_VERSIONS=("3.9" "3.10" "3.11" "3.12")

for PYTHON_VERSION in "${PYTHON_VERSIONS[@]}"; do
  uv build --wheel --directory python/ -o ../.whl/ --force-pep517 --python "$PYTHON_VERSION"
done
