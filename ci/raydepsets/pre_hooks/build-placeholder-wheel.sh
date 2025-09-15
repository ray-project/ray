#!/bin/bash

set -exuo pipefail

PYTHON_VERSION=$1

export RAY_DEBUG_BUILD=deps-only

uv build --wheel --directory python/ -o ../.whl/ --force-pep517 --python "$PYTHON_VERSION"
