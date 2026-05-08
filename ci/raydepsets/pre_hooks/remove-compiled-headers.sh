#!/bin/bash

set -euo pipefail

PYTHON_VERSION=${1:-}

if [[ -z "${PYTHON_VERSION}" ]]; then
    FILENAME="requirements_compiled.txt"
else
    FILENAME="requirements_compiled_py${PYTHON_VERSION}.txt"
fi

mkdir -p /tmp/ray-deps

# Remove the GPU constraints
cp "python/${FILENAME}" "/tmp/ray-deps/${FILENAME}"
sed -e '/^--extra-index-url /d' -e '/^--find-links /d' "/tmp/ray-deps/${FILENAME}" > "/tmp/ray-deps/${FILENAME}.tmp"
mv "/tmp/ray-deps/${FILENAME}.tmp" "/tmp/ray-deps/${FILENAME}"
