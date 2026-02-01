#!/bin/bash

set -euo pipefail

PYTHON_VERSION=${1:-}

if [[ -z "${PYTHON_VERSION}" ]]; then
    FILENAME="requirements_compiled.txt"
else
    FILENAME="requirements_compiled_py${PYTHON_VERSION}.txt"
fi

mkdir -p /tmp/ray-deps

# Remove the GPU constraints, numpy, scipy, and pandas pin (vLLM 0.15.0+ requires numpy>=2, compatible scipy, and pandas>=2.0)
cp "python/${FILENAME}" "/tmp/ray-deps/${FILENAME}"
sed -e '/^--extra-index-url /d' -e '/^--find-links /d' -e '/^numpy==/d' -e '/^scipy==/d' -e '/^pandas==/d' "/tmp/ray-deps/${FILENAME}" > "/tmp/ray-deps/${FILENAME}.tmp"
mv "/tmp/ray-deps/${FILENAME}.tmp" "/tmp/ray-deps/${FILENAME}"
