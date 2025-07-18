#!/bin/bash

set -euo pipefail

PYTHON_CODE="$(python -c "import sys; v=sys.version_info; print(f'py{v.major}{v.minor}')")"
if [[ "${PYTHON_CODE}" != "py311" ]]; then
	echo "--- Python version is not 3.11"
	echo "--- Current Python version: ${PYTHON_CODE}"
	exit 1
fi

for CUDA_CODE in cpu cu121 cu128; do
	export CUDA_CODE="${CUDA_CODE}"
	export PYTHON_CODE="${PYTHON_CODE}"
	bazel run //ci/raydepsets:raydepsets -- load
done

echo "--- Done"
