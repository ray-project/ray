#!/bin/bash

set -euo pipefail

CONFIG_PATH="${1:-ci/raydepsets/rayllm.depsets.yaml}"

PYTHON_CODE="$(python -c "import sys; v=sys.version_info; print(f'py{v.major}{v.minor}')")"
if [[ "${PYTHON_CODE}" != "py311" ]]; then
	echo "--- Python version is not 3.11"
	echo "--- Current Python version: ${PYTHON_CODE}"
	exit 1
fi

mkdir -p /tmp/ray-deps

# Remove the GPU constraints
cp python/requirements_compiled.txt /tmp/ray-deps/requirements_compiled.txt
sed -e '/^--extra-index-url /d' -e '/^--find-links /d' /tmp/ray-deps/requirements_compiled.txt > /tmp/ray-deps/requirements_compiled.txt.tmp
mv /tmp/ray-deps/requirements_compiled.txt.tmp /tmp/ray-deps/requirements_compiled.txt

bazel run //ci/raydepsets:raydepsets -- build "${CONFIG_PATH}"

echo "--- Done"
