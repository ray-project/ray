#!/bin/bash

set -euo pipefail

PYTHON_CODE="$(python -c "import sys; v=sys.version_info; print(f'py{v.major}{v.minor}')")"
echo "--- Python version: ${PYTHON_CODE}"

mkdir -p /tmp/ray-deps
mkdir -p /tmp/ray-deps/python/lock_files/ray_img

echo "ray==3.0.0.dev0" > /tmp/ray-deps/requirements.txt

# Remove the GPU constraints
cp python/requirements_compiled.txt /tmp/ray-deps/requirements_compiled.txt
sed -i '/^--extra-index-url /d' /tmp/ray-deps/requirements_compiled.txt
sed -i '/^--find-links /d' /tmp/ray-deps/requirements_compiled.txt

bazel run //ci/raydepsets:raydepsets -- build ci/raydepsets/rayimg.depsets.yaml

echo "--- Done"
