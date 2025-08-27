#!/bin/bash

set -euo pipefail

PYTHON_CODE="$(python -c "import sys; v=sys.version_info; print(f'py{v.major}{v.minor}')")"
echo "--- Python version: ${PYTHON_CODE}"

mkdir -p tmp-raydeps

echo "ray[all]==3.0.0.dev0" > tmp-raydeps/requirements.txt

# Remove the GPU constraints
cp python/requirements_compiled.txt tmp-raydeps/requirements_compiled.txt
sed -i '/^--extra-index-url /d' tmp-raydeps/requirements_compiled.txt
sed -i '/^--find-links /d' tmp-raydeps/requirements_compiled.txt

bazel run //ci/raydepsets:raydepsets -- build ci/raydepsets/rayimg.depsets.yaml

rm -rf tmp-raydeps
echo "--- Done"
