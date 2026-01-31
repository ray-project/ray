#!/bin/bash

set -euo pipefail

CONFIG_PATH="${1:-ci/raydepsets/configs/rayllm.depsets.yaml}"

mkdir -p /tmp/ray-deps

# Remove the GPU constraints, numpy, scipy, and pandas pin (vLLM 0.15.0+ requires numpy>=2, compatible scipy, and pandas>=2.0)
cp python/requirements_compiled.txt /tmp/ray-deps/requirements_compiled.txt
sed -e '/^--extra-index-url /d' -e '/^--find-links /d' -e '/^numpy==/d' -e '/^scipy==/d' -e '/^pandas==/d' /tmp/ray-deps/requirements_compiled.txt > /tmp/ray-deps/requirements_compiled.txt.tmp
mv /tmp/ray-deps/requirements_compiled.txt.tmp /tmp/ray-deps/requirements_compiled.txt

bazel run //ci/raydepsets:raydepsets -- build "${CONFIG_PATH}"

echo "--- Done"
