#!/bin/bash

set -euo pipefail

mkdir -p /tmp/ray-deps

# Remove the GPU constraints
cp python/requirements_compiled_py313.txt /tmp/ray-deps/requirements_compiled_py313.txt
sed -e '/^--extra-index-url /d' -e '/^--find-links /d' /tmp/ray-deps/requirements_compiled_py313.txt > /tmp/ray-deps/requirements_compiled_py313.txt.tmp
mv /tmp/ray-deps/requirements_compiled_py313.txt.tmp /tmp/ray-deps/requirements_compiled_py313.txt
