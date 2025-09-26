#!/bin/bash

set -euo pipefail

mkdir -p /tmp/ray-deps

# Remove the GPU constraints
cp python/requirements_compiled.txt /tmp/ray-deps/requirements_compiled.txt
sed -e '/^--extra-index-url /d' -e '/^--find-links /d' /tmp/ray-deps/requirements_compiled.txt > /tmp/ray-deps/requirements_compiled.txt.tmp
mv /tmp/ray-deps/requirements_compiled.txt.tmp /tmp/ray-deps/requirements_compiled.txt
