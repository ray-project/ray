#!/bin/bash

set -euo pipefail

PYTHON_VERSION=${1:-3.10}

mkdir -p /tmp/ray-deps

# Remove the GPU constraints
cp python/requirements_compiled_py"${PYTHON_VERSION}".txt /tmp/ray-deps/requirements_compiled_py"${PYTHON_VERSION}".txt
sed -e '/^--extra-index-url /d' -e '/^--find-links /d' /tmp/ray-deps/requirements_compiled_py"${PYTHON_VERSION}".txt > /tmp/ray-deps/requirements_compiled_py"${PYTHON_VERSION}".txt.tmp
mv /tmp/ray-deps/requirements_compiled_py"${PYTHON_VERSION}".txt.tmp /tmp/ray-deps/requirements_compiled_py"${PYTHON_VERSION}".txt
