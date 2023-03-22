#!/bin/bash
source .codebase/patch/_codebase_prepare.sh
export BAZEL_LIMIT_CPUS=8
if [[ -n "${CUSTOM_PYTHON_VERSION:-}" ]]; then
    python/build-wheel-manylinux2014.sh ${CUSTOM_PYTHON_VERSION}
else
    python/build-wheel-manylinux2014.sh cp36-cp36m,cp37-cp37m,cp38-cp38,cp39-cp39
fi
cp -r .whl output/