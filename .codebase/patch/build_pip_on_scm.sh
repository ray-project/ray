#!/bin/bash
source .codebase/patch/_codebase_prepare.sh

if [ -z "$CUSTOM_RAY_UPLOAD_TOS" ]; then
    export CUSTOM_RAY_UPLOAD_TOS=true
fi

if [[ -n "${BUILD_VERSION:-}" ]]; then
    echo "byted_scm_version = \"$BUILD_VERSION\"" > ./python/ray/byted_version.py
fi

export BAZEL_LIMIT_CPUS=8
if [[ -n "${CUSTOM_PYTHON_VERSION:-}" ]]; then
    python/build-wheel-manylinux2014.sh ${CUSTOM_PYTHON_VERSION}
else
    python/build-wheel-manylinux2014.sh cp37-cp37m,cp38-cp38,cp39-cp39,cp310-cp310,cp311-cp311,cp312-cp312
fi
cp -r .whl output/

pushd .whl
if [[ -n "${BUILD_VERSION:-}" ]]; then
    if [[ -n "${CUSTOM_RAY_UPLOAD_TOS:-}" ]]; then
        for filename in *; do
            if [[ $filename == *"x86_64.whl" ]]; then
                toscli -bucket inf-batch-ray-build -accessKey K59XHNKC1P93V992Z8L2 put -name ${BUILD_VERSION}/$filename $filename
            fi
        done
    fi
fi
popd