#!/bin/bash
# shellcheck disable=SC2086
# This script is for users to build docker images locally. It is most useful for users wishing to edit the
# base-deps, or ray images. This script is *not* tested.

GPU=""
BASE_IMAGE="ubuntu:22.04"
WHEEL_URL="https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-3.0.0.dev0-cp39-cp39-manylinux2014_x86_64.whl"
CPP_WHEEL_URL="https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray_cpp-3.0.0.dev0-cp39-cp39-manylinux2014_x86_64.whl"
PYTHON_VERSION="3.9"
USE_LOCAL_SOURCE=""

BUILD_ARGS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --gpu)
            GPU="-gpu"
            BASE_IMAGE="nvidia/cuda:11.8.0-cudnn8-devel-ubuntu22.04"
        ;;
        --base-image)
            # Override for the base image.
            shift
            BASE_IMAGE="$1"
        ;;
        --no-cache-build)
            BUILD_ARGS+=("--no-cache")
        ;;
        --shas-only)
            # output the SHA sum of each build. This is useful for scripting tests,
            # especially when builds of different versions are running on the same machine.
            # It also can facilitate cleanup.
            OUTPUT_SHA=YES
            BUILD_ARGS+=("-q")
        ;;
        --python-version)
            shift
            PYTHON_VERSION="$1"
        ;;
        --local-source)
            USE_LOCAL_SOURCE="true"
        ;;
        *)
            echo "Usage: build-docker.sh [ --gpu ] [ --base-image BASE_IMAGE ] [ --no-cache-build ] [ --shas-only ] [ --python-version PYTHON_VERSION ] [ --local-source ]"
            exit 1
    esac
    shift
done

export DOCKER_BUILDKIT=1

# Build base-deps image
if [[ "$OUTPUT_SHA" != "YES" ]]; then
    echo "=== Building base-deps image ===" >/dev/stderr
fi

BUILD_CMD=(
    docker build "${BUILD_ARGS[@]}"
    --build-arg BASE_IMAG="$BASE_IMAGE"
    --build-arg PYTHON_VERSION="${PYTHON_VERSION}"
    -t "rayproject/base-deps:dev$GPU" "docker/base-deps"
)

if [[ "$OUTPUT_SHA" == "YES" ]]; then
    IMAGE_SHA="$("${BUILD_CMD[@]}")"
    echo "rayproject/base-deps:dev$GPU SHA:$IMAGE_SHA"
else
    "${BUILD_CMD[@]}"
fi


# Build ray image
if [[ "$OUTPUT_SHA" != "YES" ]]; then
    echo "=== Building ray image ===" >/dev/stderr
fi

RAY_BUILD_DIR="$(mktemp -d)"
mkdir -p "$RAY_BUILD_DIR/.whl"

if [[ -n "$USE_LOCAL_SOURCE" ]]; then
    # Use local source to build the Python wheel
    echo "=== Using local source to build the Python wheel ===" >/dev/stderr
    pushd python
    ./build-wheel-manylinux2014.sh
    cp dist/*.whl "$RAY_BUILD_DIR/.whl/"
    popd
else
    # Use remote wheel for Python
    wget --quiet "$WHEEL_URL" -P "$RAY_BUILD_DIR/.whl"
fi

# Always download CPP wheel from remote
wget --quiet "$CPP_WHEEL_URL" -P "$RAY_BUILD_DIR/.whl"

# Copy necessary files to the build directory
cp python/requirements_compiled.txt "$RAY_BUILD_DIR"
cp docker/ray/Dockerfile "$RAY_BUILD_DIR"

WHEEL="$(basename "$WHEEL_DIR"/.whl/ray-*.whl)"

BUILD_CMD=(
    docker build "${BUILD_ARGS[@]}"
    --build-arg FULL_BASE_IMAGE="rayproject/base-deps:dev$GPU"
    --build-arg WHEEL_PATH=".whl/${WHEEL}"
    -t "rayproject/ray:dev$GPU" "$RAY_BUILD_DIR"
)

if [[ "$OUTPUT_SHA" == "YES" ]]; then
    IMAGE_SHA="$("${BUILD_CMD[@]}")"
    echo "rayproject/ray:dev$GPU SHA:$IMAGE_SHA"
else
    "${BUILD_CMD[@]}"
fi

# Clean up
rm -rf "$RAY_BUILD_DIR"
