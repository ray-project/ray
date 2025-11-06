#!/bin/bash
# shellcheck disable=SC2086
# This script is for users to build docker images locally. It is most useful for users wishing to edit the
# base-deps, or ray images. This script is *not* tested.

GPU=""
BASE_IMAGE="ubuntu:22.04"
WHEEL_URL="https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-3.0.0.dev0-cp310-cp310-manylinux2014_x86_64.whl"
CPP_WHEEL_URL="https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray_cpp-3.0.0.dev0-cp310-cp310-manylinux2014_x86_64.whl"
PYTHON_VERSION="3.10"

BUILD_ARGS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --gpu)
            GPU="-gpu"
            BASE_IMAGE="nvidia/cuda:12.8.1-cudnn-devel-ubuntu22.04"
        ;;
        --base-image)
            # Override for the base image.
            shift
            BASE_IMAGE="$1"
        ;;
        --progress-plain)
            # Use plain progress output instead of fancy output.
            # This is useful for CI systems that don't support fancy output.
            BUILD_ARGS+=("--progress=plain")
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
            # Python version to install. e.g. 3.9
            # Changing python versions may require a different wheel.
            # If not provided defaults to 3.9
            shift
            PYTHON_VERSION="$1"
        ;;
        *)
            echo "Usage: build-docker.sh [ --gpu ] [ --base-image ] [ --no-cache-build ] [ --shas-only ] [ --progress-plain] [ --python-version ]"
            exit 1
    esac
    shift
done

export DOCKER_BUILDKIT=1


# Build base-deps image
if [[ "$OUTPUT_SHA" != "YES" ]]; then
    echo "=== Building base-deps image ===" >/dev/stderr
fi

RAY_DEPS_BUILD_DIR="$(mktemp -d)"

cp docker/base-deps/Dockerfile "${RAY_DEPS_BUILD_DIR}/."
mkdir -p "${RAY_DEPS_BUILD_DIR}/python"
cp python/requirements_compiled.txt "${RAY_DEPS_BUILD_DIR}/python/requirements_compiled.txt"

BUILD_CMD=(
    docker build "${BUILD_ARGS[@]}"
    --build-arg BASE_IMAG="$BASE_IMAGE"
    --build-arg PYTHON_VERSION="${PYTHON_VERSION}"
    -t "rayproject/base-deps:dev$GPU" "${RAY_DEPS_BUILD_DIR}"
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
wget --quiet "$WHEEL_URL" -P "$RAY_BUILD_DIR/.whl"
wget --quiet "$CPP_WHEEL_URL" -P "$RAY_BUILD_DIR/.whl"
cp docker/ray/Dockerfile "$RAY_BUILD_DIR"

WHEEL="$(basename "$WHEEL_DIR"/.whl/ray-*.whl)"

BUILD_CMD=(
    docker build "${BUILD_ARGS[@]}"
    --build-arg FULL_BASE_IMAGE="rayproject/base-deps:dev$GPU"
    --build-arg WHEEL_PATH=".whl/${WHEEL}"
    -t "rayproject/ray:dev$GPU" "${RAY_BUILD_DIR}"
)

if [[ "$OUTPUT_SHA" == "YES" ]]; then
    IMAGE_SHA="$("${BUILD_CMD[@]}")"
    echo "rayproject/ray:dev$GPU SHA:$IMAGE_SHA"
else
    "${BUILD_CMD[@]}"
fi

rm -rf "$WHEEL_DIR"
