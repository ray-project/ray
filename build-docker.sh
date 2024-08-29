#!/bin/bash
# shellcheck disable=SC2086
# This script is for users to build docker images locally. It is most useful for users wishing to edit the
# base-deps, or ray images. This script is *not* tested.

VERSION="2.35.0"
GPU=""
BASE_IMAGE="nvidia/cuda:12.4.1-cudnn-devel-ubuntu22.04"
#WHEEL_URL="https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-3.0.0.dev0-cp39-cp39-manylinux2014_x86_64.whl"
#CPP_WHEEL_URL="https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray_cpp-3.0.0.dev0-cp39-cp39-manylinux2014_x86_64.whl"
WHEEL_URL="https://files.pythonhosted.org/packages/9a/88/9bbad3defb451113002eac56ed391b9fe943311fead0db5340cb7c30ef2f/ray-2.35.0-cp310-cp310-manylinux2014_x86_64.whl"
CPP_WHEEL_URL="https://files.pythonhosted.org/packages/63/a0/e6401cba6d98f227901d66cf44a3d4faff4151547e07ac7a39a65557dcc7/ray_cpp-2.35.0-cp310-cp310-manylinux2014_x86_64.whl"
PYTHON_VERSION="3.10"

BUILD_ARGS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --gpu)
            GPU="-gpu"
            BASE_IMAGE="nvidia/cuda:12.4.1-cudnn-devel-ubuntu22.04"
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
            # Python version to install. e.g. 3.9
            # Changing python versions may require a different wheel.
            # If not provided defaults to 3.9
            shift
            PYTHON_VERSION="$1"
        ;;
        *)
            echo "Usage: build-docker.sh [ --gpu ] [ --base-image ] [ --no-cache-build ] [ --shas-only ] [ --build-development-image ] [ --build-examples ] [ --python-version ]"
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
    docker buildx build "${BUILD_ARGS[@]}"
    --build-arg BASE_IMAGE="$BASE_IMAGE"
    --build-arg PYTHON_VERSION="${PYTHON_VERSION}"
    --platform linux/amd64
    -t "abridgelambda/base-deps:$VERSION$GPU" --push "docker/base-deps"
)

if [[ "$OUTPUT_SHA" == "YES" ]]; then
    IMAGE_SHA="$("${BUILD_CMD[@]}")"
    echo "abridgelambda/base-deps:$VERSION$GPU SHA:$IMAGE_SHA"
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
#cp ./.whl/* "$RAY_BUILD_DIR/.whl/" 
echo "NOT COPYING ./.whl to ${RAY_BUILD_DIR}/.whl/"
ls "$RAY_BUILD_DIR/.whl/"
cp python/requirements_compiled.txt "$RAY_BUILD_DIR"
cp docker/ray/Dockerfile "$RAY_BUILD_DIR"

WHEEL="$(basename "$WHEEL_DIR"/.whl/ray-*.whl)"

BUILD_CMD=(
    docker buildx build "${BUILD_ARGS[@]}"
    --build-arg FULL_BASE_IMAGE="abridgelambda/base-deps:$VERSION$GPU"
    --build-arg WHEEL_PATH=".whl/${WHEEL}"
    --platform linux/amd64
    -t "abridgelambda/ray:$VERSION$GPU" --push "$RAY_BUILD_DIR"
)

if [[ "$OUTPUT_SHA" == "YES" ]]; then
    IMAGE_SHA="$("${BUILD_CMD[@]}")"
    echo "abridgelambda/ray:$VERSION$GPU SHA:$IMAGE_SHA"
else
    "${BUILD_CMD[@]}"
fi

echo "=== Building the ray-ml image ==="
BUILD_CMD=(
    docker buildx build "${BUILD_ARGS[@]}"
    --build-arg FULL_BASE_IMAGE="abridgelambda/ray:$VERSION$GPU"
    --platform linux/amd64
    -t "abridgelambda/ray-ml:$VERSION$GPU" --push -f docker/ray-ml/Dockerfile .
)

if [[ "$OUTPUT_SHA" == "YES" ]]; then
    IMAGE_SHA="$("${BUILD_CMD[@]}")"
    echo "abridgelambda/ray-ml:$VERSION$GPU SHA:$IMAGE_SHA"
else
    "${BUILD_CMD[@]}"
fi

rm -rf "$WHEEL_DIR"
