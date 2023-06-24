#!/bin/bash

set -euxo pipefail

IMAGE_NAME="quay.io/pypa/manylinux2014_x86_64"
IMAGE_TAG="2022-12-20-b4884d9"

export DOCKER_BUILDKIT=1

tar --mtime="UTC 2020-01-01" -c -f - civ2/rayforge/Dockerfile \
    | docker build --progress=plain -t rayforge \
        --build-arg "BASE_IMAGE=${IMAGE_NAME}:${IMAGE_TAG}" \
        -f civ2/rayforge/Dockerfile -


export TRAVIS_PULL_REQUEST=true
export TRAVIS_COMMIT="$(git rev-parse HEAD)"
export CI=true
export RAY_INSTALL_JAVA=1
export BUILDKITE=true
export BUILDKITE_PULL_REQUEST=false
export RAY_DEBUG_BUILD=debug
export BUILD_ONE_PYTHON_ONLY=""

DOCKER_RUN_ARGS=(
    # -v "${HOME}/ray-bazel-cache":/root/ray-bazel-cache
    -e "TRAVIS=true"
    -e "TRAVIS_PULL_REQUEST=${TRAVIS_PULL_REQUEST:-false}"
    -e "TRAVIS_COMMIT=${TRAVIS_COMMIT}"
    -e "CI=${CI}"
    -e "RAY_INSTALL_JAVA=${RAY_INSTALL_JAVA:-}"
    -e "BUILDKITE=${BUILDKITE:-}"
    -e "BUILDKITE_PULL_REQUEST=${BUILDKITE_PULL_REQUEST:-}"
    -e "BUILDKITE_BAZEL_CACHE_URL=${BUILDKITE_BAZEL_CACHE_URL:-}"
    -e "RAY_DEBUG_BUILD=${RAY_DEBUG_BUILD:-}"
    -e "BUILD_ONE_PYTHON_ONLY=${BUILD_ONE_PYTHON_ONLY:-}"

    -e BUILD_JAR=1
)

RAY="$(pwd)"

docker run --rm -w /ray -v "${RAY}":/ray "${DOCKER_RUN_ARGS[@]}" \
    "${MOUNT_ENV[@]}" rayforge /ray/civ2/build-wheel-manylinux2014.sh
