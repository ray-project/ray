#!/bin/bash

set -euo pipefail

export TRAVIS_PULL_REQUEST=true
export TRAVIS_COMMIT="$(git rev-parse HEAD)"
export CI=true
export RAY_DEBUG_BUILD=debug
export BUILD_ONE_PYTHON_ONLY=""

DOCKER_RUN_ARGS=(
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
)

RAY="$(pwd)"

docker run --rm -w /ray -v "${RAY}":/ray -v "${ARTIFACTS_DIR}:/artifacts" \
    "${DOCKER_RUN_ARGS[@]}" \
    anyscale/rayforge /bin/bash /ray/runtime/ci/build-wheel-manylinux2014.sh
