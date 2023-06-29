#!/bin/bash

set -euo pipefail

export TRAVIS_COMMIT="$(git rev-parse HEAD)"

DOCKER_RUN_ARGS=(
    -e "TRAVIS=true"
    -e "TRAVIS_COMMIT=${TRAVIS_COMMIT}"
    -e "CI=true"
    -e "RAY_INSTALL_JAVA=${RAY_INSTALL_JAVA:-}"
    -e "BUILDKITE=${BUILDKITE:-}"
    -e "BUILDKITE_PULL_REQUEST=${BUILDKITE_PULL_REQUEST:-}"
    -e "BUILDKITE_BAZEL_CACHE_URL=${BUILDKITE_BAZEL_CACHE_URL:-}"
    -e "BUILD_ONE_PYTHON_ONLY=${BUILD_ONE_PYTHON_ONLY:-}"
)

RAY="$(pwd)"

docker run --rm -w /ray -v "${RAY}":/ray -v "${ARTIFACTS_DIR}:/artifacts" \
    "${DOCKER_RUN_ARGS[@]}" \
    anyscale/rayforge /bin/bash /ray/anyscale/ci/build-wheel-manylinux2014.sh
