#!/bin/bash

set -euo pipefail

TRAVIS_COMMIT="$(git rev-parse HEAD)"
export TRAVIS_COMMIT

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

FORGE_IMAGE="${CI_TMP_REPO}:${IMAGE_PREFIX}-forge"
docker pull "${FORGE_IMAGE}"

docker run --rm -w /ray -v "${RAY}":/ray -v "${ARTIFACTS_DIR}:/artifacts" \
    "${DOCKER_RUN_ARGS[@]}" "${FORGE_IMAGE}" \
    /bin/bash /ray/anyscale/ci/build-wheel-manylinux2014.sh

WHEELS=("${ARTIFACTS_DIR}"/*.whl)
for WHEEL in "${WHEELS[@]}"; do
    echo "Uploading ${WHEEL}"
    WHEEL_BASENAME="$(basename "${WHEEL}")"
    aws s3 cp "${WHEEL}" "${S3_TEMP}/${WHEEL_BASENAME}"
done
