#!/bin/bash

set -euo pipefail

export DOCKER_BUILDKIT=1

# On runtime, we always build as if it is not a PR.
export BUILDKITE_PULL_REQUEST="false"
export REMOTE_CACHE_URL="${BAZEL_REMOTE_CACHE_URL}"

echo "--- Pull CI test base"

IMAGE_CI_TEST_BASE="${CI_TMP_REPO}:${IMAGE_PREFIX}-ci-test-base"

docker pull "${IMAGE_CI_TEST_BASE}"

echo "--- Build CI ml base"

IMAGE_CI_ML_BASE="${CI_TMP_REPO}:${IMAGE_PREFIX}-ci-ml-base"

docker build --progress=plain \
    --build-arg REMOTE_CACHE_URL \
    --build-arg DOCKER_IMAGE_BASE_TEST="${IMAGE_CI_TEST_BASE}" \
    -t "${IMAGE_CI_ML_BASE}" \
    -f ci/docker/base.ml.Dockerfile .

echo "--- Build CI ml"

IMAGE_CI_ML="${CI_TMP_REPO}:${IMAGE_PREFIX}-ci-ml"

docker build --progress=plain \
  --build-arg DOCKER_IMAGE_BASE_BUILD="${IMAGE_CI_ML_BASE}" \
  --build-arg BUILDKITE_PULL_REQUEST \
  --build-arg BUILDKITE_COMMIT \
  -t "${IMAGE_CI_ML}" \
  -f ci/docker/build.Dockerfile .

docker push "${IMAGE_CI_ML}"
