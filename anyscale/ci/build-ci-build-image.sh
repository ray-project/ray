#!/bin/bash

set -euo pipefail

export DOCKER_BUILDKIT=1

# On runtime, we always build as if it is not a PR.
export BUILDKITE_PULL_REQUEST="false"
export REMOTE_CACHE_URL="${BAZEL_REMOTE_CACHE_URL}"

echo "--- Build CI test base"

IMAGE_CI_TEST_BASE="${CI_TMP_REPO}:${IMAGE_PREFIX}-ci-test-base"

docker build --progress=plain \
  --build-arg REMOTE_CACHE_URL \
  --build-arg BUILDKITE_PULL_REQUEST \
  --build-arg BUILDKITE_COMMIT \
  -t "$IMAGE_CI_TEST_BASE" \
  -f ci/docker/base.test.Dockerfile .

echo "--- Build CI build base"

IMAGE_CI_BUILD_BASE="${CI_TMP_REPO}:${IMAGE_PREFIX}-ci-build-base"

docker build --progress=plain \
    --build-arg REMOTE_CACHE_URL \
    --build-arg BUILDKITE_PULL_REQUEST \
    --build-arg BUILDKITE_COMMIT \
    --build-arg DOCKER_IMAGE_BASE_TEST="${IMAGE_CI_TEST_BASE}" \
    -t "${IMAGE_CI_BUILD_BASE}" \
    -f ci/docker/base.build.Dockerfile .

echo "--- Build CI build"

IMAGE_CI_BUILD="${CI_TMP_REPO}:${IMAGE_PREFIX}-ci-build"

docker build --progress=plain \
  --build-arg DOCKER_IMAGE_BASE_BUILD="${IMAGE_CI_BUILD_BASE}" \
  --build-arg BUILDKITE_PULL_REQUEST \
  --build-arg BUILDKITE_COMMIT \
  -t "${IMAGE_CI_BUILD}" \
  -f ci/docker/build.Dockerfile .

docker push "${IMAGE_CI_BUILD}"
