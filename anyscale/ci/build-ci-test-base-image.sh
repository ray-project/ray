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

docker push "${IMAGE_CI_TEST_BASE}"
