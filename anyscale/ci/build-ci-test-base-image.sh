#!/bin/bash

set -euo pipefail

TMP_DIR="$(mktemp -d)"

curl -sL 'https://github.com/ray-project/rayci/releases/download/v0.1.3/wanda-linux-amd64' -o "$TMP_DIR/wanda"
chmod +x "$TMP_DIR/wanda"
WANDA=(
  "$TMP_DIR/wanda"
  -name_prefix cr.ray.io/rayproject/
)

export DOCKER_BUILDKIT=1

# On runtime, we always build as if it is not a PR.
export BUILDKITE_PULL_REQUEST="false"
export REMOTE_CACHE_URL="${BAZEL_REMOTE_CACHE_URL}"

echo "--- Build CI test base"

"${WANDA[@]}" ci/docker/base.test.wanda.yaml

IMAGE_CI_TEST_BASE="${CI_TMP_REPO}:${IMAGE_PREFIX}-ci-test-base"
docker tag cr.ray.io/rayproject/oss-ci-base_test "${IMAGE_CI_TEST_BASE}"
docker push "${IMAGE_CI_TEST_BASE}"
