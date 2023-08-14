#!/bin/bash

set -euo pipefail

TMP_DIR="$(mktemp -d)"

curl -sL 'https://github.com/ray-project/rayci/releases/download/0.1/wanda-linux-amd64' -o "$TMP_DIR/wanda"
chmod +x "$TMP_DIR/wanda"
WANDA=("$TMP_DIR/wanda")

export DOCKER_BUILDKIT=1

# On runtime, we always build as if it is not a PR.
export BUILDKITE_PULL_REQUEST="false"
export REMOTE_CACHE_URL="${BAZEL_REMOTE_CACHE_URL}"

echo "--- Pull CI test base"

IMAGE_CI_TEST_BASE="${CI_TMP_REPO}:${IMAGE_PREFIX}-ci-test-base"

docker pull "${IMAGE_CI_TEST_BASE}"
docker tag "${IMAGE_CI_TEST_BASE}" cr.ray.io/rayproject/oss-ci-base_test

echo "--- Build CI ml base"

"${WANDA[@]}" ci/docker/base.ml.wanda.yaml

echo "--- Build CI ml"

IMAGE_CI_ML="${CI_TMP_REPO}:${IMAGE_PREFIX}-ci-ml"

docker build --progress=plain \
  --build-arg DOCKER_IMAGE_BASE_BUILD=cr.ray.io/rayproject/oss-ci-base_ml \
  --build-arg BUILDKITE_PULL_REQUEST \
  --build-arg BUILDKITE_COMMIT \
  -t "${IMAGE_CI_ML}" \
  -f ci/docker/build.Dockerfile .

docker push "${IMAGE_CI_ML}"
