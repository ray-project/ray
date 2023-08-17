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

echo "--- Pull CI test base"

IMAGE_CI_TEST_BASE="${CI_TMP_REPO}:${IMAGE_PREFIX}-ci-test-base"

docker pull "${IMAGE_CI_TEST_BASE}"
docker tag "${IMAGE_CI_TEST_BASE}" cr.ray.io/rayproject/oss-ci-base_test
docker tag "${IMAGE_CI_TEST_BASE}" oss-ci-base_test  # For old wanda spec file.

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
