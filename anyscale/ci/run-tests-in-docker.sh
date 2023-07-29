#!/bin/bash

set -euo pipefail

SCRIPT_ROOT="anyscale/ci"
if [[ "$1" == "serve" ]]; then
  TEST_SCRIPT="${SCRIPT_ROOT}/run-serve-tests.sh"
  IMAGE_SUFFIX="ci-build"
elif [[ "$1" == "data" ]]; then
  TEST_SCRIPT="${SCRIPT_ROOT}/run-data-tests.sh"
  IMAGE_SUFFIX="ci-ml"
else
  echo "Usage: $0 [serve|data]"
  exit 1
fi

echo "--- Pull CI build image"

IMAGE_CI_BUILD="${CI_TMP_REPO}:${IMAGE_PREFIX}-${IMAGE_SUFFIX}"
docker pull "${IMAGE_CI_BUILD}"

echo "--- Run CI build"

docker run --rm -ti \
  --env BUILDKITE_JOB_ID \
  --env BUILDKITE_COMMIT \
  --env BUILDKITE_LABEL \
  --env BUILDKITE_BRANCH \
  --env BUILDKITE_BUILD_URL \
  --env BUILDKITE_BUILD_ID \
  --env BUILDKITE_PARALLEL_JOB \
  --env BUILDKITE_PARALLEL_JOB_COUNT \
  --env BUILDKITE_MESSAGE \
  --env BUILDKITE_BUILD_NUMBER \
  --shm-size=2.5gb \
  -v /var/run/docker.sock:/var/run/docker.sock \
  "${IMAGE_CI_BUILD}" \
  /bin/bash "${TEST_SCRIPT}"
