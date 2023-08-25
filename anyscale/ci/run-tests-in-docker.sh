#!/bin/bash

set -exuo pipefail

SCRIPT_ROOT="anyscale/ci"

TEST_TYPE="$1"
shift

if [[ "${TEST_TYPE}" == "serve" ]]; then
  TEST_SCRIPT="${SCRIPT_ROOT}/run-serve-tests.sh"
  IMAGE_SUFFIX="ci-build"
elif [[ "${TEST_TYPE}" == "data" ]]; then
  TEST_SCRIPT="${SCRIPT_ROOT}/run-data-tests.sh"
  IMAGE_SUFFIX="ci-ml"
else
  echo "Usage: $0 serve|data <extra args>"
  exit 1
fi

echo "--- Pull image"

CI_IMAGE="${CI_TMP_REPO}:${IMAGE_PREFIX}-${IMAGE_SUFFIX}"
docker pull "${CI_IMAGE}"

echo "--- Run tests in image"

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
  "${CI_IMAGE}" \
  /bin/bash "${TEST_SCRIPT}" "$@"
