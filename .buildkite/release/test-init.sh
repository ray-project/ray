#!/bin/bash

set -euo pipefail

if [[ ${BUILDKITE_COMMIT} == "HEAD" ]]; then
  BUILDKITE_COMMIT="$(git rev-parse HEAD)"
  export BUILDKITE_COMMIT
fi

aws ecr get-login-password --region us-west-2 | \
    docker login --username AWS --password-stdin 029272617770.dkr.ecr.us-west-2.amazonaws.com

bash release/gcloud_docker_login.sh release/aws2gce_iam.json
export PATH="${PWD}/google-cloud-sdk/bin:$PATH"

if [[ "${AUTOMATIC:-0}" == "1" && "${BUILDKITE_BRANCH}" == "master" ]]; then
  export REPORT_TO_RAY_TEST_DB=1
fi

RUN_FLAGS=()

if [[ "${AUTOMATIC:-0}" == "0" || "${BUILDKITE_BRANCH}" == "releases/"* ]]; then
  RUN_FLAGS+=(--run-jailed-tests)
fi
if [[ "${BUILDKITE_BRANCH}" != "releases/"* ]]; then
  RUN_FLAGS+=(--run-unstable-tests)
fi

echo "---- Build test steps"
bazelisk run //release:build_pipeline -- "${RUN_FLAGS[@]}" \
    | buildkite-agent pipeline upload
