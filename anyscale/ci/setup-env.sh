#!/bin/bash

set -euo pipefail

sudo rm -rf /artifacts
sudo mkdir -p /artifacts
sudo chown -R "${USER}:root" /artifacts

if [[ "${ARTIFACTS_DIR:-}" == "" ]]; then
    export ARTIFACTS_DIR="/artifacts/${BUILDKITE_BUILD_ID}/${BUILDKITE_JOB_ID}"
fi

if [[ "${RUNTIME_BUILD_ID:-}" == "" ]]; then
    RUNTIME_BUILD_ID="$(sha256sum <<< "${BUILDKITE_BUILD_ID}" | cut -c1-8)"
    export RUNTIME_BUILD_ID
fi

export S3_TEMP="s3://bk-premerge-first-jawfish-artifacts/tmp/runtime/${RUNTIME_BUILD_ID}"
export CI_TMP_REPO="830883877497.dkr.ecr.us-west-2.amazonaws.com/anyscale/runtime-ci-tmp"
export RUNTIME_REPO="830883877497.dkr.ecr.us-west-2.amazonaws.com/anyscale/runtime"
export RUNTIME_ML_REPO="830883877497.dkr.ecr.us-west-2.amazonaws.com/anyscale/runtime-ml"
export BAZEL_REMOTE_CACHE_URL="https://bk-premerge-first-jawfish-artifacts.s3.us-west-2.amazonaws.com/bazel/cache/runtime"
export IMAGE_PREFIX="${RUNTIME_BUILD_ID}"

# Fixes the issue where BUILDKITE_COMMIT can be just "HEAD"
if [[ "${BUILDKITE_COMMIT}" == "HEAD" ]]; then
    BUILDKITE_COMMIT="$(git rev-parse HEAD)"
    export BUILDKITE_COMMIT
fi
