#!/bin/bash

set -euo pipefail

RAY_VERSION="$(python python/ray/_version.py | cut -d' ' -f1)"
export RAY_VERSION
export S3_TEMP="s3://bk-premerge-first-jawfish-artifacts/tmp/runtime/${RAYCI_BUILD_ID}"
export RUNTIME_ECR="830883877497.dkr.ecr.us-west-2.amazonaws.com"

# Always use buildkit.
export DOCKER_BUILDKIT=1

# Default cuda is also tagged as "gpu".
readonly ML_CUDA_VERSION="cu121"
export ML_CUDA_VERSION

if [[ "${RAY_RELEASE_BUILD:-}" == "" ]]; then
    if [[ "${RAY_VERSION:-}" =~ dev ]]; then
        export RAY_RELEASE_BUILD="false"
    else
        export RAY_RELEASE_BUILD="true"
    fi
fi

UPSTREAM_BRANCH="master"
if [[ "${RAY_VERSION}" != "3.0.0.dev0" ]]; then
    UPSTREAM_BRANCH="releases/${RAY_VERSION}"
fi
export UPSTREAM_BRANCH
echo "UPSTREAM_BRANCH=$UPSTREAM_BRANCH"
