#!/bin/bash

set -euo pipefail

export RAY_VERSION="3.0.0.dev0"
export S3_TEMP="s3://bk-premerge-first-jawfish-artifacts/tmp/runtime/${RAYCI_BUILD_ID}"
export RUNTIME_ECR="830883877497.dkr.ecr.us-west-2.amazonaws.com"

# Always use buildkit.
export DOCKER_BUILDKIT=1

# Default cuda is also tagged as "gpu".
readonly ML_CUDA_VERSION="cu118"
export ML_CUDA_VERSION

if [[ "${RAY_RELEASE_BUILD:-}" == "" ]]; then
    if [[ "${RAY_VERSION:-}" =~ dev ]]; then
        export RAY_RELEASE_BUILD="false"
    else
        export RAY_RELEASE_BUILD="true"
    fi
fi
