#!/usr/bin/env bash

set -ex

MEMORY_SIZE="20G"
SHM_SIZE="20G"

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

DOCKER_SHA=$($ROOT_DIR/../../../build-docker.sh --output-sha --no-cache)
SUPPRESS_OUTPUT=$ROOT_DIR/../suppress_output
echo "Using Docker image" $DOCKER_SHA

cd ../../../
ls -la

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA ./ci/perf_integration_tests/run_perf_integration.sh
