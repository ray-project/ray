#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e

# Show explicitly which commands are currently running.
set -x


MEMORY_SIZE="8G"
SHM_SIZE="4G"

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

DOCKER_SHA=$($ROOT_DIR/../../build-docker.sh --output-sha --no-cache)
SUPPRESS_OUTPUT=$ROOT_DIR/../suppress_output
echo "Using Docker image" $DOCKER_SHA

[[ ! -z "$RUN_TUNE_TESTS" ]] && bash $ROOT_DIR/run_tune_tests.sh ${MEMORY_SIZE} ${SHM_SIZE} $DOCKER_SHA
[[ ! -z "$RUN_DOC_TESTS" ]] && bash $ROOT_DIR/run_doc_tests.sh ${MEMORY_SIZE} ${SHM_SIZE} $DOCKER_SHA
[[ ! -z "$RUN_SGD_TESTS" ]] && bash $ROOT_DIR/run_sgd_tests.sh ${MEMORY_SIZE} ${SHM_SIZE} $DOCKER_SHA