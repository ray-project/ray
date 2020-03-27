#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e

# Show explicitly which commands are currently running.
set -x

RUN_DOC_TESTS=false
RUN_TUNE_TESTS=false
RUN_SGD_TESTS=false

# Parse options
while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
    --run-doc)
      RUN_DOC_TESTS=true
      shift
      ;;
    --run-tune)
      RUN_TUNE_TESTS=true
      shift
      ;;
    --run-sgd)
      RUN_SGD_TESTS=true
      shift
      ;;
    *)
      echo "ERROR: unknown option \"$key\""
      exit -1
      ;;
  esac
done

MEMORY_SIZE="8G"
SHM_SIZE="4G"

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

DOCKER_SHA=$($ROOT_DIR/../../build-docker.sh --output-sha --no-cache)
SUPPRESS_OUTPUT=$ROOT_DIR/../suppress_output
echo "Using Docker image" $DOCKER_SHA

if [ "$RUN_TUNE_TESTS" = true ]; then
    bash $ROOT_DIR/run_tune_tests.sh ${MEMORY_SIZE} ${SHM_SIZE} $DOCKER_SHA
fi

if [ "$RUN_SGD_TESTS" = true ]; then
    bash $ROOT_DIR/run_sgd_tests.sh ${MEMORY_SIZE} ${SHM_SIZE} $DOCKER_SHA
fi

if [ "$RUN_DOC_TESTS" = true ]; then
    bash $ROOT_DIR/run_doc_examples.sh ${MEMORY_SIZE} ${SHM_SIZE} $DOCKER_SHA
fi