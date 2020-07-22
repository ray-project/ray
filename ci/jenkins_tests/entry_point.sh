#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e

# Show explicitly which commands are currently running.
set -x


MEMORY_SIZE="8G"
SHM_SIZE="4G"

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

function retry {
  local n=1
  local max=3

  while true; do
    if "$@"; then
      break
    fi
    if [ $n -lt $max ]; then
      ((n++))
      echo "Command failed. Attempt $n/$max:"
    else
      echo "The command has failed after $n attempts."
      exit 1
    fi
  done
}

if [[ -n "$RUN_TUNE_TESTS" ]]; then 
    retry bash "$ROOT_DIR"/run_tune_tests.sh ${MEMORY_SIZE} ${SHM_SIZE}
fi

if [[ -n "$RUN_DOC_TESTS" ]]; then
    retry bash "$ROOT_DIR"/run_doc_tests.sh ${MEMORY_SIZE} ${SHM_SIZE}
fi

if [[ -n "$RUN_SGD_TESTS" ]]; then
    retry bash "$ROOT_DIR"/run_sgd_tests.sh ${MEMORY_SIZE} ${SHM_SIZE}
fi