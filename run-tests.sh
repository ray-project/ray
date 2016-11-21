#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

pushd "$ROOT_DIR/src/common/test"
  ./run_tests.sh
popd

pushd "$ROOT_DIR/src/plasma"
  make test
popd
