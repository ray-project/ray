#!/usr/bin/env bash

# This file exists for Jenkins compatiblity

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE:-$0}")" && pwd)"

export RUN_TUNE_TESTS=1
export RUN_DOC_TESTS=1
export RUN_SGD_TESTS=1
export CI_BUILD_FROM_SOURCE=1

bash "$ROOT_DIR"/entry_point.sh
