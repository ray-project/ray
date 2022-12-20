#!/usr/bin/env bash

#Cause the script to exit if a single command fails.
set -e

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE:-$0}")" || exit; pwd)"

bazel --nosystem_rc --nohome_rc build //:example
if [[ "$OSTYPE" == "darwin"* ]]; then
    DYLD_LIBRARY_PATH="$ROOT_DIR/thirdparty/lib" "${ROOT_DIR}"/bazel-bin/example
else
    LD_LIBRARY_PATH="$ROOT_DIR/thirdparty/lib" "${ROOT_DIR}"/bazel-bin/example
fi
