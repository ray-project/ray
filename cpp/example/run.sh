#!/usr/bin/env bash

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)
bazel build //:example
if [[ "$OSTYPE" == "darwin"* ]]; then
    DYLD_LIBRARY_PATH="$ROOT_DIR/thirdparty/lib" $ROOT_DIR/bazel-bin/example
else
    LD_LIBRARY_PATH="$ROOT_DIR/thirdparty/lib" $ROOT_DIR/bazel-bin/example
fi
