#!/usr/bin/env bash

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE:-$0}")" || exit; pwd)"

bazel --nosystem_rc --nohome_rc build //:example
if [[ "$OSTYPE" == "darwin"* ]]; then
    # TODO: We use `install_name_tool` to walk around the issue of dynamic libraries loading in macOS.
    install_name_tool -change bazel-out/darwin-opt/bin/cpp/libray_api.so ./thirdparty/lib/libray_api.so ./bazel-bin/example.so
    install_name_tool -change bazel-out/darwin-opt/bin/cpp/libray_api.so ./thirdparty/lib/libray_api.so ./bazel-bin/example
    "${ROOT_DIR}"/bazel-bin/example
else
    LD_LIBRARY_PATH="$ROOT_DIR/thirdparty/lib" "${ROOT_DIR}"/bazel-bin/example
fi
