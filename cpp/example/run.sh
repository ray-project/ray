#!/usr/bin/env bash

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE:-$0}")" || exit; pwd)"

bazel --nosystem_rc --nohome_rc build //:example
if [[ "$OSTYPE" == "darwin"* ]]; then
    sudo install_name_tool -change bazel-out/darwin-opt/bin/cpp/libray_api.so ./thirdparty/lib/libray_api.so ./bazel-bin/example.so
    sudo install_name_tool -change bazel-out/darwin-opt/bin/cpp/libray_api.so ./thirdparty/lib/libray_api.so ./bazel-bin/example
    ./bazel-bin/example
else
    ./bazel-bin/example
fi
