#!/usr/bin/env bash

bazel --nosystem_rc --nohome_rc build //:example
if [[ "$OSTYPE" == "darwin"* ]]; then
    install_name_tool -change bazel-out/darwin-opt/bin/cpp/libray_api.so ./thirdparty/lib/libray_api.so ./bazel-bin/example.so
    install_name_tool -change bazel-out/darwin-opt/bin/cpp/libray_api.so ./thirdparty/lib/libray_api.so ./bazel-bin/example
    ./bazel-bin/example
else
    ./bazel-bin/example
fi
