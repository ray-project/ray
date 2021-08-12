ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)
bazel build //:example
if mac; then
    DYLD_LIBRARY_PATH="$ROOT_DIR/thirdparty/lib" $ROOT_DIR/bazel-bin/example
else
    bazel run //:example
fi
