#/bin/bash

set -e
# Show explicitly which commands are currently running.
set -x

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

pushd $ROOT_DIR/..
bazel run //java:bazel_deps -- generate -r `pwd` -s java/third_party/workspace.bzl -d java/dependencies.yaml
popd
