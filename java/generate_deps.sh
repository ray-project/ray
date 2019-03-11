#!/usr/bin/env bash

set -e
set -x

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

cd $ROOT_DIR/..
bazel run //java:bazel_deps -- generate -r `pwd` -s java/third_party/workspace.bzl -d java/dependencies.yaml

set +x
set +e
