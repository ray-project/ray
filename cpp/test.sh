#!/bin/sh

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

api_test=$($ROOT_DIR/../bazel-bin/cpp/api_test)
echo "${api_test}"
[[ ${api_test} =~ "FAILED" ]] && exit 1
echo "cpp worker ci test finished"

