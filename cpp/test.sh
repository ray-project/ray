#!/bin/sh

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

apitest=$($ROOT_DIR/../bazel-bin/cpp/apitest --gtest_filter=ray_api_test_case.*)
wraptest=$($ROOT_DIR/../bazel-bin/cpp//wraptest --gtest_filter=ray_marshall.*)
echo "${apitest}"
[[ ${apitest} =~ "FAILED" ]] && exit 1
echo "${wraptest}"
[[ ${wraptest} =~ "FAILED" ]] && exit 1
echo "cpp worker ci test finished"

