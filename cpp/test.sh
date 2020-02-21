#!/bin/sh

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

api_test=$($ROOT_DIR/../bazel-bin/cpp/api_test --gtest_filter=ray_api_test_case.*)
wrap_test=$($ROOT_DIR/../bazel-bin/cpp/wrap_test --gtest_filter=ray_marshall.*)
slow_function_test=$($ROOT_DIR/../bazel-bin/cpp/slow_function_test)
echo "${api_test}"
[[ ${api_test} =~ "FAILED" ]] && exit 1
echo "${wrap_test}"
[[ ${wrap_test} =~ "FAILED" ]] && exit 1
echo "${slow_function_test}"
[[ ${slow_function_test} =~ "FAILED" ]] && exit 1
echo "cpp worker ci test finished"

