#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e
# Show explicitly which commands are currently running.
set -x

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

pushd $ROOT_DIR/..
echo "Linting Java code with checkstyle."
bazel test //java:all --test_tag_filters="checkstyle"

echo "Running tests under cluster mode."
ENABLE_MULTI_LANGUAGE_TESTS=1 bazel test //java:all_tests --test_output="errors" || cluster_exit_code=$?

# exit_code == 2 means there are some tests skiped.
if [ $cluster_exit_code -ne 2 ] && [ $cluster_exit_code -ne 0 ] ; then
    exit $cluster_exit_code
fi

echo "Running tests under single-process mode."
bazel test //java:all_tests --jvmopt="-Dray.run-mode=SINGLE_PROCESS" --test_output="errors" || single_exit_code=$?

# exit_code == 2 means there are some tests skiped.
if [ $single_exit_code -ne 2 ] && [ $single_exit_code -ne 0 ] ; then
    exit $single_exit_code
fi

popd
