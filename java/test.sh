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
# TODO(hchen): Ideally, we should use the following bazel command to run Java tests. However, if there're skipped tests,
# TestNG will exit with code 2. And bazel treats it as test failure.
# bazel test //java:all_tests --action_env=ENABLE_MULTI_LANGUAGE_TESTS=1 --test_output="errors" || cluster_exit_code=$?
ENABLE_MULTI_LANGUAGE_TESTS=1 java -jar $ROOT_DIR/../bazel-bin/java/all_tests_deploy.jar $ROOT_DIR/testng.xml|| cluster_exit_code=$?

# exit_code == 2 means there are some tests skiped.
if [ $cluster_exit_code -ne 2 ] && [ $cluster_exit_code -ne 0 ] ; then
    exit $cluster_exit_code
fi

echo "Running tests under single-process mode."
# bazel test //java:all_tests --jvmopt="-Dray.run-mode=SINGLE_PROCESS" --test_output="errors" || single_exit_code=$?
java -jar -Dray.run-mode="SINGLE_PROCESS" $ROOT_DIR/../bazel-bin/java/all_tests_deploy.jar $ROOT_DIR/testng.xml || single_exit_code=$?

# exit_code == 2 means there are some tests skiped.
if [ $single_exit_code -ne 2 ] && [ $single_exit_code -ne 0 ] ; then
    exit $single_exit_code
fi

popd
