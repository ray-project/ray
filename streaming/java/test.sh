#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e
# Show explicitly which commands are currently running.
set -x

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

run_testng() {
    "$@" || exit_code=$?
    # exit_code == 2 means there are skipped tests.
    if [ $exit_code -ne 2 ] && [ $exit_code -ne 0 ] ; then
        exit $exit_code
    fi
}

echo "Linting Java code with checkstyle."
bazel test //streaming/java:all --test_tag_filters="checkstyle" --build_tests_only

echo "Running streaming tests."
run_testng java -cp $ROOT_DIR/../../bazel-bin/streaming/java/all_streaming_tests_deploy.jar\
 org.testng.TestNG -d /tmp/ray_streaming_java_test_output $ROOT_DIR/testng.xml

echo "Testing maven install."
mvn clean install -DskipTests
popd
