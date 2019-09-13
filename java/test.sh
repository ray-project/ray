#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e
# Show explicitly which commands are currently running.
set -x

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

run_testng() {
    $@ || exit_code=$?
    # exit_code == 2 means there are skipped tests.
    if [ $exit_code -ne 2 ] && [ $exit_code -ne 0 ] ; then
        exit $exit_code
    fi
}

pushd $ROOT_DIR/..
echo "Linting Java code with checkstyle."
# NOTE(hchen): The `test_tag_filters` option causes bazel to ignore caches.
# Thus, we add the `build_tests_only` option to avoid re-building everything.
bazel test //java:all --test_tag_filters="checkstyle" --build_tests_only

echo "Running tests under cluster mode."
# TODO(hchen): Ideally, we should use the following bazel command to run Java tests. However, if there're skipped tests,
# TestNG will exit with code 2. And bazel treats it as test failure.
# bazel test //java:all_tests --action_env=ENABLE_MULTI_LANGUAGE_TESTS=1 --test_output="errors" || cluster_exit_code=$?
ENABLE_MULTI_LANGUAGE_TESTS=1 run_testng java -cp $ROOT_DIR/../bazel-bin/java/all_tests_deploy.jar org.testng.TestNG -d /tmp/ray_java_test_output $ROOT_DIR/testng.xml

echo "Running tests under cluster mode with direct actor call turned on."
ENABLE_MULTI_LANGUAGE_TESTS=1 ACTOR_CREATION_OPTIONS_DEFAULT_USE_DIRECT_CALL=1 run_testng java -cp $ROOT_DIR/../bazel-bin/java/all_tests_deploy.jar org.testng.TestNG -d /tmp/ray_java_test_output $ROOT_DIR/testng.xml

echo "Running tests under single-process mode."
# bazel test //java:all_tests --jvmopt="-Dray.run-mode=SINGLE_PROCESS" --test_output="errors" || single_exit_code=$?
run_testng java -Dray.run-mode="SINGLE_PROCESS" -cp $ROOT_DIR/../bazel-bin/java/all_tests_deploy.jar org.testng.TestNG -d /tmp/ray_java_test_output $ROOT_DIR/testng.xml

echo "Running streaming tests."
run_testng java -cp $ROOT_DIR/../bazel-bin/java/streaming_tests_deploy.jar org.testng.TestNG -d /tmp/ray_java_test_output $ROOT_DIR/streaming/testng.xml

popd

pushd $ROOT_DIR
echo "Testing maven install."
mvn clean install -DskipTests
popd
