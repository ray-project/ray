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

echo "Build java maven deps."
bazel build //java:gen_maven_deps

echo "Build test jar."
bazel build //java:all_tests_deploy.jar
export RAY_BACKEND_LOG_LEVEL=DEBUG

echo "Running tests under cluster mode."
for i in {1..10}
do
  echo ">>>>>>>>>>>>>>>>>>>$i"
  ENABLE_MULTI_LANGUAGE_TESTS=1 run_testng java -cp $ROOT_DIR/../bazel-bin/java/all_tests_deploy.jar org.testng.TestNG -d /tmp/ray_java_test_output $ROOT_DIR/testng.xml
done
popd
