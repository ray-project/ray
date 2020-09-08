#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e
# Show explicitly which commands are currently running.
set -x

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

run_testng() {
    local exit_code
    if "$@"; then
        exit_code=0
    else
        exit_code=$?
    fi
    # exit_code == 2 means there are skipped tests.
    if [ $exit_code -ne 2 ] && [ $exit_code -ne 0 ] ; then
        find . -name "hs_err_*log" -exec cat {} +
        exit $exit_code
    fi
}

pushd "$ROOT_DIR"/..
echo "Linting Java code with checkstyle."
# NOTE(hchen): The `test_tag_filters` option causes bazel to ignore caches.
# Thus, we add the `build_tests_only` option to avoid re-building everything.
bazel test //java:all --test_tag_filters="checkstyle" --build_tests_only

echo "Build java maven deps."
bazel build //java:gen_maven_deps

echo "Build test jar."
bazel build //java:all_tests_deploy.jar

# Enable multi-worker feature in Java test
TEST_ARGS=(-Dray.raylet.config.num_workers_per_process_java=10 -Dray.job.num-java-workers-per-process=10)

echo "Running tests under cluster mode."
# TODO(hchen): Ideally, we should use the following bazel command to run Java tests. However, if there're skipped tests,
# TestNG will exit with code 2. And bazel treats it as test failure.
# bazel test //java:all_tests --action_env=ENABLE_MULTI_LANGUAGE_TESTS=1 --config=ci || cluster_exit_code=$?
ENABLE_MULTI_LANGUAGE_TESTS=1 run_testng java -cp "$ROOT_DIR"/../bazel-bin/java/all_tests_deploy.jar "${TEST_ARGS[@]}" org.testng.TestNG -d /tmp/ray_java_test_output "$ROOT_DIR"/testng.xml

echo "Running tests under single-process mode."
# bazel test //java:all_tests --jvmopt="-Dray.run-mode=SINGLE_PROCESS" --config=ci || single_exit_code=$?
run_testng java -Dray.run-mode="SINGLE_PROCESS" -cp "$ROOT_DIR"/../bazel-bin/java/all_tests_deploy.jar "${TEST_ARGS[@]}" org.testng.TestNG -d /tmp/ray_java_test_output "$ROOT_DIR"/testng.xml

echo "Running connecting existing cluster tests"
case "${OSTYPE}" in
  linux*) ip=$(hostname -I | awk '{print $1}');;
  darwin*) ip=$(ipconfig getifaddr en0);;
  *) echo "Can't get ip address for ${OSTYPE}"; exit 1;;
esac
RAY_BACKEND_LOG_LEVEL=debug ray start --head --redis-port=6379 --redis-password=123456 --include-java --code-search-path="$PWD/bazel-bin/java/all_tests_deploy.jar"
RAY_BACKEND_LOG_LEVEL=debug java -cp bazel-bin/java/all_tests_deploy.jar -Dray.redis.address="$ip:6379"\
 -Dray.redis.password='123456' -Dray.job.code-search-path="$PWD/bazel-bin/java/all_tests_deploy.jar" io.ray.test.MultiDriverTest
ray stop
popd

pushd "$ROOT_DIR"
echo "Testing maven install."
mvn -Dorg.slf4j.simpleLogger.defaultLogLevel=WARN clean install -DskipTests
# Ensure mvn test works
mvn test -pl test -Dtest="io.ray.test.HelloWorldTest"
popd
