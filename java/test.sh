#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e
# Show explicitly which commands are currently running.
set -x

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)
java -version

pushd "$ROOT_DIR"
  echo "Check java code format."
  # check google java style
  mvn -T16 spotless:check
  # check naming and others
  mvn -T16 checkstyle:check
popd

run_testng() {
    local exit_code
    if "$@"; then
        exit_code=0
    else
        exit_code=$?
    fi
    # exit_code == 2 means there are skipped tests.
    if [ $exit_code -ne 2 ] && [ $exit_code -ne 0 ] ; then
        if [ $exit_code -gt 128 ] ; then
            # Test crashed. Print the driver log for diagnosis.
            cat /tmp/ray/session_latest/logs/java-core-driver-*
        fi
        find . -name "hs_err_*log" -exec cat {} +
        exit $exit_code
    fi
}

pushd "$ROOT_DIR"/..
echo "Build java maven deps."
bazel build //java:gen_maven_deps

echo "Build test jar."
bazel build //java:all_tests_deploy.jar

# Enable multi-worker feature in Java test
TEST_ARGS=(-Dray.job.num-java-workers-per-process=10)

echo "Running tests under cluster mode."
# TODO(hchen): Ideally, we should use the following bazel command to run Java tests. However, if there're skipped tests,
# TestNG will exit with code 2. And bazel treats it as test failure.
# bazel test //java:all_tests --config=ci || cluster_exit_code=$?
run_testng java -cp "$ROOT_DIR"/../bazel-bin/java/all_tests_deploy.jar "${TEST_ARGS[@]}" org.testng.TestNG -d /tmp/ray_java_test_output "$ROOT_DIR"/testng.xml

echo "Running tests under single-process mode."
# bazel test //java:all_tests --jvmopt="-Dray.run-mode=SINGLE_PROCESS" --config=ci || single_exit_code=$?
run_testng java -Dray.run-mode="SINGLE_PROCESS" -cp "$ROOT_DIR"/../bazel-bin/java/all_tests_deploy.jar "${TEST_ARGS[@]}" org.testng.TestNG -d /tmp/ray_java_test_output "$ROOT_DIR"/testng.xml

echo "Running connecting existing cluster tests."
case "${OSTYPE}" in
  linux*) ip=$(hostname -I | awk '{print $1}');;
  darwin*) ip=$(ipconfig getifaddr en0);;
  *) echo "Can't get ip address for ${OSTYPE}"; exit 1;;
esac
RAY_BACKEND_LOG_LEVEL=debug ray start --head --port=6379 --redis-password=123456
RAY_BACKEND_LOG_LEVEL=debug java -cp bazel-bin/java/all_tests_deploy.jar -Dray.address="$ip:6379"\
 -Dray.redis.password='123456' -Dray.job.code-search-path="$PWD/bazel-bin/java/all_tests_deploy.jar" io.ray.test.MultiDriverTest
ray stop

echo "Running documentation demo code."
docdemo_path="java/test/src/main/java/io/ray/docdemo/"
for file in "$docdemo_path"*.java; do
  file=${file#"$docdemo_path"}
  class=${file%".java"}
  echo "Running $class"
  java -cp bazel-bin/java/all_tests_deploy.jar "io.ray.docdemo.$class"
done
popd

pushd "$ROOT_DIR"
echo "Testing maven install."
mvn -Dorg.slf4j.simpleLogger.defaultLogLevel=WARN clean install -DskipTests -Dcheckstyle.skip
# Ensure mvn test works
mvn test -pl test -Dtest="io.ray.test.HelloWorldTest"
popd
