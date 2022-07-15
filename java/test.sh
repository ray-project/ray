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
    local pid
    local exit_code
    "$@" &
    pid=$!
    if wait $pid; then
        exit_code=0
    else
        exit_code=$?
    fi
    # exit_code == 2 means there are skipped tests.
    if [ $exit_code -ne 2 ] && [ $exit_code -ne 0 ] ; then
        # Only print log files if it ran in cluster mode
        if [[ ! "$*" =~ LOCAL ]]; then
          if [ $exit_code -gt 128 ] ; then
              # Test crashed. Print the driver log for diagnosis.
              cat /tmp/ray/session_latest/logs/java-core-driver-*$pid*
          fi
        fi
        # Only print the hs_err_pid file of TestNG process
        find . -name "hs_err_pid$pid.log" -exec cat {} +
        exit $exit_code
    fi
}

run_timeout() {
  local pid
  timeout=$1
  shift 1
  "$@" &
  pid=$!
  sleep "$timeout"
  if ps -p $pid > /dev/null
  then
    echo "run_timeout process exists, kill it."
    kill -9 $pid
  else
    echo "run_timeout process not exist."
    cat /tmp/ray/session_latest/logs/java-core-driver-*$pid*
    exit 1
  fi
}

pushd "$ROOT_DIR"/..
echo "Build java maven deps."
bazel build //java:gen_maven_deps

echo "Build test jar."
bazel build //java:all_tests_shaded.jar

java/generate_jni_header_files.sh

if ! git diff --exit-code -- java src/ray/core_worker/lib/java; then
  echo "Files are changed after build. Common cases are:"
  echo "    * Java native methods doesn't match JNI files. You need to either update Java code or JNI code."
  echo "    * pom_template.xml and pom.xml doesn't match. You need to either update pom_template.xml or pom.xml."
  exit 1
fi

# NOTE(kfstrom): Java test troubleshooting only.
# Set MAX_ROUNDS to a big number (e.g. 1000) to run Java tests repeatedly.
# You may also want to modify java/testng.xml to run only a subset of test cases.
MAX_ROUNDS=1
if [ $MAX_ROUNDS -gt 1 ]; then
  export RAY_BACKEND_LOG_LEVEL=debug
fi

round=1
while true; do
  echo Starting cluster mode test round $round

  echo "Running tests under cluster mode."
  # TODO(hchen): Ideally, we should use the following bazel command to run Java tests. However, if there're skipped tests,
  # TestNG will exit with code 2. And bazel treats it as test failure.
  # bazel test //java:all_tests --config=ci || cluster_exit_code=$?
  run_testng java -cp "$ROOT_DIR"/../bazel-bin/java/all_tests_shaded.jar org.testng.TestNG -d /tmp/ray_java_test_output "$ROOT_DIR"/testng.xml

  echo Finished cluster mode test round $round
  date
  round=$((round+1))
  if (( round > MAX_ROUNDS )); then
    break
  fi
done

echo "Running tests under local mode."
run_testng java -Dray.run-mode="LOCAL" -cp "$ROOT_DIR"/../bazel-bin/java/all_tests_shaded.jar org.testng.TestNG -d /tmp/ray_java_test_output "$ROOT_DIR"/testng.xml

echo "Running connecting existing cluster tests."
case "${OSTYPE}" in
  linux*) ip=$(hostname -I | awk '{print $1}');;
  darwin*) ip=$(ipconfig getifaddr en0);;
  *) echo "Can't get ip address for ${OSTYPE}"; exit 1;;
esac
RAY_BACKEND_LOG_LEVEL=debug ray start --head --port=6379 --redis-password=123456 --node-ip-address="$ip"
RAY_BACKEND_LOG_LEVEL=debug java -cp bazel-bin/java/all_tests_shaded.jar -Dray.address="$ip:6379"\
 -Dray.redis.password='123456' -Dray.job.code-search-path="$PWD/bazel-bin/java/all_tests_shaded.jar" io.ray.test.MultiDriverTest
ray stop

echo "Running documentation demo code."
docdemo_path="java/test/src/main/java/io/ray/docdemo/"
for file in "$docdemo_path"*.java; do
  file=${file#"$docdemo_path"}
  class=${file%".java"}
  echo "Running $class"
  java -cp bazel-bin/java/all_tests_shaded.jar -Dray.raylet.startup-token=0 "io.ray.docdemo.$class"
done
popd

pushd "$ROOT_DIR"
echo "Testing maven install."
mvn -Dorg.slf4j.simpleLogger.defaultLogLevel=WARN clean install -DskipTests -Dcheckstyle.skip
# Ensure mvn test works
mvn test -pl test -Dtest="io.ray.test.HelloWorldTest"
popd

pushd "$ROOT_DIR"
echo "Running performance test."
run_timeout 60 java -cp "$ROOT_DIR"/../bazel-bin/java/all_tests_shaded.jar io.ray.performancetest.test.ActorPerformanceTestCase1
# The performance process may be killed by run_timeout, so clear ray here.
ray stop
popd
