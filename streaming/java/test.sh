#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e
# Show explicitly which commands are currently running.
set -x

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

echo "build ray streaming"
bazel build //streaming/java:all

# Check that ray libstreaming_java doesn't include symbols from ray by accident.
# Otherwise the symbols may conflict.
symbols_conflict=$(nm bazel-bin/streaming/libstreaming_java.so | grep TaskFinisherInterface || true)
if [ -n "${symbols_conflict}" ]; then
    echo "streaming should not include symbols from ray: ${symbols_conflict}"
    exit 1
fi

echo "Linting Java code with checkstyle."
bazel test //streaming/java:all --test_tag_filters="checkstyle" --build_tests_only

echo "Running streaming tests."
java -cp "$ROOT_DIR"/../../bazel-bin/streaming/java/all_streaming_tests_deploy.jar\
 org.testng.TestNG -d /tmp/ray_streaming_java_test_output "$ROOT_DIR"/testng.xml ||
exit_code=$?
if [ -z ${exit_code+x} ]; then
  exit_code=0
fi
echo "Streaming TestNG results"
if [ -f "/tmp/ray_streaming_java_test_output/testng-results.xml" ] ; then
  cat /tmp/ray_streaming_java_test_output/testng-results.xml
else
  echo "Test result file doesn't exist"
fi

# exit_code == 2 means there are skipped tests.
if [ $exit_code -ne 2 ] && [ $exit_code -ne 0 ] ; then
    if [ -d "/tmp/ray_streaming_java_test_output/" ] ; then
      echo "all test output"
      for f in /tmp/ray_streaming_java_test_output/*.{log,xml}; do
        if [ -f "$f" ]; then
          echo "Cat file $f"
          cat "$f"
        elif [[ -d $f ]]; then
          echo "$f is a directory"
        fi
      done
    fi
    for f in /home/travis/build/ray-project/ray/hs_err*log; do
      if [ -f "$f" ]; then
        echo "Cat file $f"
        cat "$f"
      fi
    done
    exit $exit_code
fi

echo "Testing maven install."
cd "$ROOT_DIR"/../../java
echo "build ray maven deps"
bazel build gen_maven_deps
echo "maven install ray"
mvn -Dorg.slf4j.simpleLogger.defaultLogLevel=WARN clean install -DskipTests
cd "$ROOT_DIR"
echo "maven install ray streaming"
mvn -Dorg.slf4j.simpleLogger.defaultLogLevel=WARN clean install -DskipTests
