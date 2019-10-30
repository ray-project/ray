#!/usr/bin/env bash

# Run all streaming c++ tests using streaming queue, instead of plasma queue 
# This needs to be run in the root directory.

# Cause the script to exit if a single command fails.
set -e
set -x
export STREAMING_METRICS_MODE=DEV

# Get the directory in which this script is executing.
SCRIPT_DIR="`dirname \"$0\"`"

# Get the directory in which this script is executing.
SCRIPT_DIR="`dirname \"$0\"`"
RAY_ROOT="$SCRIPT_DIR/../../.."
# Makes $RAY_ROOT an absolute path.
RAY_ROOT="`( cd \"$RAY_ROOT\" && pwd )`"
if [ -z "$RAY_ROOT" ] ; then
  exit 1
fi

bazel build "//:core_worker_test" "//:mock_worker"  "//:raylet" "//:libray_redis_module.so" "@plasma//:plasma_store_server"
bazel build //streaming/src:streaming_test_worker
bazel build //streaming/src:streaming_writer_tests_with_streamingqueue

# Ensure we're in the right directory.
if [ ! -d "$RAY_ROOT/python" ]; then
  echo "Unable to find root Ray directory. Has this script moved?"
  exit 1
fi

REDIS_MODULE="./bazel-bin/libray_redis_module.so"
LOAD_MODULE_ARGS="--loadmodule ${REDIS_MODULE}"
STORE_EXEC="./bazel-bin/external/plasma/plasma_store_server"
RAYLET_EXEC="./bazel-bin/raylet"
STREAMING_TEST_WORKER_EXEC="./bazel-bin/streaming/src/streaming_test_worker"

# Allow cleanup commands to fail.
bazel run //:redis-cli -- -p 6379 shutdown || true
sleep 1s
bazel run //:redis-cli -- -p 6380 shutdown || true
sleep 1s
bazel run //:redis-server -- --loglevel warning ${LOAD_MODULE_ARGS} --port 6379 &
sleep 2s
bazel run //:redis-server -- --loglevel warning ${LOAD_MODULE_ARGS} --port 6380 &
sleep 2s
# Run tests.
#./bazel-bin/streaming/src/streaming_writer_tests_with_streamingqueue $STORE_EXEC $RAYLET_EXEC $STREAMING_TEST_WORKER_EXEC --gtest_filter=StreamingTest/StreamingExactlySameTest.streaming_exactly_same_operator_test/1
#./bazel-bin/streaming/src/streaming_writer_tests_with_streamingqueue $STORE_EXEC $RAYLET_EXEC $STREAMING_TEST_WORKER_EXEC --gtest_filter=StreamingTest/StreamingWriterTest.streaming_rescale_exactly_once_test/0
./bazel-bin/streaming/src/streaming_writer_tests_with_streamingqueue $STORE_EXEC $RAYLET_EXEC $STREAMING_TEST_WORKER_EXEC
sleep 1s
bazel run //:redis-cli -- -p 6379 shutdown
bazel run //:redis-cli -- -p 6380 shutdown
sleep 1s
