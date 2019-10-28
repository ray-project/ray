#!/usr/bin/env bash

# This needs to be run in the build tree, which is normally ray/build

# Cause the script to exit if a single command fails.
set -e
export STREAMING_METRICS_MODE=DEV

# Get the directory in which this script is executing.
SCRIPT_DIR="`dirname \"$0\"`"

# switch to ray dir
cd "${SCRIPT_DIR}/../../.."
bazel build @plasma//:plasma_store_server
bazel build //streaming/src:streaming_message_ring_buffer_tests
bazel build //streaming/src:streaming_message_serialization_tests
bazel build //streaming/src:streaming_barrier_merge_tests
bazel build //streaming/src:streaming_asio_tests
bazel build //streaming/src:streaming_fbs_tests
bazel build //streaming/src:streaming_perf_tests
bazel build //streaming/src:streaming_utility_tests
bazel build //streaming/src:streaming_writer_tests
bazel build //streaming/src:streaming_exactly_same_tests
bazel build //streaming/src:streaming_reader_tests
bazel build //streaming/src:streaming_rescale_tests
bazel build //streaming/src:buffer_pool_tests

PLASMA_STORE_SERVER_PATH=bazel-bin/external/plasma/plasma_store_server

function kill_if_exists() {
  pid_list=`ps -ef | grep "$1" | grep -v "grep" | awk '{print $2}'`
  if [[ $pid_list ]]; then
     kill -9 $pid_list
  fi

}

function remove_store_and_raylet_deps() {
    kill_if_exists "raylet/raylet"
    kill_if_exists "plasma_store_server"
    kill_if_exists "redis-server"
    sleep 1
}


function common_test() {
    ./bazel-bin/streaming/src/streaming_message_ring_buffer_tests
    ./bazel-bin/streaming/src/streaming_message_serialization_tests
    ./bazel-bin/streaming/src/streaming_barrier_merge_tests
    ./bazel-bin/streaming/src/streaming_asio_tests
    ./bazel-bin/streaming/src/streaming_fbs_tests
    ./bazel-bin/streaming/src/streaming_perf_tests
    ./bazel-bin/streaming/src/streaming_utility_tests
    ./bazel-bin/streaming/src/buffer_pool_tests
    echo "Finish common test, start plasma store server to test reader and writer."
}

function plasma_queue_test() {
    remove_store_and_raylet_deps
    export STREAMING_DISABLE_RAY_QUEUE=on

    ./${PLASMA_STORE_SERVER_PATH} -s /tmp/store_streaming_tests -m 1000000000 &
    sleep 1
    unset STREAMING_ENABLE_METRICS
    unset STREAMING_ENABLE_RAY_PIPE
    ./bazel-bin/streaming/src/streaming_writer_tests
    ./bazel-bin/streaming/src/streaming_rescale_tests
    ./bazel-bin/streaming/src/streaming_exactly_same_tests
    killall plasma_store_server
    unset STREAMING_DISABLE_RAY_QUEUE
    echo "plasma queue test succ"
}

if [ $# -eq 0 ]; then
    common_test
    plasma_queue_test
else
    echo "set specifc test case $1"
    start_test_with_raylet $1
fi
