#!/usr/bin/env bash

# This needs to be run in the root directory

# Cause the script to exit if a single command fails.
set -e
set -x

bazel build "//:redis_gcs_client_test" "//:subscription_executor_test" "//:asio_test" "//:libray_redis_module.so"
bazel build "//:redis_actor_info_accessor_test" "//:redis_job_info_accessor_test"

# Start Redis.
./bazel-bin/redis-server \
    --loglevel warning \
    --loadmodule ./bazel-bin/libray_redis_module.so \
    --port 6379 &
sleep 1s

./bazel-bin/redis_gcs_client_test
./bazel-bin/subscription_executor_test
./bazel-bin/asio_test
./bazel-bin/redis_actor_info_accessor_test
./bazel-bin/redis_job_info_accessor_test

./bazel-bin/redis-cli -p 6379 shutdown
sleep 1s
