#!/usr/bin/env bash

# This needs to be run in the root directory

# Cause the script to exit if a single command fails.
set -e
set -x

bazel build "//:actor_state_accessor_test" "//:libray_redis_module.so"

# Start Redis.
./bazel-bin/redis-server \
    --loglevel warning \
    --loadmodule ./bazel-bin/libray_redis_module.so \
    --port 6379 &
sleep 1s

while ./bazel-bin/actor_state_accessor_test; do sleep 0.1; done && false

./bazel-bin/redis-cli -p 6379 shutdown
sleep 1s
