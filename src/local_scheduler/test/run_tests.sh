#!/usr/bin/env bash

# This needs to be run in the build tree, which is normally ray/build

# Cause the script to exit if a single command fails.
set -e

./src/common/thirdparty/redis/src/redis-server --loglevel warning --loadmodule ./src/common/redis_module/libray_redis_module.so --port 6379 &
./src/common/thirdparty/redis/src/redis-server --loglevel warning --loadmodule ./src/common/redis_module/libray_redis_module.so --port 6380 &
sleep 1s
./src/plasma/plasma_store -s /tmp/plasma_store_socket_1 -m 100000000 &
sleep 0.5s
./src/local_scheduler/local_scheduler_tests
./src/common/thirdparty/redis/src/redis-cli shutdown
killall plasma_store
