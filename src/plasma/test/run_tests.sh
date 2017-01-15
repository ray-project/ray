#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e

./src/plasma/plasma_store -s /tmp/plasma_store_socket_1 -m 0 &
sleep 1
./src/plasma/manager_tests
killall plasma_store
./src/plasma/serialization_tests

./src/common/thirdparty/redis/src/redis-server --loglevel warning --loadmodule ./src/common/redis_module/libray_redis_module.so &
redis_pid=$!
sleep 1
# flush the redis server
../common/thirdparty/redis/src/redis-cli flushall &
sleep 1
./src/plasma/plasma_store -s /tmp/store1 -m 1000000000 &
plasma1_pid=$!
./src/plasma/plasma_manager -m /tmp/manager1 -s /tmp/store1 -h 127.0.0.1 -p 11111 -r 127.0.0.1:6379 &
plasma2_pid=$!
./src/plasma/plasma_store -s /tmp/store2 -m 1000000000 &
plasma3_pid=$!
./src/plasma/plasma_manager -m /tmp/manager2 -s /tmp/store2 -h 127.0.0.1 -p 22222 -r 127.0.0.1:6379 &
plasma4_pid=$!
sleep 1

./src/plasma/client_tests

kill $plasma4_pid
kill $plasma3_pid
kill $plasma2_pid
kill $plasma1_pid
kill $redis_pid
