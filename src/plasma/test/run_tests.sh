#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e

./src/plasma/plasma_store_server -s /tmp/plasma_store_socket_1 -m 0 &
sleep 1
./src/plasma/manager_tests
killall plasma_store_server

LaunchRedis() {
    port=$1
    if [[ "${RAY_USE_NEW_GCS}" = "on" ]]; then
        ./src/credis/redis/src/redis-server \
            --loglevel warning \
            --loadmodule ./src/credis/build/src/libmember.so \
            --loadmodule ./src/common/redis_module/libray_redis_module.so \
            --port $port &
    else
        ./src/common/thirdparty/redis/src/redis-server \
            --loglevel warning \
            --loadmodule ./src/common/redis_module/libray_redis_module.so \
            --port $port &
    fi
}

# Start the Redis shards.
LaunchRedis 6379
redis_pid1=$!
LaunchRedis 6380
redis_pid2=$!
sleep 1s

# Flush the redis server
./src/common/thirdparty/redis/src/redis-cli flushall
# Register the shard location with the primary shard.
./src/common/thirdparty/redis/src/redis-cli set NumRedisShards 1
./src/common/thirdparty/redis/src/redis-cli rpush RedisShards 127.0.0.1:6380
sleep 1
./src/plasma/plasma_store_server -s /tmp/store1 -m 1000000000 &
plasma1_pid=$!
./src/plasma/plasma_manager -m /tmp/manager1 -s /tmp/store1 -h 127.0.0.1 -p 11111 -r 127.0.0.1:6379 &
plasma2_pid=$!
./src/plasma/plasma_store_server -s /tmp/store2 -m 1000000000 &
plasma3_pid=$!
./src/plasma/plasma_manager -m /tmp/manager2 -s /tmp/store2 -h 127.0.0.1 -p 22222 -r 127.0.0.1:6379 &
plasma4_pid=$!
sleep 1

./src/plasma/client_tests

kill $plasma4_pid
kill $plasma3_pid
kill $plasma2_pid
kill $plasma1_pid
kill $redis_pid1
wait $redis_pid1
kill $redis_pid2
wait $redis_pid2
