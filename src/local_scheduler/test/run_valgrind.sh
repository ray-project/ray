#!/usr/bin/env bash

# This needs to be run in the build tree, which is normally ray/build

set -x

# Cause the script to exit if a single command fails.
set -e

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
LaunchRedis 6380
sleep 1s

# Register the shard location with the primary shard.
./src/common/thirdparty/redis/src/redis-cli set NumRedisShards 1
./src/common/thirdparty/redis/src/redis-cli rpush RedisShards 127.0.0.1:6380

./src/plasma/plasma_store_server -s /tmp/plasma_store_socket_1 -m 100000000 &
sleep 0.5s
valgrind --track-origins=yes --leak-check=full --show-leak-kinds=all --leak-check-heuristics=stdstring --error-exitcode=1 ./src/local_scheduler/local_scheduler_tests
./src/common/thirdparty/redis/src/redis-cli shutdown
./src/common/thirdparty/redis/src/redis-cli -p 6380 shutdown
killall plasma_store_server
