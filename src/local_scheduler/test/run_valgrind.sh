#!/usr/bin/env bash

# This needs to be run in the build tree, which is normally ray/build

set -x

# Cause the script to exit if a single command fails.
set -e

# Start the Redis shards.
./src/common/thirdparty/redis/src/redis-server --loglevel warning --loadmodule ./src/common/redis_module/libray_redis_module.so --port 6379 &
./src/common/thirdparty/redis/src/redis-server --loglevel warning --loadmodule ./src/common/redis_module/libray_redis_module.so --port 6380 &
sleep 1s
# Register the shard location with the primary shard.
./src/common/thirdparty/redis/src/redis-cli set NumRedisShards 1
./src/common/thirdparty/redis/src/redis-cli rpush RedisShards 127.0.0.1:6380

./src/plasma/plasma_store -s /tmp/plasma_store_socket_1 -m 100000000 &
sleep 0.5s
valgrind --track-origins=yes --leak-check=full --show-leak-kinds=all --leak-check-heuristics=stdstring --error-exitcode=1 ./src/local_scheduler/local_scheduler_tests
./src/common/thirdparty/redis/src/redis-cli shutdown
./src/common/thirdparty/redis/src/redis-cli -p 6380 shutdown
killall plasma_store
