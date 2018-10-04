#!/usr/bin/env bash

# This needs to be run in the build tree, which is normally ray/build

# Cause the script to exit if a single command fails.
set -ex

LaunchRedis() {
    port=$1
    # if [[ "${RAY_USE_NEW_GCS}" = "on" ]]; then
        ./src/credis/redis/src/redis-server \
            --loglevel warning \
            --loadmodule ./src/credis/build/src/libmember.so \
            --loadmodule ./src/common/redis_module/libray_redis_module.so \
            --port $port &
    # else
    #     ./src/common/thirdparty/redis/src/redis-server \
    #         --loglevel warning \
    #         --loadmodule ./src/common/redis_module/libray_redis_module.so \
    #         --port $port &
    # fi
    sleep 1s
}


# Start the Redis shards.
LaunchRedis 6379
LaunchRedis 6380
# Register the shard location with the primary shard.
./src/common/thirdparty/redis/src/redis-cli set NumRedisShards 1
./src/common/thirdparty/redis/src/redis-cli rpush RedisShards 127.0.0.1:6380

if [ -z "$RAY_USE_NEW_GCS" ]; then
  ./src/common/db_tests
  ./src/common/io_tests
  ./src/common/task_tests
  ./src/common/redis_tests
  ./src/common/task_table_tests
  ./src/common/object_table_tests
fi

./src/common/thirdparty/redis/src/redis-cli -p 6379 shutdown
./src/common/thirdparty/redis/src/redis-cli -p 6380 shutdown
