#!/usr/bin/env bash

# This needs to be run in the build tree, which is normally ray/build

# Cause the script to exit if a single command fails.
set -e
set -x

# Start Redis.
if [[ "${RAY_USE_NEW_GCS}" = "on" ]]; then
    ./src/credis/redis/src/redis-server \
        --loglevel warning \
        --loadmodule ./src/credis/build/src/libmember.so \
        --loadmodule ./src/common/redis_module/libray_redis_module.so \
        --port 6379 &
else
    ./src/common/thirdparty/redis/src/redis-server \
        --loglevel warning \
        --loadmodule ./src/common/redis_module/libray_redis_module.so \
        --port 6379 &
fi
sleep 1s

./src/ray/gcs/client_test
./src/ray/gcs/asio_test

./src/common/thirdparty/redis/src/redis-cli -p 6379 shutdown
sleep 1s
