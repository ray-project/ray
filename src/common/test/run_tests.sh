#!/usr/bin/env bash

# This needs to be run in the build tree, which is normally ray/build

# Cause the script to exit if a single command fails.
set -e

./src/common/thirdparty/redis/src/redis-server --loglevel warning --loadmodule ./src/common/redis_module/libray_redis_module.so &
sleep 1s
./src/common/common_tests
./src/common/db_tests
./src/common/io_tests
./src/common/task_tests
./src/common/redis_tests
./src/common/task_table_tests
if [[ "$(uname)" != "Darwin" ]]; then
  # Only run the object table test on Linux.
  ./src/common/object_table_tests
fi
./src/common/thirdparty/redis/src/redis-cli shutdown
