#!/usr/bin/env bash

# This needs to be run in the build tree, which is normally ray/python/ray/core

# Cause the script to exit if a single command fails.
set -e
set -x

# Get the directory in which this script is executing.
SCRIPT_DIR="`dirname \"$0\"`"
RAY_ROOT="$SCRIPT_DIR/../../.."
RAY_ROOT="`( cd \"$RAY_ROOT\" && pwd )`"
if [ -z "$RAY_ROOT" ] ; then
  exit 1
fi
# Ensure we're in the right directory.
if [ ! -d "$RAY_ROOT/python" ]; then
  echo "Unable to find root Ray directory. Has this script moved?"
  exit 1
fi

CORE_DIR="$RAY_ROOT/python/ray/core"
REDIS_DIR="$CORE_DIR/src/common/thirdparty/redis/src"
REDIS_MODULE="$CORE_DIR/src/common/redis_module/libray_redis_module.so"
STORE_EXEC="$CORE_DIR/src/plasma/plasma_store"

echo "$STORE_EXEC"
echo "$REDIS_DIR/redis-server --loglevel warning --loadmodule $REDIS_MODULE --port 6379"
echo "$REDIS_DIR/redis-cli -p 6379 shutdown"

# Allow cleanup commands to fail.
killall plasma_store || true
$REDIS_DIR/redis-cli -p 6379 shutdown || true
sleep 1s
$REDIS_DIR/redis-server --loglevel warning --loadmodule $REDIS_MODULE --port 6379 &
sleep 1s

# Run tests.
$CORE_DIR/src/ray/object_manager/object_manager_stress_test $STORE_EXEC
sleep 1s
$CORE_DIR/src/ray/object_manager/object_manager_test $STORE_EXEC
$REDIS_DIR/redis-cli -p 6379 shutdown
sleep 1s

# Include raylet integration test once it's ready.
# $CORE_DIR/src/ray/raylet/object_manager_integration_test $STORE_EXEC
