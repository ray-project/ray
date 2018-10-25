#!/usr/bin/env bash

# This needs to be run in the build tree, which is normally ray/build

# Cause the script to exit if a single command fails.
set -e
set -x

# Get the directory in which this script is executing.
SCRIPT_DIR="`dirname \"$0\"`"
RAY_ROOT="$SCRIPT_DIR/../../.."
# Makes $RAY_ROOT an absolute path.
RAY_ROOT="`( cd \"$RAY_ROOT\" && pwd )`"
if [ -z "$RAY_ROOT" ] ; then
  exit 1
fi
# Ensure we're in the right directory.
if [ ! -d "$RAY_ROOT/python" ]; then
  echo "Unable to find root Ray directory. Has this script moved?"
  exit 1
fi

CORE_DIR="$RAY_ROOT/build"
REDIS_MODULE="$CORE_DIR/src/ray/gcs/redis_module/libray_redis_module.so"
REDIS_DIR="$CORE_DIR/src/ray/thirdparty/redis/src"

if [[ "${RAY_USE_NEW_GCS}" = "on" ]]; then
    REDIS_SERVER="$CORE_DIR/src/credis/redis/src/redis-server"

    CREDIS_MODULE="$CORE_DIR/src/credis/build/src/libmember.so"
    LOAD_MODULE_ARGS="--loadmodule ${CREDIS_MODULE} --loadmodule ${REDIS_MODULE}"
else
    REDIS_SERVER="${REDIS_DIR}/redis-server"
    LOAD_MODULE_ARGS="--loadmodule ${REDIS_MODULE}"
fi

STORE_EXEC="$CORE_DIR/src/plasma/plasma_store_server"

# Allow cleanup commands to fail.
$REDIS_DIR/redis-cli -p 6379 shutdown || true
sleep 1s
${REDIS_SERVER} --loglevel warning ${LOAD_MODULE_ARGS} --port 6379 &
sleep 1s
# Run tests.
$CORE_DIR/src/ray/object_manager/object_manager_stress_test $STORE_EXEC
sleep 1s
$CORE_DIR/src/ray/object_manager/object_manager_test $STORE_EXEC
$REDIS_DIR/redis-cli -p 6379 shutdown
sleep 1s

# Include raylet integration test once it's ready.
# $CORE_DIR/src/ray/raylet/object_manager_integration_test $STORE_EXEC
