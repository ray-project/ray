#!/usr/bin/env bash

# This needs to be run in the root directory.

# Try to find an unused port for raylet to use.
PORTS="2000 2001 2002 2003 2004 2005 2006 2007 2008 2009"
RAYLET_PORT=0
for port in $PORTS; do
    nc -z localhost $port
    if [[ $? != 0 ]]; then
        RAYLET_PORT=$port
        break
    fi
done

if [[ $RAYLET_PORT == 0 ]]; then
    echo "WARNING: Could not find unused port for raylet to use. Exiting without running tests."
    exit
fi

# Cause the script to exit if a single command fails.
set -e
set -x

bazel build -c dbg $RAY_BAZEL_CONFIG "//:core_worker_test" "//:mock_worker"  "//:raylet" "//:raylet_monitor" "//:gcs_server" "//:libray_redis_module.so" "@plasma//:plasma_store_server"

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

REDIS_MODULE="./bazel-bin/libray_redis_module.so"
BAZEL_BIN_PREFIX="$(bazel info -c dbg $RAY_BAZEL_CONFIG bazel-bin)"
LOAD_MODULE_ARGS="--loadmodule ${REDIS_MODULE}"
STORE_EXEC="$BAZEL_BIN_PREFIX/external/plasma/plasma_store_server"
RAYLET_EXEC="$BAZEL_BIN_PREFIX/raylet"
RAYLET_MONITOR_EXEC="$BAZEL_BIN_PREFIX/raylet_monitor"
MOCK_WORKER_EXEC="$BAZEL_BIN_PREFIX/mock_worker"
GCS_SERVER_EXEC="$BAZEL_BIN_PREFIX/gcs_server"

# Allow cleanup commands to fail.
bazel run "//:redis-cli" -- -p 6379 shutdown || true
sleep 1s
bazel run "//:redis-cli" -- -p 6380 shutdown || true
sleep 1s
bazel run "//:redis-server" -- --loglevel warning ${LOAD_MODULE_ARGS} --port 6379 &
sleep 2s
bazel run "//:redis-server" -- --loglevel warning ${LOAD_MODULE_ARGS} --port 6380 &
sleep 2s
# Run tests.
bazel run -c dbg $RAY_BAZEL_CONFIG "//:core_worker_test" $STORE_EXEC $RAYLET_EXEC $RAYLET_PORT $RAYLET_MONITOR_EXEC $MOCK_WORKER_EXEC $GCS_SERVER_EXEC
sleep 1s
bazel run "//:redis-cli" -- -p 6379 shutdown
bazel run "//:redis-cli" -- -p 6380 shutdown
sleep 1s
