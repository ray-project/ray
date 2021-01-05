#!/usr/bin/env bash

# This needs to be run in the root directory.

# Cause the script to exit if a single command fails.
set -e
set -x

bazel build "//:object_manager_stress_test" "//:object_manager_test" "//:plasma_store_server"

# Get the directory in which this script is executing.
SCRIPT_DIR="$(dirname "$0")"
RAY_ROOT="$SCRIPT_DIR/../../.."
# Makes $RAY_ROOT an absolute path.
RAY_ROOT="$(cd "$RAY_ROOT" && pwd)"
if [ -z "$RAY_ROOT" ] ; then
  exit 1
fi
# Ensure we're in the right directory.
if [ ! -d "$RAY_ROOT/python" ]; then
  echo "Unable to find root Ray directory. Has this script moved?"
  exit 1
fi

REDIS_MODULE="./bazel-bin/libray_redis_module.so"
LOAD_MODULE_ARGS=(--loadmodule "${REDIS_MODULE}")
STORE_EXEC="./bazel-bin/plasma_store_server"
GCS_SERVER_EXEC="./bazel-bin/gcs_server"

# Allow cleanup commands to fail.
bazel run //:redis-cli -- -p 6379 shutdown || true
bazel run //:redis-cli -- -p 6380 shutdown || true
sleep 1s
bazel run //:redis-server -- --loglevel warning "${LOAD_MODULE_ARGS[@]}" --port 6379 &
bazel run //:redis-server -- --loglevel warning "${LOAD_MODULE_ARGS[@]}" --port 6380 &
sleep 1s
# Run tests.
./bazel-bin/object_manager_stress_test $STORE_EXEC $GCS_SERVER_EXEC
sleep 1s
# Use timeout=1000ms for the Wait tests.
./bazel-bin/object_manager_test $STORE_EXEC 1000 $GCS_SERVER_EXEC
bazel run //:redis-cli -- -p 6379 shutdown
bazel run //:redis-cli -- -p 6380 shutdown
