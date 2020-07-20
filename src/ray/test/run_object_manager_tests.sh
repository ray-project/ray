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

# Allow cleanup commands to fail.
bazel run //:redis-cli -- -p 6379 shutdown || true
sleep 1s
bazel run //:redis-server -- --loglevel warning "${LOAD_MODULE_ARGS[@]}" --port 6379 &
sleep 1s
# Run tests.
./bazel-bin/object_manager_stress_test $STORE_EXEC
sleep 1s
# Use timeout=1000ms for the Wait tests.
./bazel-bin/object_manager_test $STORE_EXEC 1000
bazel run //:redis-cli -- -p 6379 shutdown
sleep 1s

# Include raylet integration test once it's ready.
# ./bazel-bin/object_manager_integration_test $STORE_EXEC
