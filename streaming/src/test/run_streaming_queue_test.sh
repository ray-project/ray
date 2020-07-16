#!/usr/bin/env bash

# Run all streaming c++ tests using streaming queue, instead of plasma queue
# This needs to be run in the root directory.

# Try to find an unused port for raylet to use.
PORTS="2000 2001 2002 2003 2004 2005 2006 2007 2008 2009"
RAYLET_PORT=0
for port in $PORTS; do
    if ! nc -z localhost "$port"; then
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

# Get the directory in which this script is executing.
SCRIPT_DIR="$(dirname "$0")"

# Get the directory in which this script is executing.
SCRIPT_DIR="$(dirname "$0")"
RAY_ROOT="$SCRIPT_DIR/../../.."
# Makes $RAY_ROOT an absolute path.
RAY_ROOT="$(cd "$RAY_ROOT" && pwd)"
if [ -z "$RAY_ROOT" ] ; then
  exit 1
fi

bazel build "//:core_worker_test" "//:mock_worker"  "//:raylet" "//:gcs_server" "//:libray_redis_module.so" "//:plasma_store_server" "//:redis-server" "//:redis-cli"
bazel build //streaming:streaming_test_worker
bazel build //streaming:streaming_queue_tests

# Ensure we're in the right directory.
if [ ! -d "$RAY_ROOT/python" ]; then
  echo "Unable to find root Ray directory. Has this script moved?"
  exit 1
fi

REDIS_MODULE="./bazel-bin/libray_redis_module.so"
REDIS_SERVER_EXEC="./bazel-bin/external/com_github_antirez_redis/redis-server"
STORE_EXEC="./bazel-bin/plasma_store_server"
REDIS_CLIENT_EXEC="./bazel-bin/redis-cli"
RAYLET_EXEC="./bazel-bin/raylet"
STREAMING_TEST_WORKER_EXEC="./bazel-bin/streaming/streaming_test_worker"
GCS_SERVER_EXEC="./bazel-bin/gcs_server"

# Allow cleanup commands to fail.
# Run tests.
./bazel-bin/streaming/streaming_queue_tests $STORE_EXEC $RAYLET_EXEC "$RAYLET_PORT" $STREAMING_TEST_WORKER_EXEC $GCS_SERVER_EXEC $REDIS_SERVER_EXEC $REDIS_MODULE $REDIS_CLIENT_EXEC
sleep 1s
