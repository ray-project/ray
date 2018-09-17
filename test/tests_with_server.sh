#!/bin/bash

RAY_DIR="${TEST_SRCDIR}"
REDIS="${RAY_DIR}/redis_server"
REDIS_CLI="${TEST_SRCDIR}/redis_cli"
PLASMA_STORE="${TEST_SRCDIR}/plasma_store"
PLASMA_MANAGER="${RAY_DIR}/plasma_manager"
PLASMA_PIDS=()

function cleanup_and_exit() {
  local code=$1
  $REDIS_CLI -p 6379 shutdown
  $REDIS_CLI -p 6380 shutdown
  if [ "${#PLASMA_PIDS[@]}" -ne 0 ]
  then
    for pid in "${PLASMA_PIDS[@]}"
    do
      echo "Shutting down Plasma PID $pid"
      kill $pid
    done
  fi
  exit $code
}

$REDIS --port 6379 &
$REDIS --port 6380 &

sleep 3

function set_redis_state() {
  $REDIS_CLI set NumRedisShards 1
  $REDIS_CLI ltrim RedisShards 1 0  # clear the list
  $REDIS_CLI rpush RedisShards 127.0.0.1:6380
}

function launch_plasma_store() {
  local socket=$1
  local m=$2
  $PLASMA_STORE -s $1 -m $2 &
  PLASMA_PIDS+=($!)
  sleep 1
}

TESTS="
    db_tests
    io_tests
    task_tests
    redis_tests
    task_table_tests
    object_table_tests
"

for test in $TESTS; do
    echo "Testing $test"
    set_redis_state
    "${RAY_DIR}/$test" || cleanup_and_exit 1
done

"${RAY_DIR}/asio_test" || cleanup_and_exit 1
"${RAY_DIR}/gcs_client_test" || cleanup_and_exit 1
"${RAY_DIR}/object_manager_test" $PLASMA_STORE || cleanup_and_exit 1
"${RAY_DIR}/object_manager_stress_test" $PLASMA_STORE || cleanup_and_exit 1

# plasma_manager_tests
# ==============================================================================
launch_plasma_store /tmp/plasma_store_socket_1 0
"${RAY_DIR}/plasma_manager_tests" || cleanup_and_exit 1
# ==============================================================================

# plasma_client_tests
# ==============================================================================
$REDIS_CLI flushall
set_redis_state

launch_plasma_store /tmp/store1 1000000000
$PLASMA_MANAGER -m /tmp/manager1 -s /tmp/store1 -h 127.0.0.1 -p 11111 -r 127.0.0.1:6379 &
PLASMA_PIDS+=($!)

launch_plasma_store /tmp/store2 1000000000
$PLASMA_MANAGER -m /tmp/manager2 -s /tmp/store2 -h 127.0.0.1 -p 22222 -r 127.0.0.1:6379 &
PLASMA_PIDS+=($!)

sleep 3

"${RAY_DIR}/plasma_client_tests" || cleanup_and_exit 1
# ==============================================================================

cleanup_and_exit 0
