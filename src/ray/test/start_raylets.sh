#!/usr/bin/env bash

# This needs to be run in the build tree, which is normally ray/python/ray/core

# Cause the script to exit if a single command fails.
set -e

# Start the GCS.
./src/common/thirdparty/redis/src/redis-server --loglevel warning --loadmodule ./src/common/redis_module/libray_redis_module.so --port 6379 >/dev/null &
sleep 1s

if [[ $1 ]]; then
  NUM_RAYLETS=$1
else
  NUM_RAYLETS=1
fi


for i in `seq 1 $NUM_RAYLETS`; do
  STORE_SOCKET_NAME="/tmp/store$i"
  RAYLET_SOCKET_NAME="/tmp/raylet$i"

  if [[ `stat $RAYLET_SOCKET_NAME` ]]; then
    rm $RAYLET_SOCKET_NAME
  fi
  if [[ `stat $STORE_SOCKET_NAME` ]]; then
    rm $STORE_SOCKET_NAME
  fi

  ./src/plasma/plasma_store -m 1000000000 -s $STORE_SOCKET_NAME &
  ./src/ray/raylet/raylet $RAYLET_SOCKET_NAME $STORE_SOCKET_NAME 127.0.0.1 127.0.0.1 6379 &

  echo
  echo "WORKER COMMAND: python ../../../src/ray/python/worker.py $RAYLET_SOCKET_NAME $STORE_SOCKET_NAME"
  echo
done
