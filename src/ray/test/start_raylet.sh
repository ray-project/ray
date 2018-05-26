#!/usr/bin/env bash

# This needs to be run in the build tree, which is normally ray/python/ray/core

# Cause the script to exit if a single command fails.
set -e

if [[ $1 ]]; then
  RAYLET_NUM=$1
else
  RAYLET_NUM=1
fi

STORE_SOCKET_NAME="/tmp/store$RAYLET_NUM"
RAYLET_SOCKET_NAME="/tmp/raylet$RAYLET_NUM"

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
