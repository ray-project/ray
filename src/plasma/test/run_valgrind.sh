#!/usr/bin/env bash

set -x

# Cause the script to exit if a single command fails.
set -e

./src/plasma/plasma_store -s /tmp/plasma_store_socket_1 -m 0 &
sleep 1
valgrind --leak-check=full --error-exitcode=1 ./src/plasma/manager_tests
killall plasma_store
