#!/usr/bin/env bash

set -x

# Cause the script to exit if a single command fails.
set -e

./src/plasma/plasma_store_server -s /tmp/plasma_store_socket_1 -m 0 &
sleep 1
valgrind --track-origins=yes --leak-check=full --show-leak-kinds=all --leak-check-heuristics=stdstring --error-exitcode=1 ./src/plasma/manager_tests
killall plasma_store_server
