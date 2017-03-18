#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

python $ROOT_DIR/multi_node_docker_test.py --num-nodes=5 --test-script=/ray/test/jenkins_tests/multi_node_tests/test_0.py "$@"
