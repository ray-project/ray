#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

DOCKER_SHA=$($ROOT_DIR/../../build-docker.sh --output-sha --no-cache --skip-examples)
echo "Using Docker image" $DOCKER_SHA

python $ROOT_DIR/multi_node_docker_test.py \
    --docker-image=$DOCKER_SHA \
    --num-nodes=5 \
    --test-script=/ray/test/jenkins_tests/multi_node_tests/test_0.py

python $ROOT_DIR/multi_node_docker_test.py \
    --docker-image=$DOCKER_SHA \
    --num-nodes=1 \
    --shm-size=40G \
    --test-script=/ray/test/jenkins_tests/multi_node_tests/large_memory_test.py
