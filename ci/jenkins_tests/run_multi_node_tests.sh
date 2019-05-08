#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e

# Show explicitly which commands are currently running.
set -x

MEMORY_SIZE="20G"
SHM_SIZE="20G"

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

DOCKER_SHA=$($ROOT_DIR/../../build-docker.sh --output-sha --no-cache)
SUPPRESS_OUTPUT=$ROOT_DIR/../suppress_output
echo "Using Docker image" $DOCKER_SHA

######################## RLLIB TESTS #################################

source $ROOT_DIR/run_rllib_tests.sh

######################## TUNE TESTS #################################

bash $ROOT_DIR/run_tune_tests.sh ${MEMORY_SIZE} ${SHM_SIZE} $DOCKER_SHA

######################## RAY BACKEND TESTS #################################

$SUPPRESS_OUTPUT docker run --rm --shm-size=60G --memory=60G $DOCKER_SHA \
    python /ray/ci/jenkins_tests/miscellaneous/large_memory_test.py

######################## SGD TESTS #################################

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    python /ray/python/ray/experimental/sgd/test_sgd.py --num-iters=2 \
        --batch-size=1 --strategy=simple

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    python /ray/python/ray/experimental/sgd/test_sgd.py --num-iters=2 \
        --batch-size=1 --strategy=ps

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    python /ray/python/ray/experimental/sgd/test_save_and_restore.py --num-iters=2 \
        --batch-size=1 --strategy=simple

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    python /ray/python/ray/experimental/sgd/test_save_and_restore.py --num-iters=2 \
        --batch-size=1 --strategy=ps

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    python /ray/python/ray/experimental/sgd/mnist_example.py --num-iters=1 \
        --num-workers=1 --devices-per-worker=1 --strategy=ps

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    python /ray/python/ray/experimental/sgd/mnist_example.py --num-iters=1 \
        --num-workers=1 --devices-per-worker=1 --strategy=ps --tune
