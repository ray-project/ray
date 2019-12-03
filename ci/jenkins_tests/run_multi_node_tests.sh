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
    python -m pytest /ray/python/ray/experimental/sgd/tests

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    python /ray/python/ray/experimental/sgd/examples/train_example.py

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    python /ray/python/ray/experimental/sgd/examples/train_example.py --num-replicas=2

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    python /ray/python/ray/experimental/sgd/examples/tune_example.py

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    python /ray/python/ray/experimental/sgd/examples/tune_example.py --num-replicas=2

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    python /ray/doc/examples/doc_code/torch_example.py

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    python /ray/doc/examples/doc_code/tf_example.py

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    python /ray/python/ray/experimental/sgd/examples/tensorflow_train_example.py

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    python /ray/python/ray/experimental/sgd/examples/tensorflow_train_example.py --num-replicas=2

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    python /ray/python/ray/experimental/sgd/examples/tensorflow_train_example.py --tune

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    python /ray/python/ray/experimental/sgd/examples/cifar_tf_example.py --smoke-test

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    python /ray/python/ray/experimental/sgd/examples/cifar_tf_example.py --num-replicas 2 --smoke-test

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    python /ray/python/ray/experimental/sgd/examples/cifar_tf_example.py --num-replicas 2 --smoke-test --augment-data
