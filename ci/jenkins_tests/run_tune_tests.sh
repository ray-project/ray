#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e

# Show explicitly which commands are currently running.
set -x

MEMORY_SIZE=$1
SHM_SIZE=$2
DOCKER_SHA=$3

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)
SUPPRESS_OUTPUT=$ROOT_DIR/../suppress_output

if [ "$MEMORY_SIZE" == "" ]; then
    MEMORY_SIZE="20G"
fi
if [ "$SHM_SIZE" == "" ]; then
    SHM_SIZE="20G"
fi
if [ "$DOCKER_SHA" == "" ]; then
    echo "Building application docker."
    docker build -q --no-cache -t ray-project/base-deps docker/base-deps

    # Add Ray source
    git rev-parse HEAD > ./docker/tune_test/git-rev
    git archive -o ./docker/tune_test/ray.tar $(git rev-parse HEAD)
    DOCKER_SHA=$(docker build --no-cache -q -t ray-project/tune_test docker/tune_test)
fi

echo "Using Docker image" $DOCKER_SHA

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    pytest /ray/python/ray/tune/tests/test_cluster.py

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    pytest /ray/python/ray/tune/tests/test_actor_reuse.py

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    pytest /ray/python/ray/tune/tests/test_tune_restore.py

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    python /ray/python/ray/tune/tests/example.py

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    python /ray/python/ray/tune/tests/tutorial.py

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    python /ray/python/ray/tune/examples/tune_mnist_ray.py \
    --smoke-test

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    python /ray/python/ray/tune/examples/pbt_example.py \
    --smoke-test

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    python /ray/python/ray/tune/examples/hyperband_example.py \
    --smoke-test

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    python /ray/python/ray/tune/examples/async_hyperband_example.py \
    --smoke-test

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    python /ray/python/ray/tune/examples/tune_mnist_ray_hyperband.py \
    --smoke-test

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    python /ray/python/ray/tune/examples/tune_mnist_async_hyperband.py \
    --smoke-test

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    python /ray/python/ray/tune/examples/logging_example.py \
    --smoke-test

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    python /ray/python/ray/tune/examples/bayesopt_example.py \
    --smoke-test

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    python /ray/python/ray/tune/examples/hyperopt_example.py \
    --smoke-test

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} -e SIGOPT_KEY $DOCKER_SHA \
    python /ray/python/ray/tune/examples/sigopt_example.py \
    --smoke-test

# Runs only on Python3
$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    python /ray/python/ray/tune/examples/nevergrad_example.py \
    --smoke-test

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    python /ray/python/ray/tune/examples/tune_mnist_keras.py \
    --smoke-test

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    python /ray/python/ray/tune/examples/mnist_pytorch.py --smoke-test

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    python /ray/python/ray/tune/examples/mnist_pytorch_trainable.py \
    --smoke-test

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    python /ray/python/ray/tune/examples/genetic_example.py \
    --smoke-test

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    python /ray/python/ray/tune/examples/skopt_example.py \
    --smoke-test
