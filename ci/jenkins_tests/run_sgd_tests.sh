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


######################## SGD TESTS #################################

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} --memory-swap=-1 $DOCKER_SHA \
    python -m pytest /ray/python/ray/util/sgd/tests

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} --memory-swap=-1 $DOCKER_SHA \
    python /ray/python/ray/util/sgd/torch/examples/raysgd_torch_signatures.py

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} --memory-swap=-1 $DOCKER_SHA \
    python /ray/python/ray/util/sgd/torch/examples/train_example.py

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} --memory-swap=-1 $DOCKER_SHA \
    python /ray/python/ray/util/sgd/torch/examples/train_example.py --num-workers=2

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} --memory-swap=-1 $DOCKER_SHA \
    python /ray/python/ray/util/sgd/torch/examples/tune_example.py

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} --memory-swap=-1 $DOCKER_SHA \
    python /ray/python/ray/util/sgd/torch/examples/tune_example.py --num-workers=2

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} --memory-swap=-1 $DOCKER_SHA \
    python /ray/python/ray/util/sgd/torch/examples/cifar_pytorch_example.py --smoke-test

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} --memory-swap=-1 $DOCKER_SHA \
    python /ray/python/ray/util/sgd/torch/examples/cifar_pytorch_example.py --smoke-test --num-workers=2

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} --memory-swap=-1 $DOCKER_SHA \
    python /ray/python/ray/util/sgd/torch/examples/cifar_pytorch_pbt.py --smoke-test

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} --memory-swap=-1 $DOCKER_SHA \
    python /ray/python/ray/util/sgd/torch/examples/dcgan.py --smoke-test --num-workers=2

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} --memory-swap=-1 $DOCKER_SHA \
    python /ray/python/ray/util/sgd/torch/examples/benchmarks/benchmark.py --smoke-test

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} --memory-swap=-1 $DOCKER_SHA \
    python /ray/python/ray/util/sgd/tf/examples/tensorflow_train_example.py

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} --memory-swap=-1 $DOCKER_SHA \
    python /ray/python/ray/util/sgd/tf/examples/tensorflow_train_example.py --num-replicas=2

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} --memory-swap=-1 $DOCKER_SHA \
    python /ray/python/ray/util/sgd/tf/examples/tensorflow_train_example.py --tune

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} --memory-swap=-1 $DOCKER_SHA \
    python /ray/python/ray/util/sgd/tf/examples/cifar_tf_example.py --smoke-test

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} --memory-swap=-1 $DOCKER_SHA \
    python /ray/python/ray/util/sgd/tf/examples/cifar_tf_example.py --num-replicas 2 --smoke-test

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} --memory-swap=-1 $DOCKER_SHA \
    python /ray/python/ray/util/sgd/tf/examples/cifar_tf_example.py --num-replicas 2 --smoke-test --augment-data
