#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e

# Show explicitly which commands are currently running.
set -x

MEMORY_SIZE="20G"
SHM_SIZE="20G"

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

DOCKER_SHA=$($ROOT_DIR/../../build-docker.sh --output-sha --no-cache)
echo "Using Docker image" $DOCKER_SHA

######################## RLLIB TESTS #################################

source $ROOT_DIR/run_rllib_tests.sh

######################## TUNE TESTS #################################

bash $ROOT_DIR/run_tune_tests.sh ${MEMORY_SIZE} ${SHM_SIZE} $DOCKER_SHA

######################## SGD TESTS #################################

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    python /ray/python/ray/experimental/sgd/test_sgd.py --num-iters=2 \
        --batch-size=1 --strategy=simple

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    python /ray/python/ray/experimental/sgd/test_sgd.py --num-iters=2 \
        --batch-size=1 --strategy=ps

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    python /ray/python/ray/experimental/sgd/test_save_and_restore.py --num-iters=2 \
        --batch-size=1 --strategy=simple

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    python /ray/python/ray/experimental/sgd/test_save_and_restore.py --num-iters=2 \
        --batch-size=1 --strategy=ps

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    python /ray/python/ray/experimental/sgd/mnist_example.py --num-iters=1 \
        --num-workers=1 --devices-per-worker=1 --strategy=ps

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    python /ray/python/ray/experimental/sgd/mnist_example.py --num-iters=1 \
        --num-workers=1 --devices-per-worker=1 --strategy=ps --tune

######################## RAY BACKEND TESTS #################################

python3 $ROOT_DIR/multi_node_docker_test.py \
    --docker-image=$DOCKER_SHA \
    --num-nodes=5 \
    --num-redis-shards=10 \
    --test-script=/ray/ci/jenkins_tests/multi_node_tests/test_0.py

python3 $ROOT_DIR/multi_node_docker_test.py \
    --docker-image=$DOCKER_SHA \
    --num-nodes=5 \
    --num-redis-shards=5 \
    --num-gpus=0,1,2,3,4 \
    --num-drivers=7 \
    --driver-locations=0,1,0,1,2,3,4 \
    --test-script=/ray/ci/jenkins_tests/multi_node_tests/remove_driver_test.py

python3 $ROOT_DIR/multi_node_docker_test.py \
    --docker-image=$DOCKER_SHA \
    --num-nodes=5 \
    --num-redis-shards=2 \
    --num-gpus=0,0,5,6,50 \
    --num-drivers=100 \
    --test-script=/ray/ci/jenkins_tests/multi_node_tests/many_drivers_test.py

python3 $ROOT_DIR/multi_node_docker_test.py \
    --docker-image=$DOCKER_SHA \
    --num-nodes=1 \
    --mem-size=60G \
    --shm-size=60G \
    --test-script=/ray/ci/jenkins_tests/multi_node_tests/large_memory_test.py
