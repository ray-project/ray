#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e

# Show explicitly which commands are currently running.
set -x

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

DOCKER_SHA=$($ROOT_DIR/../../build-docker.sh --output-sha --no-cache)
echo "Using Docker image" $DOCKER_SHA

python $ROOT_DIR/multi_node_docker_test.py \
    --docker-image=$DOCKER_SHA \
    --num-nodes=5 \
    --num-redis-shards=10 \
    --test-script=/ray/test/jenkins_tests/multi_node_tests/test_0.py

python $ROOT_DIR/multi_node_docker_test.py \
    --docker-image=$DOCKER_SHA \
    --num-nodes=5 \
    --num-redis-shards=5 \
    --num-gpus=0,1,2,3,4 \
    --num-drivers=7 \
    --driver-locations=0,1,0,1,2,3,4 \
    --test-script=/ray/test/jenkins_tests/multi_node_tests/remove_driver_test.py

python $ROOT_DIR/multi_node_docker_test.py \
    --docker-image=$DOCKER_SHA \
    --num-nodes=5 \
    --num-redis-shards=2 \
    --num-gpus=0,0,5,6,50 \
    --num-drivers=100 \
    --test-script=/ray/test/jenkins_tests/multi_node_tests/many_drivers_test.py

python $ROOT_DIR/multi_node_docker_test.py \
    --docker-image=$DOCKER_SHA \
    --num-nodes=1 \
    --mem-size=60G \
    --shm-size=60G \
    --test-script=/ray/test/jenkins_tests/multi_node_tests/large_memory_test.py

# Test that the example applications run.

# docker run --shm-size=10G --memory=10G $DOCKER_SHA \
#     python /ray/examples/lbfgs/driver.py

# docker run --shm-size=10G --memory=10G $DOCKER_SHA \
#     python /ray/examples/rl_pong/driver.py \
#     --iterations=3

# docker run --shm-size=10G --memory=10G $DOCKER_SHA \
#     python /ray/examples/hyperopt/hyperopt_simple.py

# docker run --shm-size=10G --memory=10G $DOCKER_SHA \
#     python /ray/examples/hyperopt/hyperopt_adaptive.py

docker run --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/a3c/example.py \
    --environment=PongDeterministic-v0 \
    --iterations=2

# docker run --shm-size=10G --memory=10G $DOCKER_SHA \
#     python /ray/python/ray/rllib/policy_gradient/example.py \
#     --iterations=2

docker run --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/evolution_strategies/example.py \
    --env-name=Pendulum-v0 \
    --iterations=2

docker run --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/dqn/example-cartpole.py \
    --iterations=2
