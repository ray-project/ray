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
    git archive -o ./docker/tune_test/ray.tar "$(git rev-parse HEAD)"
    if [ "$CI_BUILD_FROM_SOURCE" == "1" ]; then
      DOCKER_SHA=$(docker build --no-cache -q -t ray-project/tune_test docker/tune_test -f docker/tune_test/build_from_source.Dockerfile)
    else
      DOCKER_SHA=$(docker build --no-cache -q -t ray-project/tune_test docker/tune_test)
    fi
fi

echo "Using Docker image" "$DOCKER_SHA"

######################## TUNE TESTS #################################

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} --memory-swap=-1 "$DOCKER_SHA" \
    pytest /ray/python/ray/tune/tests/test_actor_reuse.py

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} --memory-swap=-1 "$DOCKER_SHA" \
    pytest /ray/python/ray/tune/tests/test_tune_restore.py

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} --memory-swap=-1 "$DOCKER_SHA" \
    python /ray/python/ray/tune/tests/example.py

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} --memory-swap=-1 "$DOCKER_SHA" \
    bash -c 'pip install -U tensorflow && python /ray/python/ray/tune/tests/test_logger.py'

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} --memory-swap=-1 "$DOCKER_SHA" \
    bash -c 'pip install -U tensorflow==1.15 && python /ray/python/ray/tune/tests/test_logger.py'

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} --memory-swap=-1 "$DOCKER_SHA" \
    bash -c 'pip install -U tensorflow==1.14 && python /ray/python/ray/tune/tests/test_logger.py'

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} --memory-swap=-1 -e MPLBACKEND=Agg "$DOCKER_SHA" \
    python /ray/python/ray/tune/tests/tutorial.py

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} --memory-swap=-1 "$DOCKER_SHA" \
    python /ray/python/ray/tune/examples/pbt_example.py \
    --smoke-test

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} --memory-swap=-1 "$DOCKER_SHA" \
    python /ray/python/ray/tune/examples/hyperband_example.py

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} --memory-swap=-1 "$DOCKER_SHA" \
    python /ray/python/ray/tune/examples/async_hyperband_example.py \
    --smoke-test

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} --memory-swap=-1 "$DOCKER_SHA" \
    python /ray/python/ray/tune/examples/tf_mnist_example.py --smoke-test

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} --memory-swap=-1 "$DOCKER_SHA" \
    python /ray/python/ray/tune/examples/lightgbm_example.py

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} --memory-swap=-1 "$DOCKER_SHA" \
    python /ray/python/ray/tune/examples/xgboost_example.py

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} --memory-swap=-1 "$DOCKER_SHA" \
    python /ray/python/ray/tune/examples/logging_example.py \
    --smoke-test

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} --memory-swap=-1 "$DOCKER_SHA" \
    python /ray/python/ray/tune/examples/mlflow_example.py

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} --memory-swap=-1 "$DOCKER_SHA" \
    python /ray/python/ray/tune/examples/bayesopt_example.py \
    --smoke-test

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} --memory-swap=-1 "$DOCKER_SHA" \
    python /ray/python/ray/tune/examples/cifar10_pytorch.py --smoke-test

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} --memory-swap=-1 "$DOCKER_SHA" \
    python /ray/python/ray/tune/examples/hyperopt_example.py \
    --smoke-test

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} --memory-swap=-1 "$DOCKER_SHA" \
    python /ray/doc/source/tune/_tutorials/tune-sklearn.py

# if [ -n "$SIGOPT_KEY" ]; then
#     $SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} --memory-swap=-1 -e SIGOPT_KEY "$DOCKER_SHA" \
#         python /ray/python/ray/tune/examples/sigopt_example.py \
#         --smoke-test
# fi

# Runs only on Python3
$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} --memory-swap=-1 "$DOCKER_SHA" \
    python /ray/python/ray/tune/examples/nevergrad_example.py \
    --smoke-test

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} --memory-swap=-1 "$DOCKER_SHA" \
    python /ray/python/ray/tune/examples/tune_mnist_keras.py \
    --smoke-test

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} --memory-swap=-1 "$DOCKER_SHA" \
    python /ray/python/ray/tune/examples/mnist_pytorch.py --smoke-test

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} --memory-swap=-1 "$DOCKER_SHA" \
    python /ray/python/ray/tune/examples/mnist_pytorch_lightning.py --smoke-test

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} --memory-swap=-1 "$DOCKER_SHA" \
    python /ray/python/ray/tune/examples/mnist_pytorch_trainable.py \
    --smoke-test

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} --memory-swap=-1 "$DOCKER_SHA" \
    python /ray/python/ray/tune/examples/genetic_example.py \
    --smoke-test

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} --memory-swap=-1 "$DOCKER_SHA" \
    python /ray/python/ray/tune/examples/skopt_example.py \
    --smoke-test

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} --memory-swap=-1 "$DOCKER_SHA" \
    python /ray/python/ray/tune/examples/dragonfly_example.py \
    --smoke-test

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} --memory-swap=-1 "$DOCKER_SHA" \
    python /ray/python/ray/tune/examples/zoopt_example.py \
    --smoke-test

# Commenting out because flaky
# $SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} --memory-swap=-1 "$DOCKER_SHA" \
#     python /ray/python/ray/tune/examples/pbt_memnn_example.py \
#     --smoke-test

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} --memory-swap=-1 "$DOCKER_SHA" \
    python /ray/python/ray/tune/examples/pbt_convnet_example.py \
    --smoke-test

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} --memory-swap=-1 "$DOCKER_SHA" \
    python /ray/python/ray/tune/examples/hyperband_function_example.py \
    --smoke-test

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} --memory-swap=-1 "$DOCKER_SHA" \
    python /ray/python/ray/tune/examples/pbt_function.py \
    --smoke-test

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} --memory-swap=-1 "$DOCKER_SHA" \
    python /ray/python/ray/tune/examples/pbt_dcgan_mnist/pbt_dcgan_mnist.py \
    --smoke-test

$SUPPRESS_OUTPUT docker run --rm --shm-size${SHM_SIZE} --memory=${MEMORY_SIZE} --memory-swap=-1 "$DOCKER_SHA" \
    python /ray/python/ray/tune/examples/pbt_transformers/pbt_transformers.py \
    --smoke-test

$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} --memory-swap=-1 "$DOCKER_SHA" \
    python /ray/ci/long_running_distributed_tests/workloads/pytorch_pbt_failure.py \
    --smoke-test

# uncomment once statsmodels is updated.
$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} --memory-swap=-1 "$DOCKER_SHA" \
    python /ray/python/ray/tune/examples/bohb_example.py

# Moved to bottom because flaky
$SUPPRESS_OUTPUT docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} --memory-swap=-1 "$DOCKER_SHA" \
    pytest /ray/python/ray/tune/tests/test_cluster.py
