#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e

# Show explicitly which commands are currently running.
set -x

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

DOCKER_SHA=$($ROOT_DIR/../../build-docker.sh --output-sha --no-cache)
echo "Using Docker image" $DOCKER_SHA

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/tune/examples/mnist_pytorch.py \
    --smoke-test

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/tune/examples/mnist_pytorch_trainable.py \
    --smoke-test

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env PongDeterministic-v4 \
    --run A3C \
    --stop '{"training_iteration": 2}' \
    --config '{"num_workers": 2, "use_pytorch": true, "model": {"use_lstm": false, "grayscale": true, "zero_mean": false, "dim": 84, "channel_major": true}, "preprocessor_pref": "rllib"}'

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env CartPole-v1 \
    --run A3C \
    --stop '{"training_iteration": 2}' \
    --config '{"num_workers": 2, "use_pytorch": true}'

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/tune/examples/mnist_pytorch.py \
    --smoke-test

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/tune/examples/mnist_pytorch_trainable.py \
    --smoke-test

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env PongDeterministic-v4 \
    --run A3C \
    --stop '{"training_iteration": 2}' \
    --config '{"num_workers": 2, "use_pytorch": true, "model": {"use_lstm": false, "grayscale": true, "zero_mean": false, "dim": 84, "channel_major": true}, "preprocessor_pref": "rllib"}'

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env CartPole-v1 \
    --run A3C \
    --stop '{"training_iteration": 2}' \
    --config '{"num_workers": 2, "use_pytorch": true}'

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/tune/examples/mnist_pytorch.py \
    --smoke-test

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/tune/examples/mnist_pytorch_trainable.py \
    --smoke-test

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env PongDeterministic-v4 \
    --run A3C \
    --stop '{"training_iteration": 2}' \
    --config '{"num_workers": 2, "use_pytorch": true, "model": {"use_lstm": false, "grayscale": true, "zero_mean": false, "dim": 84, "channel_major": true}, "preprocessor_pref": "rllib"}'

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env CartPole-v1 \
    --run A3C \
    --stop '{"training_iteration": 2}' \
    --config '{"num_workers": 2, "use_pytorch": true}'

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/tune/examples/mnist_pytorch.py \
    --smoke-test

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/tune/examples/mnist_pytorch_trainable.py \
    --smoke-test

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env PongDeterministic-v4 \
    --run A3C \
    --stop '{"training_iteration": 2}' \
    --config '{"num_workers": 2, "use_pytorch": true, "model": {"use_lstm": false, "grayscale": true, "zero_mean": false, "dim": 84, "channel_major": true}, "preprocessor_pref": "rllib"}'

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env CartPole-v1 \
    --run A3C \
    --stop '{"training_iteration": 2}' \
    --config '{"num_workers": 2, "use_pytorch": true}'

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/tune/examples/mnist_pytorch.py \
    --smoke-test

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/tune/examples/mnist_pytorch_trainable.py \
    --smoke-test

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env PongDeterministic-v4 \
    --run A3C \
    --stop '{"training_iteration": 2}' \
    --config '{"num_workers": 2, "use_pytorch": true, "model": {"use_lstm": false, "grayscale": true, "zero_mean": false, "dim": 84, "channel_major": true}, "preprocessor_pref": "rllib"}'

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env CartPole-v1 \
    --run A3C \
    --stop '{"training_iteration": 2}' \
    --config '{"num_workers": 2, "use_pytorch": true}'

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/tune/examples/mnist_pytorch.py \
    --smoke-test

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/tune/examples/mnist_pytorch_trainable.py \
    --smoke-test

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env PongDeterministic-v4 \
    --run A3C \
    --stop '{"training_iteration": 2}' \
    --config '{"num_workers": 2, "use_pytorch": true, "model": {"use_lstm": false, "grayscale": true, "zero_mean": false, "dim": 84, "channel_major": true}, "preprocessor_pref": "rllib"}'

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env CartPole-v1 \
    --run A3C \
    --stop '{"training_iteration": 2}' \
    --config '{"num_workers": 2, "use_pytorch": true}'

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/tune/examples/mnist_pytorch.py \
    --smoke-test

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/tune/examples/mnist_pytorch_trainable.py \
    --smoke-test

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env PongDeterministic-v4 \
    --run A3C \
    --stop '{"training_iteration": 2}' \
    --config '{"num_workers": 2, "use_pytorch": true, "model": {"use_lstm": false, "grayscale": true, "zero_mean": false, "dim": 84, "channel_major": true}, "preprocessor_pref": "rllib"}'

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env CartPole-v1 \
    --run A3C \
    --stop '{"training_iteration": 2}' \
    --config '{"num_workers": 2, "use_pytorch": true}'

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/tune/examples/mnist_pytorch.py \
    --smoke-test

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/tune/examples/mnist_pytorch_trainable.py \
    --smoke-test

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env PongDeterministic-v4 \
    --run A3C \
    --stop '{"training_iteration": 2}' \
    --config '{"num_workers": 2, "use_pytorch": true, "model": {"use_lstm": false, "grayscale": true, "zero_mean": false, "dim": 84, "channel_major": true}, "preprocessor_pref": "rllib"}'

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env CartPole-v1 \
    --run A3C \
    --stop '{"training_iteration": 2}' \
    --config '{"num_workers": 2, "use_pytorch": true}'

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/tune/examples/mnist_pytorch.py \
    --smoke-test

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/tune/examples/mnist_pytorch_trainable.py \
    --smoke-test

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env PongDeterministic-v4 \
    --run A3C \
    --stop '{"training_iteration": 2}' \
    --config '{"num_workers": 2, "use_pytorch": true, "model": {"use_lstm": false, "grayscale": true, "zero_mean": false, "dim": 84, "channel_major": true}, "preprocessor_pref": "rllib"}'

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env CartPole-v1 \
    --run A3C \
    --stop '{"training_iteration": 2}' \
    --config '{"num_workers": 2, "use_pytorch": true}'

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/tune/examples/mnist_pytorch.py \
    --smoke-test

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/tune/examples/mnist_pytorch_trainable.py \
    --smoke-test

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env PongDeterministic-v4 \
    --run A3C \
    --stop '{"training_iteration": 2}' \
    --config '{"num_workers": 2, "use_pytorch": true, "model": {"use_lstm": false, "grayscale": true, "zero_mean": false, "dim": 84, "channel_major": true}, "preprocessor_pref": "rllib"}'

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env CartPole-v1 \
    --run A3C \
    --stop '{"training_iteration": 2}' \
    --config '{"num_workers": 2, "use_pytorch": true}'
