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

# docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
#     python /ray/examples/lbfgs/driver.py

# docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
#     python /ray/examples/rl_pong/driver.py \
#     --iterations=3

# docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
#     python /ray/examples/hyperopt/hyperopt_simple.py

# docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
#     python /ray/examples/hyperopt/hyperopt_adaptive.py

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env PongDeterministic-v0 \
    --run A3C \
    --stop '{"training_iteration": 2}' \
    --config '{"num_workers": 16}'

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env CartPole-v1 \
    --run PPO \
    --stop '{"training_iteration": 2}' \
    --config '{"kl_coeff": 1.0, "num_sgd_iter": 10, "sgd_stepsize": 1e-4, "sgd_batchsize": 64, "timesteps_per_batch": 2000, "num_workers": 1, "model": {"free_log_std": true}}'

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env CartPole-v1 \
    --run PPO \
    --stop '{"training_iteration": 2}' \
    --config '{"kl_coeff": 1.0, "num_sgd_iter": 10, "sgd_stepsize": 1e-4, "sgd_batchsize": 64, "timesteps_per_batch": 2000, "num_workers": 1, "use_gae": false}'

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env Pendulum-v0 \
    --run ES \
    --stop '{"training_iteration": 2}' \
    --config '{"stepsize": 0.01, "episodes_per_batch": 20, "timesteps_per_batch": 100}'

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env Pong-v0 \
    --run ES \
    --stop '{"training_iteration": 2}' \
    --config '{"stepsize": 0.01, "episodes_per_batch": 20, "timesteps_per_batch": 100}'

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env CartPole-v0 \
    --run A3C \
    --stop '{"training_iteration": 2}' \
    --config '{"use_lstm": false}'

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env CartPole-v0 \
    --run DQN \
    --stop '{"training_iteration": 2}' \
    --config '{"lr": 1e-3, "schedule_max_timesteps": 100000, "exploration_fraction": 0.1, "exploration_final_eps": 0.02, "dueling": false, "hiddens": [], "model": {"fcnet_hiddens": [64], "fcnet_activation": "relu"}}'

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env CartPole-v0 \
    --run DQN \
    --stop '{"training_iteration": 2}' \
    --config '{"async_updates": true, "num_workers": 2}'

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env CartPole-v0 \
    --run DQN \
    --stop '{"training_iteration": 2}' \
    --config '{"multi_gpu": true, "optimizer": {"sgd_batch_size": 4}}'

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env FrozenLake-v0 \
    --run DQN \
    --stop '{"training_iteration": 2}'

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env FrozenLake-v0 \
    --run PPO \
    --stop '{"training_iteration": 2}' \
    --config '{"num_sgd_iter": 10, "sgd_batchsize": 64, "timesteps_per_batch": 1000, "num_workers": 1}'

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env PongDeterministic-v4 \
    --run DQN \
    --stop '{"training_iteration": 2}' \
    --config '{"lr": 1e-4, "schedule_max_timesteps": 2000000, "buffer_size": 10000, "exploration_fraction": 0.1, "exploration_final_eps": 0.01, "sample_batch_size": 4, "learning_starts": 10000, "target_network_update_freq": 1000, "gamma": 0.99, "prioritized_replay": true}'

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env MontezumaRevenge-v0 \
    --run PPO \
    --stop '{"training_iteration": 2}' \
    --config '{"kl_coeff": 1.0, "num_sgd_iter": 10, "sgd_stepsize": 1e-4, "sgd_batchsize": 64, "timesteps_per_batch": 2000, "num_workers": 1, "model": {"dim": 40, "conv_filters": [[16, [8, 8], 4], [32, [4, 4], 2], [512, [5, 5], 1]]}, "extra_frameskip": 4}'

# docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
#     python /ray/python/ray/rllib/train.py \
#     --env PongDeterministic-v4 \
#     --run A3C \
#     --stop '{"training_iteration": 2}' \
#     --config '{"num_workers": 2, "use_lstm": false, "use_pytorch": true, "model": {"grayscale": true, "zero_mean": false, "dim": 80, "channel_major": true}}'

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    sh /ray/test/jenkins_tests/multi_node_tests/test_rllib_eval.sh

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/test/test_checkpoint_restore.py

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/test/test_supported_spaces.py

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/tune/examples/tune_mnist_ray.py \
    --smoke-test

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/tune/examples/pbt_example.py \
    --smoke-test

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/tune/examples/hyperband_example.py \
    --smoke-test

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/examples/multiagent_mountaincar.py

docker run --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/examples/multiagent_pendulum.py
