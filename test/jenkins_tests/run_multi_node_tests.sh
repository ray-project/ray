#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e

# Show explicitly which commands are currently running.
set -x

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

DOCKER_SHA=$($ROOT_DIR/../../build-docker.sh --output-sha --no-cache)
echo "Using Docker image" $DOCKER_SHA

docker run  -e "RAY_USE_XRAY=1" --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env PongDeterministic-v0 \
    --run A3C \
    --stop '{"training_iteration": 2}' \
    --config '{"num_workers": 16}'

docker run  -e "RAY_USE_XRAY=1" --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env CartPole-v1 \
    --run PPO \
    --stop '{"training_iteration": 2}' \
    --config '{"kl_coeff": 1.0, "num_sgd_iter": 10, "sgd_stepsize": 1e-4, "sgd_batchsize": 64, "timesteps_per_batch": 2000, "num_workers": 1, "model": {"free_log_std": true}}'

docker run  -e "RAY_USE_XRAY=1" --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env CartPole-v1 \
    --run PPO \
    --stop '{"training_iteration": 2}' \
    --config '{"simple_optimizer": false, "num_sgd_iter": 2, "model": {"use_lstm": true}}'

docker run  -e "RAY_USE_XRAY=1" --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env CartPole-v1 \
    --run PPO \
    --stop '{"training_iteration": 2}' \
    --config '{"simple_optimizer": true, "num_sgd_iter": 2, "model": {"use_lstm": true}}'

docker run  -e "RAY_USE_XRAY=1" --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env CartPole-v1 \
    --run PPO \
    --stop '{"training_iteration": 2}' \
    --config '{"kl_coeff": 1.0, "num_sgd_iter": 10, "sgd_stepsize": 1e-4, "sgd_batchsize": 64, "timesteps_per_batch": 2000, "num_workers": 1, "use_gae": false}'

docker run  -e "RAY_USE_XRAY=1" --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env Pendulum-v0 \
    --run ES \
    --stop '{"training_iteration": 2}' \
    --config '{"stepsize": 0.01, "episodes_per_batch": 20, "timesteps_per_batch": 100}'

docker run  -e "RAY_USE_XRAY=1" --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env Pong-v0 \
    --run ES \
    --stop '{"training_iteration": 2}' \
    --config '{"stepsize": 0.01, "episodes_per_batch": 20, "timesteps_per_batch": 100}'

docker run  -e "RAY_USE_XRAY=1" --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env CartPole-v0 \
    --run A3C \
    --stop '{"training_iteration": 2}' \

docker run  -e "RAY_USE_XRAY=1" --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env CartPole-v0 \
    --run DQN \
    --stop '{"training_iteration": 2}' \
    --config '{"lr": 1e-3, "schedule_max_timesteps": 100000, "exploration_fraction": 0.1, "exploration_final_eps": 0.02, "dueling": false, "hiddens": [], "model": {"fcnet_hiddens": [64], "fcnet_activation": "relu"}}'

docker run  -e "RAY_USE_XRAY=1" --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env CartPole-v0 \
    --run DQN \
    --stop '{"training_iteration": 2}' \
    --config '{"num_workers": 2}'

docker run  -e "RAY_USE_XRAY=1" --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env CartPole-v0 \
    --run APEX \
    --stop '{"training_iteration": 2}' \
    --config '{"num_workers": 2, "timesteps_per_iteration": 1000, "gpu": false, "min_iter_time_s": 1}'

docker run  -e "RAY_USE_XRAY=1" --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env FrozenLake-v0 \
    --run DQN \
    --stop '{"training_iteration": 2}'

docker run  -e "RAY_USE_XRAY=1" --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env FrozenLake-v0 \
    --run PPO \
    --stop '{"training_iteration": 2}' \
    --config '{"num_sgd_iter": 10, "sgd_batchsize": 64, "timesteps_per_batch": 1000, "num_workers": 1}'

docker run  -e "RAY_USE_XRAY=1" --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env PongDeterministic-v4 \
    --run DQN \
    --stop '{"training_iteration": 2}' \
    --config '{"lr": 1e-4, "schedule_max_timesteps": 2000000, "buffer_size": 10000, "exploration_fraction": 0.1, "exploration_final_eps": 0.01, "sample_batch_size": 4, "learning_starts": 10000, "target_network_update_freq": 1000, "gamma": 0.99, "prioritized_replay": true}'

docker run  -e "RAY_USE_XRAY=1" --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env MontezumaRevenge-v0 \
    --run PPO \
    --stop '{"training_iteration": 2}' \
    --config '{"kl_coeff": 1.0, "num_sgd_iter": 10, "sgd_stepsize": 1e-4, "sgd_batchsize": 64, "timesteps_per_batch": 2000, "num_workers": 1, "model": {"dim": 40, "conv_filters": [[16, [8, 8], 4], [32, [4, 4], 2], [512, [5, 5], 1]]}}'

docker run  -e "RAY_USE_XRAY=1" --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env PongDeterministic-v4 \
    --run A3C \
    --stop '{"training_iteration": 2}' \
    --config '{"num_workers": 2, "use_pytorch": true, "model": {"use_lstm": false, "grayscale": true, "zero_mean": false, "dim": 80, "channel_major": true}}'

docker run  -e "RAY_USE_XRAY=1" --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env CartPole-v1 \
    --run A3C \
    --stop '{"training_iteration": 2}' \
    --config '{"num_workers": 2, "use_pytorch": true}'

docker run  -e "RAY_USE_XRAY=1" --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env CartPole-v1 \
    --run A3C \
    --stop '{"training_iteration": 2}' \
    --config '{"num_workers": 2, "model": {"use_lstm": true}}'

docker run  -e "RAY_USE_XRAY=1" --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env CartPole-v0 \
    --run DQN \
    --stop '{"training_iteration": 2}' \
    --config '{"num_workers": 2}'

docker run  -e "RAY_USE_XRAY=1" --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env CartPole-v0 \
    --run PG \
    --stop '{"training_iteration": 2}' \
    --config '{"sample_batch_size": 500, "num_workers": 1}'

docker run  -e "RAY_USE_XRAY=1" --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env CartPole-v0 \
    --run PG \
    --stop '{"training_iteration": 2}' \
    --config '{"sample_batch_size": 500, "num_workers": 1, "model": {"use_lstm": true, "max_seq_len": 100}}'

docker run  -e "RAY_USE_XRAY=1" --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env CartPole-v0 \
    --run PG \
    --stop '{"training_iteration": 2}' \
    --config '{"sample_batch_size": 500, "num_workers": 1, "num_envs_per_worker": 10}'

docker run  -e "RAY_USE_XRAY=1" --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env Pong-v0 \
    --run PG \
    --stop '{"training_iteration": 2}' \
    --config '{"sample_batch_size": 500, "num_workers": 1}'

docker run  -e "RAY_USE_XRAY=1" --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env FrozenLake-v0 \
    --run PG \
    --stop '{"training_iteration": 2}' \
    --config '{"sample_batch_size": 500, "num_workers": 1}'

docker run  -e "RAY_USE_XRAY=1" --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env Pendulum-v0 \
    --run DDPG \
    --stop '{"training_iteration": 2}' \
    --config '{"num_workers": 1}'

docker run  -e "RAY_USE_XRAY=1" --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env CartPole-v0 \
    --run IMPALA \
    --stop '{"training_iteration": 2}' \
    --config '{"gpu": false, "num_workers": 2, "min_iter_time_s": 1}'

docker run  -e "RAY_USE_XRAY=1" --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env CartPole-v0 \
    --run IMPALA \
    --stop '{"training_iteration": 2}' \
    --config '{"gpu": false, "num_workers": 2, "min_iter_time_s": 1, "model": {"use_lstm": true}}'

docker run  -e "RAY_USE_XRAY=1" --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env MountainCarContinuous-v0 \
    --run DDPG \
    --stop '{"training_iteration": 2}' \
    --config '{"num_workers": 1}'

docker run  -e "RAY_USE_XRAY=1" --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    rllib train \
    --env MountainCarContinuous-v0 \
    --run DDPG \
    --stop '{"training_iteration": 2}' \
    --config '{"num_workers": 1}'

docker run  -e "RAY_USE_XRAY=1" --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/train.py \
    --env Pendulum-v0 \
    --run APEX_DDPG \
    --ray-num-cpus 8 \
    --stop '{"training_iteration": 2}' \
    --config '{"num_workers": 2, "optimizer": {"num_replay_buffer_shards": 1}, "learning_starts": 100, "min_iter_time_s": 1}'

docker run  -e "RAY_USE_XRAY=1" --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    sh /ray/test/jenkins_tests/multi_node_tests/test_rllib_eval.sh

docker run  -e "RAY_USE_XRAY=1" --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/test/test_checkpoint_restore.py

docker run  -e "RAY_USE_XRAY=1" --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/test/test_policy_evaluator.py

docker run  -e "RAY_USE_XRAY=1" --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/test/test_serving_env.py

docker run  -e "RAY_USE_XRAY=1" --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/test/test_lstm.py

docker run  -e "RAY_USE_XRAY=1" --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/test/test_multi_agent_env.py

docker run  -e "RAY_USE_XRAY=1" --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/test/test_supported_spaces.py

docker run  -e "RAY_USE_XRAY=1" --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/tune/examples/tune_mnist_ray.py \
    --smoke-test

docker run  -e "RAY_USE_XRAY=1" --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/tune/examples/pbt_example.py \
    --smoke-test

docker run  -e "RAY_USE_XRAY=1" --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/tune/examples/hyperband_example.py \
    --smoke-test

docker run  -e "RAY_USE_XRAY=1" --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/tune/examples/async_hyperband_example.py \
    --smoke-test

docker run  -e "RAY_USE_XRAY=1" --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/tune/examples/tune_mnist_ray_hyperband.py \
    --smoke-test

docker run  -e "RAY_USE_XRAY=1" --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/tune/examples/tune_mnist_async_hyperband.py \
    --smoke-test

docker run  -e "RAY_USE_XRAY=1" --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/tune/examples/hyperopt_example.py \
    --smoke-test

docker run  -e "RAY_USE_XRAY=1" --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/tune/examples/tune_mnist_keras.py \
    --smoke-test

docker run  -e "RAY_USE_XRAY=1" --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/examples/legacy_multiagent/multiagent_mountaincar.py

docker run  -e "RAY_USE_XRAY=1" --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/examples/legacy_multiagent/multiagent_pendulum.py

docker run  -e "RAY_USE_XRAY=1" --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/examples/multiagent_cartpole.py --num-iters=2

docker run  -e "RAY_USE_XRAY=1" --rm --shm-size=10G --memory=10G $DOCKER_SHA \
    python /ray/python/ray/rllib/examples/multiagent_two_trainers.py --num-iters=2

python3 $ROOT_DIR/multi_node_docker_test.py \
    --docker-image=$DOCKER_SHA \
    --num-nodes=5 \
    --num-redis-shards=10 \
    --use-raylet \
    --test-script=/ray/test/jenkins_tests/multi_node_tests/test_0.py

python3 $ROOT_DIR/multi_node_docker_test.py \
    --docker-image=$DOCKER_SHA \
    --num-nodes=5 \
    --num-redis-shards=5 \
    --num-gpus=0,1,2,3,4 \
    --num-drivers=7 \
    --driver-locations=0,1,0,1,2,3,4 \
    --test-script=/ray/test/jenkins_tests/multi_node_tests/remove_driver_test.py

python3 $ROOT_DIR/multi_node_docker_test.py \
    --docker-image=$DOCKER_SHA \
    --num-nodes=5 \
    --num-redis-shards=2 \
    --num-gpus=0,0,5,6,50 \
    --num-drivers=100 \
    --use-raylet \
    --test-script=/ray/test/jenkins_tests/multi_node_tests/many_drivers_test.py

python3 $ROOT_DIR/multi_node_docker_test.py \
    --docker-image=$DOCKER_SHA \
    --num-nodes=1 \
    --mem-size=60G \
    --shm-size=60G \
    --use-raylet \
    --test-script=/ray/test/jenkins_tests/multi_node_tests/large_memory_test.py