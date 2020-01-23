# --------------------------------------------------------------------
# BAZEL/Travis-ci test cases.
#
# NOTE: Move more test cases here from ci/jenkins/run_rllib_tests.sh
# as Travis tests seem to run more stable.
# --------------------------------------------------------------------

#load("//bazel:rllib_train.bzl", "train")

# ---------------------------------------
# Agents (short learning tasks)
# ---------------------------------------

# TODO:

# ---------------------------------------
# Agents (Compilation and Losses)
# ---------------------------------------

# PGTrainer
py_test(
    name = "test_pg",
    size = "small",
    srcs = ["agents/pg/tests/test_pg.py"]
)

# PPOTrainer
py_test(
    name = "test_ppo",
    size = "small",
    srcs = ["agents/ppo/tests/test_ppo.py",
            "agents/ppo/tests/test.py"]  # TODO: Move down once PR 6889 merged
)

# ---------------------------------------
# Agents (quick training test iterations)
# ---------------------------------------

# A2C/A3C

py_binary(
    name = "test_a3c_pong_deterministic_v0",
    main = "train.py",
    srcs = ["train.py"],
    args = [
        "--env", "PongDeterministic-v0",
        "--run", "A3C",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"num_workers\": 2}'"
        ]
)

py_binary(
    name = "test_a3c_pong_ram_v4",
    main = "train.py",
    srcs = ["train.py"],
    args = [
        "--env", "Pong-ram-v4",
        "--run", "A3C",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"num_workers\": 2}'"
        ]
)

# TODO: add all here from new_bazel file.


# ---------------------------------------
# Models and Distributions
# ---------------------------------------

# TODO: Move here once PR 6889 merged
#py_test(
#    name = "test_distributions",
#    size = "small",
#    srcs = ["models/tests/test_distributions.py"]
#)

# ---------------------------------------
# Utils
# ---------------------------------------

py_test(
    name = "test_framework_agnostic_components",
    size = "small",
    srcs = ["utils/tests/test_framework_agnostic_components.py"]
)




# TODO
"""
train(
    name = "test",
    options = "--env PongDeterministic-v0 --run A2C --stop '{\"training_iteration\": 1}' --config '{\"num_workers\": 2}'"
)


train(
    name = "test",
    options = "--env CartPole-v1 \
    --run PPO \
    --stop '{\"training_iteration\": 1}' \
    --config '{"kl_coeff": 1.0, \"num_sgd_iter\": 10, \"lr\": 1e-4, \"sgd_minibatch_size\": 64, \"train_batch_size\": 2000, \"num_workers\": 1, \"model\": {"free_log_std": true}}'"
)


train(
    name = "test",
    options = "--env CartPole-v1 --run PPO --stop '{\"training_iteration\": 1}' --config '{\"simple_optimizer\": false, \"num_sgd_iter\": 2, \"model\": {\"use_lstm\": true}}'"
)


train(
    name = "test",
    options = "--env CartPole-v1 --run PPO --stop '{\"training_iteration\": 1}' --config '{\"simple_optimizer\": true, \"num_sgd_iter\": 2, \"model\": {\"use_lstm\": true}}'"
)


train(
    name = "test",
    options = "
    --env CartPole-v1 \
    --run PPO \
    --stop '{\"training_iteration\": 1}' \
    --config '{"kl_coeff": 1.0, \"num_sgd_iter\": 10, \"lr\": 1e-4, \"sgd_minibatch_size\": 64, \"train_batch_size\": 2000, \"num_workers\": 1, "use_gae": false, "batch_mode": "complete_episodes"}'"
)


train(
    name = "test",
    options = "
    --env CartPole-v1 \
    --run PPO \
    --stop '{\"training_iteration\": 1}' \
    --config '{"remote_worker_envs": true, "remote_env_batch_wait_ms": 99999999, \"num_envs_per_worker\": 2, \"num_workers\": 1, \"train_batch_size\": 100, \"sgd_minibatch_size\": 50}'"
)


train(
    name = "test",
    options = "
    --env CartPole-v1 \
    --run PPO \
    --stop '{\"training_iteration\": 2}' \
    --config '{"remote_worker_envs": true, \"num_envs_per_worker\": 2, \"num_workers\": 1, \"train_batch_size\": 100, \"sgd_minibatch_size\": 50}'"
)


train(
    name = "test",
    options = "
    --env Pendulum-v0 \
    --run APPO \
    --stop '{\"training_iteration\": 1}' \
    --config '{\"num_workers\": 2, \"num_gpus\": 0}'"
)


train(
    name = "test",
    options = "
    --env Pendulum-v0 \
    --run ES \
    --stop '{\"training_iteration\": 1}' \
    --config '{\"stepsize\": 0.01, \"episodes_per_batch\": 20, \"train_batch_size\": 100, \"num_workers\": 2}'"
)


train(
    name = "test",
    options = "
    --env Pong-v0 \
    --run ES \
    --stop '{\"training_iteration\": 1}' \
    --config '{\"stepsize\": 0.01, \"episodes_per_batch\": 20, \"train_batch_size\": 100, \"num_workers\": 2}'"
)


train(
    name = "test",
    options = "
    --env CartPole-v0 \
    --run A3C \
    --stop '{\"training_iteration\": 1}' \"
)


train(
    name = "test",
    options = "
    --env CartPole-v0 \
    --run DQN \
    --stop '{\"training_iteration\": 1}' \
    --config '{\"lr\": 1e-3, \"schedule_max_timesteps\": 100000, \"exploration_fraction\": 0.1, \"exploration_final_eps\": 0.02, "dueling": false, "hiddens": [], \"model\": {"fcnet_hiddens": [64], "fcnet_activation": "relu"}}'"
)


train(
    name = "test",
    options = "
    --env CartPole-v0 \
    --run DQN \
    --stop '{\"training_iteration\": 1}' \
    --config '{\"num_workers\": 2}'"
)


train(
    name = "test",
    options = "
    --env CartPole-v0 \
    --run APEX \
    --stop '{\"training_iteration\": 1}' \
    --config '{\"num_workers\": 2, "timesteps_per_iteration": 1000, \"num_gpus\": 0, \"min_iter_time_s\": 1}'"
)


train(
    name = "test",
    options = "
    --env FrozenLake-v0 \
    --run DQN \
    --stop '{\"training_iteration\": 1}'"
)


train(
    name = "test",
    options = "
    --env FrozenLake-v0 \
    --run PPO \
    --stop '{\"training_iteration\": 1}' \
    --config '{\"num_sgd_iter\": 10, \"sgd_minibatch_size\": 64, \"train_batch_size\": 1000, \"num_workers\": 1}'"
)


train(
    name = "test",
    options = "
    --env PongDeterministic-v4 \
    --run DQN \
    --stop '{\"training_iteration\": 1}' \
    --config '{\"lr\": 1e-4, \"schedule_max_timesteps\": 2000000, "buffer_size": 10000, \"exploration_fraction\": 0.1, \"exploration_final_eps\": 0.01, \"sample_batch_size\": 4, \"learning_starts\": 10000, "target_network_update_freq": 1000, "gamma": 0.99, "prioritized_replay": true}'"
)


train(
    name = "test",
    options = "
    --env MontezumaRevenge-v0 \
    --run PPO \
    --stop '{\"training_iteration\": 1}' \
    --config '{"kl_coeff": 1.0, \"num_sgd_iter\": 10, \"lr\": 1e-4, \"sgd_minibatch_size\": 64, \"train_batch_size\": 2000, \"num_workers\": 1, \"model\": {"dim": 40, "conv_filters": [[16, [8, 8], 4], [32, [4, 4], 2], [512, [5, 5], 1]]}}'"
)


train(
    name = "test",
    options = "
    --env CartPole-v1 \
    --run A3C \
    --stop '{\"training_iteration\": 1}' \
    --config '{\"num_workers\": 2, \"model\": {\"use_lstm\": true}}'"
)


train(
    name = "test",
    options = "
    --env CartPole-v0 \
    --run DQN \
    --stop '{\"training_iteration\": 1}' \
    --config '{\"num_workers\": 2}'"
)


train(
    name = "test",
    options = "
    --env CartPole-v0 \
    --run PG \
    --stop '{\"training_iteration\": 1}' \
    --config '{\"sample_batch_size\": 500, \"num_workers\": 1}'"
)


train(
    name = "test",
    options = "
    --env CartPole-v0 \
    --run PG \
    --stop '{\"training_iteration\": 1}' \
    --config '{\"sample_batch_size\": 500, "use_pytorch": true}'"
)


train(
    name = "test",
    options = "
    --env CartPole-v0 \
    --run PG \
    --stop '{\"training_iteration\": 1}' \
    --config '{\"sample_batch_size\": 500, \"num_workers\": 1, \"model\": {\"use_lstm\": true, \"max_seq_len\": 100}}'"
)


train(
    name = "test",
    options = "
    --env CartPole-v0 \
    --run PG \
    --stop '{\"training_iteration\": 1}' \
    --config '{\"sample_batch_size\": 500, \"num_workers\": 1, \"num_envs_per_worker\": 10}'"
)


train(
    name = "test",
    options = "
    --env Pong-v0 \
    --run PG \
    --stop '{\"training_iteration\": 1}' \
    --config '{\"sample_batch_size\": 500, \"num_workers\": 1}'"
)


train(
    name = "test",
    options = "
    --env FrozenLake-v0 \
    --run PG \
    --stop '{\"training_iteration\": 1}' \
    --config '{\"sample_batch_size\": 500, \"num_workers\": 1}'"
)


train(
    name = "test",
    options = "
    --env Pendulum-v0 \
    --run DDPG \
    --stop '{\"training_iteration\": 1}' \
    --config '{\"num_workers\": 1}'"
)


train(
    name = "test",
    options = "
    --env CartPole-v0 \
    --run IMPALA \
    --stop '{\"training_iteration\": 1}' \
    --config '{\"num_gpus\": 0, \"num_workers\": 2, \"min_iter_time_s\": 1}'"
)


train(
    name = "test",
    options = "
    --env CartPole-v0 \
    --run IMPALA \
    --stop '{\"training_iteration\": 1}' \
    --config '{\"num_gpus\": 0, \"num_workers\": 2, \"num_aggregation_workers\": 2, \"min_iter_time_s\": 1}'"
)


train(
    name = "test",
    options = "
    --env CartPole-v0 \
    --run IMPALA \
    --stop '{\"training_iteration\": 1}' \
    --config '{\"num_gpus\": 0, \"num_workers\": 2, \"min_iter_time_s\": 1, \"model\": {\"use_lstm\": true}}'"
)


train(
    name = "test",
    options = "
    --env CartPole-v0 \
    --run IMPALA \
    --stop '{\"training_iteration\": 1}' \
    --config '{\"num_gpus\": 0, \"num_workers\": 2, \"min_iter_time_s\": 1, "num_data_loader_buffers": 2, "replay_buffer_num_slots": 100, "replay_proportion": 1.0}'"
)


train(
    name = "test",
    options = "
    --env CartPole-v0 \
    --run IMPALA \
    --stop '{\"training_iteration\": 1}' \
    --config '{\"num_gpus\": 0, \"num_workers\": 2, \"min_iter_time_s\": 1, "num_data_loader_buffers": 2, "replay_buffer_num_slots": 100, "replay_proportion": 1.0, \"model\": {\"use_lstm\": true}}'"
)


train(
    name = "test",
    options = "
    --env MountainCarContinuous-v0 \
    --run DDPG \
    --stop '{\"training_iteration\": 1}' \
    --config '{\"num_workers\": 1}'"
)


train(
    name = "test",
    env = "MountainCarContinuous-v0",
    run = "DDPG",
    options = "--stop '{\"training_iteration\": 1}' --config '{\"num_workers\": 1}'"
)


train(
    name = "test",
    env = "Pendulum-v0",
    run = "APEX_DDPG",
    options = "--ray-num-cpus 8 --stop '{\"training_iteration\": 1}' --config '{\"num_workers\": 2, \"optimizer\": {\"num_replay_buffer_shards\": 1}, \"learning_starts\": 100, \"min_iter_time_s\": 1}'"
)

train(
    name = "test",
    env = "Pendulum-v0",
    run = "APEX_DDPG",
    options = "--ray-num-cpus 8 --stop '{\"training_iteration\": 1}' --config '{\"num_workers\": 2, \"optimizer\": {\"num_replay_buffer_shards\": 1}, \"learning_starts\": 100, \"min_iter_time_s\": 1, "batch_mode": "complete_episodes", "parameter_noise": false}'"
)

train(
    name = "test",
    env = "CartPole-v0",
    run = "MARWIL",
    options = "--stop '{\"training_iteration\": 1}' --config '{\"input\": "/ray/rllib/tests/data/cartpole_small", \"learning_starts\": 0, \"input_evaluation\": ["wis", "is"], "shuffle_buffer_size": 10}'"
)


train(
    name = "test",
    env = "CartPole-v0",
    run = "DQN",
    options = "--stop '{\"training_iteration\": 1}' --config '{\"input\": \"/ray/rllib/tests/data/cartpole_small\", \"learning_starts\": 0, \"input_evaluation\": [\"wis\", \"is\"], \"soft_q\": true}'"
)


docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output /ray/rllib/train.py \
    --env PongDeterministic-v4 \
    --run A3C \
    --stop '{"training_iteration": 1}' \
    --config '{"num_workers": 2, "use_pytorch": true, "sample_async": false, "model": {"use_lstm": false, "grayscale": true, "zero_mean": false, "dim": 84}, "preprocessor_pref": "rllib"}'

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output /ray/rllib/train.py \
    --env CartPole-v1 \
    --run A3C \
    --stop '{"training_iteration": 1}' \
    --config '{"num_workers": 2, "use_pytorch": true, "sample_async": false}'

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output /ray/rllib/train.py \
    --env Pendulum-v0 \
    --run A3C \
    --stop '{"training_iteration": 1}' \
    --config '{"num_workers": 2, "use_pytorch": true, "sample_async": false}'

docker run --rm --shm-size=${SHM_SIZE} --memory=${MEMORY_SIZE} $DOCKER_SHA \
    /ray/ci/suppress_output /ray/rllib/train.py \
    --env PongDeterministic-v4 \
    --run IMPALA \
    --stop='{"timesteps_total": 40000}' \
    --ray-object-store-memory=1000000000 \
    --config '{"num_workers": 1, "num_gpus": 0, "num_envs_per_worker": 32, "sample_batch_size": 50, "train_batch_size": 50, "learner_queue_size": 1}'
"""

