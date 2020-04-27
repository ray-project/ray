# --------------------------------------------------------------------
# BAZEL/Travis-ci test cases.
# --------------------------------------------------------------------

# To add new RLlib tests, first find the correct category of your new test
# within this file.

# All new tests - within their category - should be added alphabetically!
# Do not just add tests to the bottom of the file.

# Currently we have the following categories:

# a) Learning tests/regression, tagged: "learning_tests"
# b) Quick agent compilation/tune-train tests, tagged "quick_train"
# c-e) Utils, Models, Agents, tagged "utils", "models", and "agents_dir".
# f) Tests directory (everything in rllib/tests/...), tagged: "tests_dir"
# g) Examples directory (everything in rllib/examples/...), tagged: "examples"

# The "examples" and "tests_dir" tags have further sub-tags going by the
# starting letter of the test name (e.g. "examples_A", or "tests_dir_F") for
# split-up purposes in travis, which doesn't like tests that run for too long
# (problems: 10min timeout, not respecting ray/ci/keep_alive.sh, or even
# `travis_wait n`, etc..).

# Our travis.yml file executes all these tests in 6 different jobs, which are:
# 1) everything in a) using tf2.x
# 2) everything in a) using tf1.x
# 3) everything in b) c) d) and e)
# 4) everything in g)
# 5) f), BUT only those tagged `tests_dir_A` to `tests_dir_I`
# 6) f), BUT only those tagged `tests_dir_J` to `tests_dir_Z`


# --------------------------------------------------------------------
# Agents learning regression tests.
#
# Tag: learning_tests
#
# This will test all yaml files (via `rllib train`)
# inside rllib/tuned_examples/regression_tests for actual learning success.
# --------------------------------------------------------------------

py_test(
    name = "run_regression_tests_cartpole",
    main = "tests/run_regression_tests.py",
    tags = ["learning_tests", "learning_tests_cartpole"],
    size = "enormous",  # = 60min timeout
    srcs = ["tests/run_regression_tests.py"],
    data = glob(["tuned_examples/regression_tests/cartpole-*.yaml"]),
    # Pass `BAZEL` option and the path to look for yaml regression files.
    args = ["BAZEL", "tuned_examples/regression_tests"]
)

py_test(
    name = "run_regression_tests_pendulum",
    main = "tests/run_regression_tests.py",
    tags = ["learning_tests", "learning_tests_pendulum"],
    size = "enormous",  # = 60min timeout
    srcs = ["tests/run_regression_tests.py"],
    data = glob(["tuned_examples/regression_tests/pendulum-*.yaml"]),
    # Pass `BAZEL` option and the path to look for yaml regression files.
    args = ["BAZEL", "tuned_examples/regression_tests"]
)

# --------------------------------------------------------------------
# Agents (Compilation, Losses, simple agent functionality tests)
# rllib/agents/
#
# Tag: agents_dir
# --------------------------------------------------------------------

# A2CTrainer
py_test(
    name = "test_a2c",
    tags = ["agents_dir"],
    size = "small",
    srcs = ["agents/a3c/tests/test_a2c.py"]
)

# DDPGTrainer
py_test(
    name = "test_ddpg",
    tags = ["agents_dir"],
    size = "medium",
    srcs = ["agents/ddpg/tests/test_ddpg.py"]
)

# DQNTrainer/SimpleQTrainer
py_test(
    name = "test_dqn",
    tags = ["agents_dir"],
    size = "medium",
    srcs = ["agents/dqn/tests/test_dqn.py"]
)
py_test(
    name = "test_simple_q",
    tags = ["agents_dir"],
    size = "medium",
    srcs = ["agents/dqn/tests/test_simple_q.py"]
)

# APEXTrainer
py_test(
    name = "test_apex",
    tags = ["agents_dir"],
    size = "large",
    srcs = ["agents/dqn/tests/test_apex.py"]
)

# IMPALA
py_test(
    name = "test_vtrace",
    tags = ["agents_dir"],
    size = "small",
    srcs = ["agents/impala/tests/test_vtrace.py"]
)

# MARWILTrainer
py_test(
    name = "test_marwil",
    tags = ["agents_dir"],
    size = "small",
    srcs = ["agents/marwil/tests/test_marwil.py"]
)

# PGTrainer
py_test(
    name = "test_pg",
    tags = ["agents_dir"],
    size = "small",
    srcs = ["agents/pg/tests/test_pg.py"]
)

# PPOTrainer
py_test(
    name = "test_ppo",
    tags = ["agents_dir"],
    size = "large",
    srcs = ["agents/ppo/tests/test_ppo.py",
            "agents/ppo/tests/test.py"]  # TODO(sven): Move down once PR 6889 merged
)

# DDPPO
py_test(
    name = "test_ddppo",
    tags = ["agents_dir"],
    size = "small",
    srcs = ["agents/ppo/tests/test_ddppo.py"]
)

# APPO
py_test(
    name = "test_appo",
    tags = ["agents_dir"],
    size = "medium",
    srcs = ["agents/ppo/tests/test_appo.py"]
)

# SAC
py_test(
    name = "test_sac",
    tags = ["agents_dir"],
    size = "large",
    srcs = ["agents/sac/tests/test_sac.py"]
)

# TD3Trainer
py_test(
    name = "test_td3",
    tags = ["agents_dir"],
    size = "medium",
    srcs = ["agents/ddpg/tests/test_td3.py"]
)

# --------------------------------------------------------------------
# contrib Agents
# --------------------------------------------------------------------

py_test(
    name = "random_agent",
    tags = ["agents_dir"],
    main = "contrib/random_agent/random_agent.py",
    size = "small",
    srcs = ["contrib/random_agent/random_agent.py"]
)

py_test(
    name = "alpha_zero_cartpole",
    tags = ["agents_dir"],
    main = "contrib/alpha_zero/examples/train_cartpole.py",
    size = "large",
    srcs = ["contrib/alpha_zero/examples/train_cartpole.py"],
    args = ["--training-iteration=1", "--num-workers=2", "--ray-num-cpus=3"]
)


# --------------------------------------------------------------------
# Agents (quick training test iterations via `rllib train`)
#
# Tag: quick_train
#
# These are not(!) learning tests, we only test here compilation and
# support for certain envs, spaces, setups.
# Should all be very short tests with label: "quick_train".
# --------------------------------------------------------------------

# A2C/A3C

py_test(
    name = "test_a3c_tf_cartpole_v0",
    main = "train.py", srcs = ["train.py"],
    tags = ["quick_train"],
    args = [
        "--env", "CartPole-v0",
        "--run", "A3C",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"num_workers\": 2}'",
        "--ray-num-cpus", "4"
        ]
)

py_test(
    name = "test_a3c_torch_cartpole_v1",
    main = "train.py", srcs = ["train.py"],
    tags = ["quick_train"],
    args = [
        "--torch",
        "--env", "CartPole-v1",
        "--run", "A3C",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"num_workers\": 2, \"sample_async\": false}'",
        "--ray-num-cpus", "4"
        ]
)

py_test(
    name = "test_a3c_tf_cartpole_v1_lstm",
    main = "train.py", srcs = ["train.py"],
    tags = ["quick_train"],
    args = [
        "--env", "CartPole-v1",
        "--run", "A3C",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"num_workers\": 2, \"model\": {\"use_lstm\": true}}'",
        "--ray-num-cpus", "4"
        ]
)

py_test(
    name = "test_a3c_torch_pendulum_v0",
    main = "train.py", srcs = ["train.py"],
    size = "small",
    tags = ["quick_train"],
    args = [
        "--torch",
        "--env", "Pendulum-v0",
        "--run", "A3C",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"num_workers\": 2, \"sample_async\": false}'",
        "--ray-num-cpus", "4"
        ]
)

py_test(
    name = "test_a3c_tf_pong_deterministic_v0",
    main = "train.py", srcs = ["train.py"],
    tags = ["quick_train"],
    args = [
        "--env", "PongDeterministic-v0",
        "--run", "A3C",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"num_workers\": 2}'",
        "--ray-num-cpus", "4"
        ]
)

py_test(
    name = "test_a3c_torch_pong_deterministic_v0",
    main = "train.py", srcs = ["train.py"],
    tags = ["quick_train"],
    args = [
        "--torch",
        "--env", "PongDeterministic-v0",
        "--run", "A3C",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"num_workers\": 2, \"sample_async\": false}'",
        "--ray-num-cpus", "4"
        ]
)

py_test(
    name = "test_a3c_torch_pong_deterministic_v4",
    main = "train.py", srcs = ["train.py"],
    tags = ["quick_train"],
    args = [
        "--torch",
        "--env", "PongDeterministic-v0",
        "--run", "A3C",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"num_workers\": 2, \"use_pytorch\": true, \"sample_async\": false, \"model\": {\"use_lstm\": false, \"grayscale\": true, \"zero_mean\": false, \"dim\": 84}, \"preprocessor_pref\": \"rllib\"}'",
        "--ray-num-cpus", "4"
        ]
)

py_test(
    name = "test_a3c_tf_pong_ram_v4",
    main = "train.py", srcs = ["train.py"],
    tags = ["quick_train"],
    args = [
        "--env", "Pong-ram-v4",
        "--run", "A3C",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"num_workers\": 2}'",
        "--ray-num-cpus", "4"
        ]
)

py_test(
    name = "test_a2c_tf_pong_deterministic_v0",
    main = "train.py", srcs = ["train.py"],
    tags = ["quick_train"],
    args = [
        "--env", "PongDeterministic-v0",
        "--run", "A2C",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"num_workers\": 2}'",
        "--ray-num-cpus", "4"
        ]
)


# DDPG/APEX-DDPG/TD3

py_test(
    name = "test_ddpg_pendulum_v0",
    main = "train.py", srcs = ["train.py"],
    tags = ["quick_train"],
    args = [
        "--env", "Pendulum-v0",
        "--run", "DDPG",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"num_workers\": 1}'"
        ]
)

py_test(
    name = "test_ddpg_mountaincar_continuous_v0_num_workers_0",
    main = "train.py", srcs = ["train.py"],
    tags = ["quick_train"],
    args = [
        "--env", "MountainCarContinuous-v0",
        "--run", "DDPG",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"num_workers\": 0}'"
        ]
)

py_test(
    name = "test_ddpg_mountaincar_continuous_v0_num_workers_1",
    main = "train.py", srcs = ["train.py"],
    tags = ["quick_train"],
    args = [
        "--env", "MountainCarContinuous-v0",
        "--run", "DDPG",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"num_workers\": 1}'"
        ]
)

py_test(
    name = "test_apex_ddpg_pendulum_v0",
    main = "train.py", srcs = ["train.py"],
    tags = ["quick_train"],
    args = [
        "--env", "Pendulum-v0",
        "--run", "APEX_DDPG",
        "--ray-num-cpus", "8",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"num_workers\": 2, \"optimizer\": {\"num_replay_buffer_shards\": 1}, \"learning_starts\": 100, \"min_iter_time_s\": 1}'",
        "--ray-num-cpus", "4"
        ]
)

py_test(
    name = "test_apex_ddpg_pendulum_v0_complete_episode_batches",
    main = "train.py", srcs = ["train.py"],
    tags = ["quick_train"],
    args = [
        "--env", "Pendulum-v0",
        "--run", "APEX_DDPG",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"num_workers\": 2, \"optimizer\": {\"num_replay_buffer_shards\": 1}, \"learning_starts\": 100, \"min_iter_time_s\": 1, \"batch_mode\": \"complete_episodes\"}'",
        "--ray-num-cpus", "4",
        ]
)

py_test(
    name = "test_td3_pendulum_v0",
    main = "train.py", srcs = ["train.py"],
    tags = ["quick_train"],
    args = [
        "--env", "Pendulum-v0",
        "--run", "TD3",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"num_workers\": 1}'"
        ]
)

# DQN/APEX

py_test(
    name = "test_dqn_frozenlake_v0",
    main = "train.py", srcs = ["train.py"],
    size = "small",
    tags = ["quick_train"],
    args = [
        "--env", "FrozenLake-v0",
        "--run", "DQN",
        "--stop", "'{\"training_iteration\": 1}'"
        ]
)

py_test(
    name = "test_dqn_cartpole_v0_no_dueling",
    main = "train.py", srcs = ["train.py"],
    size = "small",
    tags = ["quick_train"],
    args = [
        "--env", "CartPole-v0",
        "--run", "DQN",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"lr\": 1e-3, \"exploration_config\": {\"epsilon_timesteps\": 10000, \"final_epsilon\": 0.02}, \"dueling\": false, \"hiddens\": [], \"model\": {\"fcnet_hiddens\": [64], \"fcnet_activation\": \"relu\"}}'"
        ]
)

py_test(
    name = "test_dqn_cartpole_v0",
    main = "train.py", srcs = ["train.py"],
    tags = ["quick_train"],
    args = [
        "--env", "CartPole-v0",
        "--run", "DQN",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"num_workers\": 2}'",
        "--ray-num-cpus", "4"
        ]
)

py_test(
    name = "test_dqn_cartpole_v0_with_offline_input_and_softq",
    main = "train.py", srcs = ["train.py"],
    tags = ["quick_train", "external_files"],
    size = "small",
    # Include the json data file.
    data = glob(["tests/data/cartpole_small/**"]),
    args = [
        "--env", "CartPole-v0",
        "--run", "DQN",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"input\": \"tests/data/cartpole_small\", \"learning_starts\": 0, \"input_evaluation\": [\"wis\", \"is\"], \"exploration_config\": {\"type\": \"SoftQ\"}}'"
        ]
)

py_test(
    name = "test_dqn_pong_deterministic_v4",
    main = "train.py", srcs = ["train.py"],
    tags = ["quick_train"],
    args = [
        "--env", "PongDeterministic-v4",
        "--run", "DQN",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"lr\": 1e-4, \"exploration_config\": {\"epsilon_timesteps\": 200000, \"final_epsilon\": 0.01}, \"buffer_size\": 10000, \"rollout_fragment_length\": 4, \"learning_starts\": 10000, \"target_network_update_freq\": 1000, \"gamma\": 0.99, \"prioritized_replay\": true}'"
        ]
)

py_test(
    name = "test_apex_cartpole_v0",
    main = "train.py", srcs = ["train.py"],
    tags = ["quick_train"],
    args = [
        "--env", "CartPole-v0",
        "--run", "APEX",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"num_workers\": 2, \"timesteps_per_iteration\": 1000, \"num_gpus\": 0, \"min_iter_time_s\": 1}'",
        "--ray-num-cpus", "4"
        ]
)

# ES

py_test(
    name = "test_es_pendulum_v0",
    main = "train.py", srcs = ["train.py"],
    tags = ["quick_train"],
    args = [
        "--env", "Pendulum-v0",
        "--run", "ES",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"stepsize\": 0.01, \"episodes_per_batch\": 20, \"train_batch_size\": 100, \"num_workers\": 2}'",
        "--ray-num-cpus", "4"
        ]
)

py_test(
    name = "test_es_pong_v0",
    main = "train.py", srcs = ["train.py"],
    tags = ["quick_train"],
    args = [
        "--env", "Pong-v0",
        "--run", "ES",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"stepsize\": 0.01, \"episodes_per_batch\": 20, \"train_batch_size\": 100, \"num_workers\": 2}'",
        "--ray-num-cpus", "4"
        ]
)

# IMPALA

py_test(
    name = "test_impala_cartpole_v0",
    main = "train.py", srcs = ["train.py"],
    tags = ["quick_train"],
    args = [
        "--env", "CartPole-v0",
        "--run", "IMPALA",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"num_gpus\": 0, \"num_workers\": 2, \"min_iter_time_s\": 1}'",
        "--ray-num-cpus", "4",
        ]
)

py_test(
    name = "test_impala_cartpole_v0_num_aggregation_workers_2",
    main = "train.py", srcs = ["train.py"],
    tags = ["quick_train"],
    args = [
        "--env", "CartPole-v0",
        "--run", "IMPALA",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"num_gpus\": 0, \"num_workers\": 2, \"num_aggregation_workers\": 2, \"min_iter_time_s\": 1}'",
        "--ray-num-cpus", "5",
        ]
)

py_test(
    name = "test_impala_cartpole_v0_lstm",
    main = "train.py", srcs = ["train.py"],
    tags = ["quick_train"],
    args = [
        "--env", "CartPole-v0",
        "--run", "IMPALA",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"num_gpus\": 0, \"num_workers\": 2, \"min_iter_time_s\": 1, \"model\": {\"use_lstm\": true}}'",
        "--ray-num-cpus", "4",
        ]
)

py_test(
    name = "test_impala_buffers_2",
    main = "train.py", srcs = ["train.py"],
    tags = ["quick_train"],
    args = [
        "--env", "CartPole-v0",
        "--run", "IMPALA",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"num_gpus\": 0, \"num_workers\": 2, \"min_iter_time_s\": 1, \"num_data_loader_buffers\": 2, \"replay_buffer_num_slots\": 100, \"replay_proportion\": 1.0}'",
        "--ray-num-cpus", "4",
        ]
)

py_test(
    name = "test_impala_cartpole_v0_buffers_2_lstm",
    main = "train.py", srcs = ["train.py"],
    tags = ["quick_train"],
    args = [
        "--env", "CartPole-v0",
        "--run", "IMPALA",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"num_gpus\": 0, \"num_workers\": 2, \"min_iter_time_s\": 1, \"num_data_loader_buffers\": 2, \"replay_buffer_num_slots\": 100, \"replay_proportion\": 1.0, \"model\": {\"use_lstm\": true}}'",
        "--ray-num-cpus", "4",
        ]
)

py_test(
    name = "test_impala_pong_deterministic_v4_40k_ts_1G_obj_store",
    main = "train.py", srcs = ["train.py"],
    tags = ["quick_train"],
    args = [
        "--env", "PongDeterministic-v4",
        "--run", "IMPALA",
        "--stop", "'{\"timesteps_total\": 40000}'",
        "--ray-object-store-memory=1000000000",
        "--config", "'{\"num_workers\": 1, \"num_gpus\": 0, \"num_envs_per_worker\": 32, \"rollout_fragment_length\": 50, \"train_batch_size\": 50, \"learner_queue_size\": 1}'"
        ]
)

# MARWIL

py_test(
    name = "test_marwil_cartpole_v0_tf",
    main = "train.py",
    srcs = ["train.py"],
    tags = ["quick_train", "external_files"],
    size = "small",
    # Include the json data file.
    data = glob(["tests/data/cartpole_small/**"]),
    args = [
        "--env", "CartPole-v0",
        "--run", "MARWIL",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"input\": \"tests/data/cartpole_small\", \"learning_starts\": 0, \"input_evaluation\": [\"wis\", \"is\"], \"shuffle_buffer_size\": 10}'"
        ]
)

py_test(
    name = "test_marwil_cartpole_v0_torch",
    main = "train.py",
    srcs = ["train.py"],
    tags = ["quick_train", "external_files"],
    size = "small",
    # Include the json data file.
    data = glob(["tests/data/cartpole_small/**"]),
    args = [
        "--env", "CartPole-v0",
        "--run", "MARWIL",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"use_pytorch\": true, \"input\": \"tests/data/cartpole_small\", \"learning_starts\": 0, \"input_evaluation\": [\"wis\", \"is\"], \"shuffle_buffer_size\": 10}'"
        ]
)

# PG

py_test(
    name = "test_pg_tf_frozenlake_v0",
    main = "train.py", srcs = ["train.py"],
    tags = ["quick_train"],
    args = [
        "--env", "FrozenLake-v0",
        "--run", "PG",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"rollout_fragment_length\": 500, \"num_workers\": 1}'"
        ]
)

py_test(
    name = "test_pg_torch_frozenlake_v0",
    main = "train.py", srcs = ["train.py"],
    size = "small",
    tags = ["quick_train"],
    args = [
        "--torch",
        "--env", "FrozenLake-v0",
        "--run", "PG",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"rollout_fragment_length\": 500, \"num_workers\": 1}'"
        ]
)

py_test(
    name = "test_pg_tf_cartpole_v0",
    main = "train.py", srcs = ["train.py"],
    size = "small",
    tags = ["quick_train"],
    args = [
        "--env", "CartPole-v0",
        "--run", "PG",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"rollout_fragment_length\": 500, \"num_workers\": 1}'"
        ]
)

py_test(
    name = "test_pg_torch_cartpole_v0",
    main = "train.py", srcs = ["train.py"],
    tags = ["quick_train"],
    args = [
        "--torch",
        "--env", "CartPole-v0",
        "--run", "PG",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"rollout_fragment_length\": 500}'"
        ]
)

py_test(
    name = "test_pg_tf_cartpole_v0_lstm",
    main = "train.py", srcs = ["train.py"],
    tags = ["quick_train"],
    args = [
        "--env", "CartPole-v0",
        "--run", "PG",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"rollout_fragment_length\": 500, \"num_workers\": 1, \"model\": {\"use_lstm\": true, \"max_seq_len\": 100}}'"
        ]
)

py_test(
    name = "test_pg_tf_cartpole_v0_multi_envs_per_worker",
    main = "train.py", srcs = ["train.py"],
    size = "small",
    tags = ["quick_train"],
    args = [
        "--env", "CartPole-v0",
        "--run", "PG",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"rollout_fragment_length\": 500, \"num_workers\": 1, \"num_envs_per_worker\": 10}'"
        ]
)


py_test(
    name = "test_pg_tf_pong_v0",
    main = "train.py", srcs = ["train.py"],
    tags = ["quick_train"],
    args = [
        "--env", "Pong-v0",
        "--run", "PG",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"rollout_fragment_length\": 500, \"num_workers\": 1}'"
        ]
)

# PPO/APPO

py_test(
    name = "test_ppo_tf_frozenlake_v0",
    main = "train.py", srcs = ["train.py"],
    size = "small",
    tags = ["quick_train"],
    args = [
        "--env", "FrozenLake-v0",
        "--run", "PPO",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"num_sgd_iter\": 10, \"sgd_minibatch_size\": 64, \"train_batch_size\": 1000, \"num_workers\": 1}'"
        ]
)

py_test(
    name = "test_ppo_torch_frozenlake_v0",
    main = "train.py", srcs = ["train.py"],
    size = "small",
    tags = ["quick_train"],
    args = [
        "--torch",
        "--env", "FrozenLake-v0",
        "--run", "PPO",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"num_sgd_iter\": 10, \"sgd_minibatch_size\": 64, \"train_batch_size\": 1000, \"num_workers\": 1}'"
        ]
)

py_test(
    name = "test_ppo_tf_cartpole_v1",
    main = "train.py", srcs = ["train.py"],
    tags = ["quick_train"],
    args = [
        "--env", "CartPole-v1",
        "--run", "PPO",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"kl_coeff\": 1.0, \"num_sgd_iter\": 10, \"lr\": 1e-4, \"sgd_minibatch_size\": 64, \"train_batch_size\": 2000, \"num_workers\": 1, \"model\": {\"free_log_std\": true}}'"
        ]
)

py_test(
    name = "test_ppo_torch_cartpole_v1",
    main = "train.py", srcs = ["train.py"],
    size = "small",
    tags = ["quick_train"],
    args = [
        "--torch",
        "--env", "CartPole-v1",
        "--run", "PPO",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"kl_coeff\": 1.0, \"num_sgd_iter\": 10, \"lr\": 1e-4, \"sgd_minibatch_size\": 64, \"train_batch_size\": 2000, \"num_workers\": 1, \"model\": {\"free_log_std\": true}}'"
        ]
)

py_test(
    name = "test_ppo_tf_cartpole_v1_lstm",
    main = "train.py", srcs = ["train.py"],
    tags = ["quick_train"],
    args = [
        "--env", "CartPole-v1",
        "--run", "PPO",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"simple_optimizer\": false, \"num_sgd_iter\": 2, \"model\": {\"use_lstm\": true}}'",
        "--ray-num-cpus", "4"
        ]
)

py_test(
    name = "test_ppo_tf_cartpole_v1_lstm_simple_optimizer",
    main = "train.py", srcs = ["train.py"],
    tags = ["quick_train"],
    args = [
        "--env", "CartPole-v1",
        "--run", "PPO",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"simple_optimizer\": true, \"num_sgd_iter\": 2, \"model\": {\"use_lstm\": true}}'",
        "--ray-num-cpus", "4"
        ]
)

# TODO(sven): Fix LSTM auto-wrapping for torch models. This test case did not(!) exist in Jenkins.
#py_test(
#    name = "test_ppo_torch_cartpole_v1_lstm_simple_optimizer",
#    main = "train.py", srcs = ["train.py"],
#    args = [
#        "--torch",
#        "--env", "CartPole-v1",
#        "--run", "PPO",
#        "--stop", "'{\"training_iteration\": 1}'",
#        "--config", "'{\"simple_optimizer\": true, \"num_sgd_iter\": 2, \"model\": {\"use_lstm\": true}}'",
#        "--ray-num-cpus", "4"
#        ]
#)

py_test(
    name = "test_ppo_tf_cartpole_v1_complete_episode_batches",
    main = "train.py", srcs = ["train.py"],
    tags = ["quick_train"],
    args = [
        "--env", "CartPole-v1",
        "--run", "PPO",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"kl_coeff\": 1.0, \"num_sgd_iter\": 10, \"lr\": 1e-4, \"sgd_minibatch_size\": 64, \"train_batch_size\": 2000, \"num_workers\": 1, \"use_gae\": false, \"batch_mode\": \"complete_episodes\"}'"
        ]
)

py_test(
    name = "test_ppo_tf_cartpole_v1_remote_worker_envs",
    main = "train.py", srcs = ["train.py"],
    tags = ["quick_train"],
    args = [
        "--env", "CartPole-v1",
        "--run", "PPO",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"remote_worker_envs\": true, \"remote_env_batch_wait_ms\": 99999999, \"num_envs_per_worker\": 2, \"num_workers\": 1, \"train_batch_size\": 100, \"sgd_minibatch_size\": 50}'"
        ]
)

py_test(
    name = "test_ppo_tf_cartpole_v1_remote_worker_envs_b",
    main = "train.py", srcs = ["train.py"],
    tags = ["quick_train"],
    args = [
        "--env", "CartPole-v1",
        "--run", "PPO",
        "--stop", "'{\"training_iteration\": 2}'",
        "--config", "'{\"remote_worker_envs\": true, \"num_envs_per_worker\": 2, \"num_workers\": 1, \"train_batch_size\": 100, \"sgd_minibatch_size\": 50}'"
        ]
)

py_test(
    name = "test_ppo_tf_montezuma_revenge_v0",
    main = "train.py", srcs = ["train.py"],
    tags = ["quick_train"],
    args = [
        "--env", "MontezumaRevenge-v0",
        "--run", "PPO",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"kl_coeff\": 1.0, \"num_sgd_iter\": 10, \"lr\": 1e-4, \"sgd_minibatch_size\": 64, \"train_batch_size\": 2000, \"num_workers\": 1, \"model\": {\"dim\": 40, \"conv_filters\": [[16, [8, 8], 4], [32, [4, 4], 2], [512, [5, 5], 1]]}}'"
        ]
)

py_test(
    name = "test_ppo_torch_montezuma_revenge_v0",
    main = "train.py", srcs = ["train.py"],
    tags = ["quick_train"],
    args = [
        "--torch",
        "--env", "MontezumaRevenge-v0",
        "--run", "PPO",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"kl_coeff\": 1.0, \"num_sgd_iter\": 10, \"lr\": 1e-4, \"sgd_minibatch_size\": 64, \"train_batch_size\": 2000, \"num_workers\": 1, \"model\": {\"dim\": 40, \"conv_filters\": [[16, [8, 8], 4], [32, [4, 4], 2], [512, [5, 5], 1]]}}'"
        ]
)

py_test(
    name = "test_appo_tf_pendulum_v0_no_gpus",
    main = "train.py", srcs = ["train.py"],
    tags = ["quick_train"],
    args = [
        "--env", "Pendulum-v0",
        "--run", "APPO",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"num_workers\": 2, \"num_gpus\": 0}'",
        "--ray-num-cpus", "4"
        ]
)

# --------------------------------------------------------------------
# Models and Distributions
# rllib/models/
#
# Tag: models
# --------------------------------------------------------------------

py_test(
    name = "test_distributions",
    tags = ["models"],
    size = "small",
    srcs = ["models/tests/test_distributions.py"]
)

# --------------------------------------------------------------------
# Optimizers and Memories
# rllib/optimizers/
#
# Tag: optimizers
# --------------------------------------------------------------------

py_test(
    name = "test_optimizers",
    tags = ["optimizers"],
    size = "large",
    srcs = ["optimizers/tests/test_optimizers.py"]
)

py_test(
    name = "test_segment_tree",
    tags = ["optimizers"],
    size = "small",
    srcs = ["optimizers/tests/test_segment_tree.py"]
)

py_test(
    name = "test_prioritized_replay_buffer",
    tags = ["optimizers"],
    size = "small",
    srcs = ["optimizers/tests/test_prioritized_replay_buffer.py"]
)

# --------------------------------------------------------------------
# Policies
# rllib/policy/
#
# Tag: policy
# --------------------------------------------------------------------

py_test(
    name = "policy/tests/test_compute_log_likelihoods",
    tags = ["policy"],
    size = "medium",
    srcs = ["policy/tests/test_compute_log_likelihoods.py"]
)

# --------------------------------------------------------------------
# Utils:
# rllib/utils/
#
# Tag: utils
# --------------------------------------------------------------------

py_test(
    name = "test_explorations",
    tags = ["utils"],
    size = "large",
    srcs = ["utils/exploration/tests/test_explorations.py"]
)

py_test(
    name = "test_parameter_noise",
    tags = ["utils"],
    size = "small",
    srcs = ["utils/exploration/tests/test_parameter_noise.py"]
)

# Schedules
py_test(
    name = "test_schedules",
    tags = ["utils"],
    size = "small",
    srcs = ["utils/schedules/tests/test_schedules.py"]
)

py_test(
    name = "test_framework_agnostic_components",
    tags = ["utils"],
    size = "small",
    data = glob(["utils/tests/**"]),
    srcs = ["utils/tests/test_framework_agnostic_components.py"]
)

# TaskPool
py_test(
    name = "test_taskpool",
    tags = ["utils"],
    size = "small",
    srcs = ["utils/tests/test_taskpool.py"]
)

# --------------------------------------------------------------------
# rllib/tests/ directory
#
# Tag: tests_dir, tests_dir_[A-Z]
#
# NOTE: Add tests alphabetically into this list and make sure, to tag
# it correctly by its starting letter, e.g. tags=["tests_dir", "tests_dir_A"]
# for `tests/test_all_stuff.py`.
# --------------------------------------------------------------------

py_test(
    name = "tests/test_avail_actions_qmix",
    tags = ["tests_dir", "tests_dir_A"],
    size = "small",
    srcs = ["tests/test_avail_actions_qmix.py"]
)

py_test(
    name = "tests/test_catalog",
    tags = ["tests_dir", "tests_dir_C"],
    size = "small",
    srcs = ["tests/test_catalog.py"]
)

py_test(
    name = "tests/test_checkpoint_restore",
    tags = ["tests_dir", "tests_dir_C"],
    size = "enormous",
    srcs = ["tests/test_checkpoint_restore.py"]
)

py_test(
    name = "tests/test_dependency",
    tags = ["tests_dir", "tests_dir_D"],
    size = "small",
    srcs = ["tests/test_dependency.py"]
)

py_test(
    name = "tests/test_dependency_torch",
    tags = ["tests_dir", "tests_dir_D"],
    size = "small",
    srcs = ["tests/test_dependency_torch.py"]
)

py_test(
    name = "tests/test_eager_support",
    tags = ["tests_dir", "tests_dir_E"],
    size = "enormous",
    srcs = ["tests/test_eager_support.py"]
)

py_test(
    name = "test_env_with_subprocess",
    main = "tests/test_env_with_subprocess.py",
    tags = ["tests_dir", "tests_dir_E"],
    size = "small",
    srcs = ["tests/test_env_with_subprocess.py"]
)

py_test(
    name = "tests/test_evaluators",
    tags = ["tests_dir", "tests_dir_E"],
    size = "medium",
    srcs = ["tests/test_evaluators.py"]
)

py_test(
    name = "tests/test_external_env",
    tags = ["tests_dir", "tests_dir_E"],
    size = "large",
    srcs = ["tests/test_external_env.py"]
)

py_test(
    name = "tests/test_external_multi_agent_env",
    tags = ["tests_dir", "tests_dir_E"],
    size = "medium",
    srcs = ["tests/test_external_multi_agent_env.py"]
)

py_test(
    name = "tests/test_filters",
    tags = ["tests_dir", "tests_dir_F"],
    size = "small",
    srcs = ["tests/test_filters.py"]
)

py_test(
    name = "tests/test_ignore_worker_failure",
    tags = ["tests_dir", "tests_dir_I"],
    size = "large",
    srcs = ["tests/test_ignore_worker_failure.py"]
)

py_test(
    name = "tests/test_io",
    tags = ["tests_dir", "tests_dir_I"],
    size = "medium",
    srcs = ["tests/test_io.py"]
)

py_test(
    name = "tests/test_execution",
    tags = ["tests_dir", "tests_dir_E"],
    size = "medium",
    srcs = ["tests/test_execution.py"]
)

py_test(
    name = "tests/test_local",
    tags = ["tests_dir", "tests_dir_L"],
    size = "medium",
    srcs = ["tests/test_local.py"]
)

py_test(
    name = "tests/test_lstm",
    tags = ["tests_dir", "tests_dir_L"],
    size = "medium",
    srcs = ["tests/test_lstm.py"]
)

py_test(
    name = "tests/test_model_imports",
    tags = ["tests_dir", "tests_dir_M", "model_imports"],
    size = "small",
    data = glob(["tests/data/model_weights/**"]),
    srcs = ["tests/test_model_imports.py"]
)

py_test(
    name = "tests/test_multi_agent_env",
    tags = ["tests_dir", "tests_dir_M"],
    size = "large",
    srcs = ["tests/test_multi_agent_env.py"]
)

py_test(
    name = "tests/test_multi_agent_pendulum",
    tags = ["tests_dir", "tests_dir_M"],
    size = "large",
    srcs = ["tests/test_multi_agent_pendulum.py"]
)

py_test(
    name = "tests/test_nested_spaces",
    tags = ["tests_dir", "tests_dir_N"],
    size = "small",
    srcs = ["tests/test_nested_spaces.py"]
)

py_test(
    name = "tests/test_exec_api",
    tags = ["tests_dir", "tests_dir_E"],
    size = "small",
    srcs = ["tests/test_exec_api.py"]
)

py_test(
    name = "tests/test_reproducibility",
    tags = ["tests_dir", "tests_dir_R"],
    size = "large",
    srcs = ["tests/test_reproducibility.py"]
)

py_test(
    name = "test_rollout",
    main = "tests/test_rollout.py",
    tags = ["tests_dir", "tests_dir_R"],
    size = "enormous",
    data = ["train.py", "rollout.py"],
    srcs = ["tests/test_rollout.py"]
)

py_test(
    name = "tests/test_rollout_worker",
    tags = ["tests_dir", "tests_dir_R"],
    size = "large",
    srcs = ["tests/test_rollout_worker.py"]
)

py_test(
    name = "tests/test_supported_spaces",
    tags = ["tests_dir", "tests_dir_S"],
    size = "enormous",
    srcs = ["tests/test_supported_spaces.py"]
)

# --------------------------------------------------------------------
# examples/ directory
#
# Tag: examples, examples_[A-Z]
#
# NOTE: Add tests alphabetically into this list and make sure, to tag
# it correctly by its starting letter, e.g. tags=["examples", "examples_A"]
# for `examples/all_stuff.py`.
# --------------------------------------------------------------------


py_test(
    name = "examples/autoregressive_action_dist", main = "examples/autoregressive_action_dist.py",
    tags = ["examples", "examples_A"],
    size = "large",
    srcs = ["examples/autoregressive_action_dist.py"],
    args = ["--stop=150", "--num-cpus=4"]
)

py_test(
    name = "examples/batch_norm_model_ppo", main="examples/batch_norm_model.py",
    tags = ["examples", "examples_B"],
    size = "medium",
    srcs = ["examples/batch_norm_model.py"],
    args = ["--run=PPO", "--num-iters=1"]
)

py_test(
    name = "examples/batch_norm_model_pg", main="examples/batch_norm_model.py",
    tags = ["examples", "examples_B"],
    size = "medium",
    srcs = ["examples/batch_norm_model.py"],
    args = ["--run=PG", "--num-iters=1"]
)

py_test(
    name = "examples/batch_norm_model_dqn", main="examples/batch_norm_model.py",
    tags = ["examples", "examples_B"],
    size = "medium",
    srcs = ["examples/batch_norm_model.py"],
    args = ["--run=DQN", "--num-iters=1"]
)

py_test(
    name = "examples/batch_norm_model_ddpg", main="examples/batch_norm_model.py",
    tags = ["examples", "examples_B"],
    size = "medium",
    srcs = ["examples/batch_norm_model.py"],
    args = ["--run=DDPG", "--num-iters=1"]
)

py_test(
    name = "examples/cartpole_lstm_impala", main="examples/cartpole_lstm.py",
    tags = ["examples", "examples_C"],
    size = "medium",
    srcs = ["examples/cartpole_lstm.py"],
    args = ["--run=IMPALA", "--stop=40", "--num-cpus=4"]
)

py_test(
    name = "examples/cartpole_lstm_ppo", main="examples/cartpole_lstm.py",
    tags = ["examples", "examples_C"],
    size = "medium",
    srcs = ["examples/cartpole_lstm.py"],
    args = ["--run=PPO", "--stop=40", "--num-cpus=4"]
)

py_test(
    name = "examples/cartpole_lstm_ppo_with_prev_a_and_r", main="examples/cartpole_lstm.py",
    tags = ["examples", "examples_C"],
    size = "large",
    srcs = ["examples/cartpole_lstm.py"],
    args = ["--run=PPO", "--stop=40", "--use-prev-action-reward", "--num-cpus=4"]
)

py_test(
    name = "examples/centralized_critic",
    tags = ["examples", "examples_C"],
    size = "medium",
    srcs = ["examples/centralized_critic.py"],
    args = ["--stop=2000"]
)

py_test(
    name = "examples/centralized_critic_2",
    tags = ["examples", "examples_C"],
    size = "medium",
    srcs = ["examples/centralized_critic_2.py"],
    args = ["--stop=2000"]
)

py_test(
    name = "examples/custom_eval", main = "examples/custom_eval.py",
    tags = ["examples", "examples_C"],
    size = "medium",
    srcs = ["examples/custom_eval.py"],
    args = ["--custom-eval", "--num-cpus=4"]
)

py_test(
    name = "examples/custom_keras_model_a2c", main="examples/custom_keras_model.py",
    tags = ["examples", "examples_C"],
    size = "large",
    srcs = ["examples/custom_keras_model.py"],
    args = ["--run=A2C", "--stop=50", "--num-cpus=4"]
)

py_test(
    name = "examples/custom_keras_model_dqn", main="examples/custom_keras_model.py",
    tags = ["examples", "examples_C"],
    size = "medium",
    srcs = ["examples/custom_keras_model.py"],
    args = ["--run=DQN", "--stop=50"]
)

py_test(
    name = "examples/custom_keras_model_ppo", main="examples/custom_keras_model.py",
    tags = ["examples", "examples_C"],
    size = "medium",
    srcs = ["examples/custom_keras_model.py"],
    args = ["--run=PPO", "--stop=50", "--num-cpus=4"]
)

py_test(
    name = "examples/custom_keras_rnn_model_repeat_after_me", main = "examples/custom_keras_rnn_model.py",
    tags = ["examples", "examples_C"],
    size = "large",
    srcs = ["examples/custom_keras_rnn_model.py"],
    args = ["--run=PPO", "--stop=50", "--env=RepeatAfterMeEnv", "--num-cpus=4"]
)

py_test(
    name = "examples/custom_keras_rnn_model_repeat_initial",
    main = "examples/custom_keras_rnn_model.py",
    tags = ["examples", "examples_C"],
    size = "large",
    srcs = ["examples/custom_keras_rnn_model.py"],
    args = ["--run=PPO", "--stop=50", "--env=RepeatInitialEnv", "--num-cpus=4"]
)

py_test(
    name = "examples/custom_loss",
    tags = ["examples", "examples_C"],
    size = "small",
    # Include the json data file.
    data = glob(["tests/data/cartpole_small/**"]),
    srcs = ["examples/custom_loss.py"],
    args = ["--iters=2", "--input-files=tests/data/cartpole_small"]
)

py_test(
    name = "examples/custom_metrics_and_callbacks",
    tags = ["examples", "examples_C"],
    size = "small",
    srcs = ["examples/custom_metrics_and_callbacks.py"],
    args = ["--num-iters=2"]
)

py_test(
    name = "examples/custom_metrics_and_callbacks_legacy",
    tags = ["examples", "examples_C"],
    size = "small",
    srcs = ["examples/custom_metrics_and_callbacks_legacy.py"],
    args = ["--num-iters=2"]
)

py_test(
    name = "examples/custom_tf_policy",
    tags = ["examples", "examples_C"],
    size = "medium",
    srcs = ["examples/custom_tf_policy.py"],
    args = ["--iters=2", "--num-cpus=4"]
)

py_test(
    name = "examples/custom_torch_rnn_model",
    main = "examples/custom_torch_rnn_model.py",
    tags = ["examples", "examples_C"],
    size = "medium",
    srcs = ["examples/custom_torch_rnn_model.py"],
    args = ["--run=PPO", "--stop=90", "--num-cpus=4"]
)

py_test(
    name = "examples/custom_torch_policy",
    tags = ["examples", "examples_C"],
    size = "small",
    srcs = ["examples/custom_torch_policy.py"],
    args = ["--iters=2", "--num-cpus=4"]
)

py_test(
    name = "examples/eager_execution",
    tags = ["examples", "examples_E"],
    size = "small",
    srcs = ["examples/eager_execution.py"],
    args = ["--iters=2"]
)

py_test(
    name = "examples/multi_agent_cartpole",
    tags = ["examples", "examples_M"],
    size = "medium",
    srcs = ["examples/multi_agent_cartpole.py"],
    args = ["--num-iters=2", "--num-cpus=4"]
)

py_test(
    name = "examples/multi_agent_custom_policy",
    tags = ["examples", "examples_M"],
    size = "medium",
    srcs = ["examples/multi_agent_custom_policy.py"],
)

py_test(
    name = "examples/multi_agent_two_trainers",
    tags = ["examples", "examples_M"],
    size = "medium",
    srcs = ["examples/multi_agent_two_trainers.py"],
    args = ["--num-iters=2"]
)

py_test(
    name = "examples/parametric_action_cartpole_pg", main="examples/parametric_action_cartpole.py",
    tags = ["examples", "examples_P"],
    size = "medium",
    srcs = ["examples/parametric_action_cartpole.py"],
    args = ["--run=PG", "--stop=50"]
)

py_test(
    name = "examples/parametric_action_cartpole_ppo", main="examples/parametric_action_cartpole.py",
    tags = ["examples", "examples_P"],
    size = "medium",
    srcs = ["examples/parametric_action_cartpole.py"],
    args = ["--run=PPO", "--stop=50"]
)

py_test(
    name = "examples/parametric_action_cartpole_dqn", main="examples/parametric_action_cartpole.py",
    tags = ["examples", "examples_P"],
    size = "medium",
    srcs = ["examples/parametric_action_cartpole.py"],
    args = ["--run=DQN", "--stop=50"]
)

py_test(
    name = "examples/random_env", main = "examples/random_env.py",
    tags = ["examples", "examples_R"],
    size = "large",
    srcs = ["examples/random_env.py"]
)

py_test(
    name = "examples/rollout_worker_custom_workflow",
    tags = ["examples", "examples_R"],
    size = "small",
    srcs = ["examples/rollout_worker_custom_workflow.py"],
    args = ["--num-cpus=4"]
)

sh_test(
    name = "examples/serving/test_local_inference",
    tags = ["examples", "examples_L", "exclusive"],
    size = "medium",
    srcs = ["examples/serving/test_local_inference.sh"],
    data = glob(["examples/serving/*.py"]),
)

sh_test(
    name = "examples/serving/test_remote_inference",
    tags = ["examples", "examples_R", "exclusive"],
    size = "medium",
    srcs = ["examples/serving/test_remote_inference.sh"],
    data = glob(["examples/serving/*.py"]),
)

py_test(
    name = "examples/rock_paper_scissors_multiagent",
    main = "examples/rock_paper_scissors_multiagent.py",
    tags = ["examples", "examples_R"],
    size = "large",
    srcs = ["examples/rock_paper_scissors_multiagent.py"],
    args = ["--stop=200"],
)

py_test(
    name = "examples/twostep_game_maddpg", main = "examples/twostep_game.py",
    tags = ["examples", "examples_T"],
    size = "large",
    srcs = ["examples/twostep_game.py"],
    args = ["--stop=2000", "--run=contrib/MADDPG"]
)

py_test(
    name = "contrib/bandits/examples/lin_ts",
    main = "contrib/bandits/examples/simple_context_bandit.py",
    tags = ["examples", "examples_T"],
    size = "small",
    srcs = ["contrib/bandits/examples/simple_context_bandit.py"],
    args = ["--stop-at-reward=10", "--run=contrib/LinTS"],
)

py_test(
    name = "contrib/bandits/examples/lin_ucb",
    main = "contrib/bandits/examples/simple_context_bandit.py",
    tags = ["examples", "examples_U"],
    size = "small",
    srcs = ["contrib/bandits/examples/simple_context_bandit.py"],
    args = ["--stop-at-reward=10", "--run=contrib/LinUCB"],
)

py_test(
    name = "examples/twostep_game_pg", main = "examples/twostep_game.py",
    tags = ["examples", "examples_T"],
    size = "medium",
    srcs = ["examples/twostep_game.py"],
    args = ["--stop=2000", "--run=PG"]
)

py_test(
    name = "examples/twostep_game_qmix", main = "examples/twostep_game.py",
    tags = ["examples", "examples_T"],
    size = "medium",
    srcs = ["examples/twostep_game.py"],
    args = ["--stop=2000", "--run=QMIX"]
)

py_test(
    name = "examples/twostep_game_apex_qmix", main = "examples/twostep_game.py",
    tags = ["examples", "examples_T"],
    size = "medium",
    srcs = ["examples/twostep_game.py"],
    args = ["--stop=2000", "--run=APEX_QMIX", "--num-cpus=4"]
)
