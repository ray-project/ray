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

# Our travis.yml file executes all these tests in 7 different jobs, which are:
# 1) everything in a) using tf2.x
# 2) everything in a) using tf1.x
# 3) everything in a) using torch
# 4) everything in b) c) d) and e)
# 5) everything in g)
# 6) f), BUT only those tagged `tests_dir_A` to `tests_dir_[some letter]`
# 7) f), BUT only those tagged `tests_dir_[some letter]` to `tests_dir_Z`


# --------------------------------------------------------------------
# Agents learning regression tests.
#
# Tag: learning_tests
#
# This will test all yaml files (via `rllib train`)
# inside rllib/tuned_examples/[algo-name] for actual learning success.
# --------------------------------------------------------------------

# A2C/A3C
py_test(
    name = "run_regression_tests_cartpole_a2c_tf",
    main = "tests/run_regression_tests.py",
    tags = ["learning_tests_tf", "learning_tests_cartpole"],
    size = "medium",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/a3c/cartpole-a2c.yaml"],
    args = ["--yaml-dir=tuned_examples/a3c"]
)

py_test(
    name = "run_regression_tests_cartpole_a2c_torch",
    main = "tests/run_regression_tests.py",
    tags = ["learning_tests_torch", "learning_tests_cartpole"],
    size = "medium",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/a3c/cartpole-a2c.yaml"],
    args = ["--yaml-dir=tuned_examples/a3c", "--framework=torch"]
)

py_test(
    name = "run_regression_tests_cartpole_a3c_tf",
    main = "tests/run_regression_tests.py",
    tags = ["learning_tests_tf", "learning_tests_cartpole"],
    size = "medium",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/a3c/cartpole-a3c.yaml"],
    args = ["--yaml-dir=tuned_examples/a3c"]
)

py_test(
    name = "run_regression_tests_cartpole_a3c_torch",
    main = "tests/run_regression_tests.py",
    tags = ["learning_tests_torch", "learning_tests_cartpole"],
    size = "medium",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/a3c/cartpole-a3c.yaml"],
    args = ["--yaml-dir=tuned_examples/a3c", "--framework=torch"]
)

# APPO
py_test(
    name = "run_regression_tests_cartpole_appo_tf",
    main = "tests/run_regression_tests.py",
    tags = ["learning_tests_tf", "learning_tests_cartpole"],
    size = "medium",
    srcs = ["tests/run_regression_tests.py"],
    data = [
        "tuned_examples/ppo/cartpole-appo.yaml",
        "tuned_examples/ppo/cartpole-appo-vtrace.yaml"
    ],
    args = ["--yaml-dir=tuned_examples/ppo"]
)

py_test(
    name = "run_regression_tests_cartpole_appo_torch",
    main = "tests/run_regression_tests.py",
    tags = ["learning_tests_torch", "learning_tests_cartpole"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = [
        "tuned_examples/ppo/cartpole-appo.yaml",
        "tuned_examples/ppo/cartpole-appo-vtrace.yaml"
    ],
    args = ["--yaml-dir=tuned_examples/ppo", "--framework=torch"]
)

# ARS
py_test(
    name = "run_regression_tests_cartpole_ars_tf",
    main = "tests/run_regression_tests.py",
    tags = ["learning_tests_tf", "learning_tests_cartpole"],
    size = "medium",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/ars/cartpole-ars.yaml"],
    args = ["--yaml-dir=tuned_examples/ars"]
)

py_test(
    name = "run_regression_tests_cartpole_ars_torch",
    main = "tests/run_regression_tests.py",
    tags = ["learning_tests_torch", "learning_tests_cartpole"],
    size = "medium",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/ars/cartpole-ars.yaml"],
    args = ["--yaml-dir=tuned_examples/ars", "--framework=torch"]
)

# DDPG
py_test(
    name = "run_regression_tests_pendulum_ddpg_tf",
    main = "tests/run_regression_tests.py",
    tags = ["learning_tests_tf", "learning_tests_pendulum"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = glob(["tuned_examples/ddpg/pendulum-ddpg.yaml"]),
    args = ["--yaml-dir=tuned_examples/ddpg"]
)

py_test(
    name = "run_regression_tests_pendulum_ddpg_torch",
    main = "tests/run_regression_tests.py",
    tags = ["learning_tests_torch", "learning_tests_pendulum"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = glob(["tuned_examples/ddpg/pendulum-ddpg.yaml"]),
    args = ["--yaml-dir=tuned_examples/ddpg", "--framework=torch"]
)

# DDPPO
py_test(
    name = "run_regression_tests_cartpole_ddppo_torch",
    main = "tests/run_regression_tests.py",
    tags = ["learning_tests_torch", "learning_tests_cartpole"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = glob(["tuned_examples/ppo/cartpole-ddppo.yaml"]),
    args = ["--yaml-dir=tuned_examples/ppo", "--framework=torch"]
)

# DQN/R2D2
py_test(
    name = "run_regression_tests_cartpole_dqn_tf",
    main = "tests/run_regression_tests.py",
    tags = ["learning_tests_tf", "learning_tests_cartpole"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = [
        "tuned_examples/dqn/cartpole-dqn.yaml",
        "tuned_examples/dqn/cartpole-dqn-softq.yaml",
        "tuned_examples/dqn/cartpole-dqn-param-noise.yaml",
    ],
    args = ["--yaml-dir=tuned_examples/dqn"]
)

py_test(
    name = "run_regression_tests_cartpole_dqn_torch",
    main = "tests/run_regression_tests.py",
    tags = ["learning_tests_torch", "learning_tests_cartpole"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = [
        "tuned_examples/dqn/cartpole-dqn.yaml",
        "tuned_examples/dqn/cartpole-dqn-softq.yaml",
        "tuned_examples/dqn/cartpole-dqn-param-noise.yaml",
    ],
    args = ["--yaml-dir=tuned_examples/dqn", "--framework=torch"]
)

# Simple-Q
py_test(
    name = "run_regression_tests_cartpole_simpleq_tf",
    main = "tests/run_regression_tests.py",
    tags = ["learning_tests_tf", "learning_tests_cartpole"],
    size = "medium",
    srcs = ["tests/run_regression_tests.py"],
    data = [
        "tuned_examples/dqn/cartpole-simpleq.yaml",
    ],
    args = ["--yaml-dir=tuned_examples/dqn"]
)

py_test(
    name = "run_regression_tests_cartpole_simpleq_torch",
    main = "tests/run_regression_tests.py",
    tags = ["learning_tests_torch", "learning_tests_cartpole"],
    size = "medium",
    srcs = ["tests/run_regression_tests.py"],
    data = [
        "tuned_examples/dqn/cartpole-simpleq.yaml",
    ],
    args = ["--yaml-dir=tuned_examples/dqn", "--framework=torch"]
)

# ES
py_test(
    name = "run_regression_tests_cartpole_es_tf",
    main = "tests/run_regression_tests.py",
    tags = ["learning_tests_tf", "learning_tests_cartpole"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/es/cartpole-es.yaml"],
    args = ["--yaml-dir=tuned_examples/es"]
)

py_test(
    name = "run_regression_tests_cartpole_es_torch",
    main = "tests/run_regression_tests.py",
    tags = ["learning_tests_torch", "learning_tests_cartpole"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/es/cartpole-es.yaml"],
    args = ["--yaml-dir=tuned_examples/es", "--framework=torch"]
)

# IMPALA
py_test(
    name = "run_regression_tests_cartpole_impala_tf",
    main = "tests/run_regression_tests.py",
    tags = ["learning_tests_tf", "learning_tests_cartpole"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/impala/cartpole-impala.yaml"],
    args = ["--yaml-dir=tuned_examples/impala"]
)

py_test(
    name = "run_regression_tests_cartpole_impala_torch",
    main = "tests/run_regression_tests.py",
    tags = ["learning_tests_torch", "learning_tests_cartpole"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/impala/cartpole-impala.yaml"],
    args = ["--yaml-dir=tuned_examples/impala", "--framework=torch"]
)

# Working, but takes a long time to learn (>15min).
# Removed due to Higher API conflicts with Pytorch-Import tests
## MB-MPO
#py_test(
#    name = "run_regression_tests_pendulum_mbmpo_torch",
#    main = "tests/run_regression_tests.py",
#    tags = ["learning_tests_torch", "learning_tests_pendulum"],
#    size = "large",
#    srcs = ["tests/run_regression_tests.py"],
#    data = ["tuned_examples/mbmpo/pendulum-mbmpo.yaml"],
#    args = ["--torch", "--yaml-dir=tuned_examples/mbmpo"]
#)

# PG
py_test(
    name = "run_regression_tests_cartpole_pg_tf",
    main = "tests/run_regression_tests.py",
    tags = ["learning_tests_tf", "learning_tests_cartpole"],
    size = "medium",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/pg/cartpole-pg.yaml"],
    args = ["--yaml-dir=tuned_examples/pg"]
)

py_test(
    name = "run_regression_tests_cartpole_pg_torch",
    main = "tests/run_regression_tests.py",
    tags = ["learning_tests_torch", "learning_tests_cartpole"],
    size = "medium",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/pg/cartpole-pg.yaml"],
    args = ["--yaml-dir=tuned_examples/pg", "--framework=torch"]
)

# PPO
py_test(
    name = "run_regression_tests_cartpole_ppo_tf",
    main = "tests/run_regression_tests.py",
    tags = ["learning_tests_tf", "learning_tests_cartpole"],
    size = "medium",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/ppo/cartpole-ppo.yaml"],
    args = ["--yaml-dir=tuned_examples/ppo"]
)

py_test(
    name = "run_regression_tests_cartpole_ppo_torch",
    main = "tests/run_regression_tests.py",
    tags = ["learning_tests_torch", "learning_tests_cartpole"],
    size = "medium",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/ppo/cartpole-ppo.yaml"],
    args = ["--yaml-dir=tuned_examples/ppo", "--framework=torch"]
)

py_test(
    name = "run_regression_tests_pendulum_ppo_tf",
    main = "tests/run_regression_tests.py",
    tags = ["learning_tests_tf", "learning_tests_pendulum"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/ppo/pendulum-ppo.yaml"],
    args = ["--yaml-dir=tuned_examples/ppo"]
)

py_test(
    name = "run_regression_tests_pendulum_ppo_torch",
    main = "tests/run_regression_tests.py",
    tags = ["learning_tests_torch", "learning_tests_pendulum"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/ppo/pendulum-ppo.yaml"],
    args = ["--yaml-dir=tuned_examples/ppo", "--framework=torch"]
)

py_test(
    name = "run_regression_tests_repeat_after_me_tf",
    main = "tests/run_regression_tests.py",
    tags = ["learning_tests_tf"],
    size = "medium",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/ppo/repeatafterme-ppo-lstm.yaml"],
    args = ["--yaml-dir=tuned_examples/ppo"]
)

py_test(
    name = "run_regression_tests_repeat_after_me_torch",
    main = "tests/run_regression_tests.py",
    tags = ["learning_tests_torch"],
    size = "medium",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/ppo/repeatafterme-ppo-lstm.yaml"],
    args = ["--yaml-dir=tuned_examples/ppo", "--framework=torch"]
)

# R2D2
py_test(
    name = "run_regression_tests_cartpole_r2d2_tf",
    main = "tests/run_regression_tests.py",
    tags = ["learning_tests_tf", "learning_tests_cartpole"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = [
        "tuned_examples/dqn/cartpole-r2d2.yaml",
    ],
    args = ["--yaml-dir=tuned_examples/dqn"]
)

py_test(
    name = "run_regression_tests_cartpole_r2d2_torch",
    main = "tests/run_regression_tests.py",
    tags = ["learning_tests_torch", "learning_tests_cartpole"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = [
        "tuned_examples/dqn/cartpole-r2d2.yaml",
    ],
    args = ["--yaml-dir=tuned_examples/dqn", "--framework=torch"]
)

# SAC
py_test(
    name = "run_regression_tests_cartpole_sac_tf",
    main = "tests/run_regression_tests.py",
    tags = ["learning_tests_tf", "learning_tests_cartpole"],
    size = "medium",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/sac/cartpole-sac.yaml"],
    args = ["--yaml-dir=tuned_examples/sac"]
)

py_test(
    name = "run_regression_tests_cartpole_continuous_pybullet_sac_tf",
    main = "tests/run_regression_tests.py",
    tags = ["learning_tests_tf", "learning_tests_cartpole"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/sac/cartpole-continuous-pybullet-sac.yaml"],
    args = ["--yaml-dir=tuned_examples/sac"]
)

py_test(
    name = "run_regression_tests_cartpole_sac_torch",
    main = "tests/run_regression_tests.py",
    tags = ["learning_tests_torch", "learning_tests_cartpole"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/sac/cartpole-sac.yaml"],
    args = ["--yaml-dir=tuned_examples/sac", "--framework=torch"]
)

py_test(
    name = "run_regression_tests_cartpole_continuous_pybullet_sac_torch",
    main = "tests/run_regression_tests.py",
    tags = ["learning_tests_torch", "learning_tests_cartpole"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/sac/cartpole-continuous-pybullet-sac.yaml"],
    args = ["--yaml-dir=tuned_examples/sac", "--framework=torch"]
)

py_test(
    name = "run_regression_tests_pendulum_sac_tf",
    main = "tests/run_regression_tests.py",
    tags = ["learning_tests_tf", "learning_tests_pendulum"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/sac/pendulum-sac.yaml"],
    args = ["--yaml-dir=tuned_examples/sac"]
)

py_test(
    name = "run_regression_tests_pendulum_sac_torch",
    main = "tests/run_regression_tests.py",
    tags = ["learning_tests_torch", "learning_tests_pendulum"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/sac/pendulum-sac.yaml"],
    args = ["--yaml-dir=tuned_examples/sac", "--framework=torch"]
)


# TD3
py_test(
    name = "run_regression_tests_pendulum_td3_tf",
    main = "tests/run_regression_tests.py",
    tags = ["learning_tests_tf", "learning_tests_pendulum"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/ddpg/pendulum-td3.yaml"],
    args = ["--yaml-dir=tuned_examples/ddpg"]
)

py_test(
    name = "run_regression_tests_pendulum_td3_torch",
    main = "tests/run_regression_tests.py",
    tags = ["learning_tests_torch", "learning_tests_pendulum"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/ddpg/pendulum-td3.yaml"],
    args = ["--yaml-dir=tuned_examples/ddpg", "--framework=torch"]
)


# --------------------------------------------------------------------
# Agents (Compilation, Losses, simple agent functionality tests)
# rllib/agents/
#
# Tag: agents_dir
# --------------------------------------------------------------------

# A2/3CTrainer
py_test(
    name = "test_a2c",
    tags = ["agents_dir"],
    size = "medium",
    srcs = ["agents/a3c/tests/test_a2c.py"]
)

py_test(
    name = "test_a3c",
    tags = ["agents_dir"],
    size = "medium",
    srcs = ["agents/a3c/tests/test_a3c.py"]
)

# APEXTrainer (DQN)
py_test(
    name = "test_apex_dqn",
    tags = ["agents_dir"],
    size = "medium",
    srcs = ["agents/dqn/tests/test_apex_dqn.py"]
)

# APEXDDPGTrainer
py_test(
    name = "test_apex_ddpg",
    tags = ["agents_dir"],
    size = "medium",
    srcs = ["agents/ddpg/tests/test_apex_ddpg.py"]
)

# ARS
py_test(
    name = "test_ars",
    tags = ["agents_dir"],
    size = "medium",
    srcs = ["agents/ars/tests/test_ars.py"]
)

# CQLTrainer
py_test(
    name = "test_cql",
    tags = ["agents_dir"],
    size = "medium",
    srcs = ["agents/cql/tests/test_cql.py"]
)

# DDPGTrainer
py_test(
    name = "test_ddpg",
    tags = ["agents_dir"],
    size = "large",
    srcs = ["agents/ddpg/tests/test_ddpg.py"]
)

# DQNTrainer/SimpleQTrainer
py_test(
    name = "test_dqn",
    tags = ["agents_dir"],
    size = "large",
    srcs = ["agents/dqn/tests/test_dqn.py"]
)
py_test(
    name = "test_simple_q",
    tags = ["agents_dir"],
    size = "medium",
    srcs = ["agents/dqn/tests/test_simple_q.py"]
)

# TODO: enable once we have a MuJoCo-independent test case.
## Dreamer
#py_test(
#    name = "test_dreamer",
#    tags = ["agents_dir"],
#    size = "small",
#    srcs = ["agents/dreamer/tests/test_dreamer.py"]
#)

# ES
py_test(
    name = "test_es",
    tags = ["agents_dir"],
    size = "medium",
    srcs = ["agents/es/tests/test_es.py"]
)

# IMPALA
py_test(
    name = "test_impala",
    tags = ["agents_dir"],
    size = "large",
    srcs = ["agents/impala/tests/test_impala.py"]
)
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
    size = "large",
    # Include the json data file.
    data = ["tests/data/cartpole/large.json"],
    srcs = ["agents/marwil/tests/test_marwil.py"]
)

# BCTrainer (sub-type of MARWIL)
py_test(
    name = "test_bc",
    tags = ["agents_dir"],
    size = "large",
    # Include the json data file.
    data = ["tests/data/cartpole/large.json"],
    srcs = ["agents/marwil/tests/test_bc.py"]
)

# MAMLTrainer
py_test(
    name = "test_maml",
    tags = ["agents_dir"],
    size = "medium",
    srcs = ["agents/maml/tests/test_maml.py"]
)

# MBMPOTrainer
py_test(
    name = "test_mbmpo",
    tags = ["agents_dir"],
    size = "medium",
    srcs = ["agents/mbmpo/tests/test_mbmpo.py"]
)

# PGTrainer
py_test(
    name = "test_pg",
    tags = ["agents_dir"],
    size = "medium",
    srcs = ["agents/pg/tests/test_pg.py"]
)

# PPOTrainer
py_test(
    name = "test_ppo",
    tags = ["agents_dir"],
    size = "large",
    srcs = ["agents/ppo/tests/test_ppo.py"]
)

# PPO: DDPPO
py_test(
    name = "test_ddppo",
    tags = ["agents_dir"],
    size = "small",
    srcs = ["agents/ppo/tests/test_ddppo.py"]
)

# PPO: APPO
py_test(
    name = "test_appo",
    tags = ["agents_dir"],
    size = "medium",
    srcs = ["agents/ppo/tests/test_appo.py"]
)

# QMixTrainer
py_test(
    name = "test_qmix",
    tags = ["agents_dir"],
    size = "medium",
    srcs = ["agents/qmix/tests/test_qmix.py"]
)

# SACTrainer
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
    name = "test_a3c_tf_cartpole_v1_lstm",
    main = "train.py", srcs = ["train.py"],
    tags = ["quick_train"],
    args = [
        "--env", "CartPole-v1",
        "--run", "A3C",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"framework\": \"tf\", \"num_workers\": 2, \"model\": {\"use_lstm\": true}}'",
        "--ray-num-cpus", "4"
        ]
)

py_test(
    name = "test_a3c_torch_pong_deterministic_v4",
    main = "train.py", srcs = ["train.py"],
    tags = ["quick_train"],
    args = [
        "--env", "PongDeterministic-v4",
        "--run", "A3C",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"framework\": \"torch\", \"num_workers\": 2, \"sample_async\": false, \"model\": {\"use_lstm\": false, \"grayscale\": true, \"zero_mean\": false, \"dim\": 84}, \"preprocessor_pref\": \"rllib\"}'",
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
        "--config", "'{\"framework\": \"tf\", \"num_workers\": 2}'",
        "--ray-num-cpus", "4"
        ]
)

# DDPG/APEX-DDPG/TD3

py_test(
    name = "test_ddpg_mountaincar_continuous_v0_num_workers_0",
    main = "train.py", srcs = ["train.py"],
    tags = ["quick_train"],
    args = [
        "--env", "MountainCarContinuous-v0",
        "--run", "DDPG",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"framework\": \"tf\", \"num_workers\": 0}'"
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
        "--config", "'{\"framework\": \"tf\", \"num_workers\": 1}'"
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
        "--config", "'{\"framework\": \"tf\", \"num_workers\": 2, \"optimizer\": {\"num_replay_buffer_shards\": 1}, \"learning_starts\": 100, \"min_iter_time_s\": 1, \"batch_mode\": \"complete_episodes\"}'",
        "--ray-num-cpus", "4",
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
        "--config", "'{\"framework\": \"tf\"}'",
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
        "--config", "'{\"framework\": \"tf\", \"lr\": 1e-3, \"exploration_config\": {\"epsilon_timesteps\": 10000, \"final_epsilon\": 0.02}, \"dueling\": false, \"hiddens\": [], \"model\": {\"fcnet_hiddens\": [64], \"fcnet_activation\": \"relu\"}}'"
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
        "--config", "'{\"framework\": \"tf\", \"num_workers\": 2}'",
        "--ray-num-cpus", "4"
        ]
)

py_test(
    name = "test_dqn_cartpole_v0_with_offline_input_and_softq",
    main = "train.py", srcs = ["train.py"],
    tags = ["quick_train", "external_files"],
    size = "small",
    # Include the json data file.
    data = ["tests/data/cartpole/small.json"],
    args = [
        "--env", "CartPole-v0",
        "--run", "DQN",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"framework\": \"tf\", \"input\": \"tests/data/cartpole\", \"learning_starts\": 0, \"input_evaluation\": [\"wis\", \"is\"], \"exploration_config\": {\"type\": \"SoftQ\"}}'"
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
        "--config", "'{\"framework\": \"tf\", \"lr\": 1e-4, \"exploration_config\": {\"epsilon_timesteps\": 200000, \"final_epsilon\": 0.01}, \"buffer_size\": 10000, \"rollout_fragment_length\": 4, \"learning_starts\": 10000, \"target_network_update_freq\": 1000, \"gamma\": 0.99, \"prioritized_replay\": true}'"
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
        "--config", "'{\"framework\": \"tf\", \"stepsize\": 0.01, \"episodes_per_batch\": 20, \"train_batch_size\": 100, \"num_workers\": 2}'",
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
        "--config", "'{\"framework\": \"tf\", \"stepsize\": 0.01, \"episodes_per_batch\": 20, \"train_batch_size\": 100, \"num_workers\": 2}'",
        "--ray-num-cpus", "4"
        ]
)

# IMPALA

py_test(
    name = "test_impala_buffers_2",
    main = "train.py", srcs = ["train.py"],
    tags = ["quick_train"],
    args = [
        "--env", "CartPole-v0",
        "--run", "IMPALA",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"framework\": \"tf\", \"num_gpus\": 0, \"num_workers\": 2, \"min_iter_time_s\": 1, \"num_data_loader_buffers\": 2, \"replay_buffer_num_slots\": 100, \"replay_proportion\": 1.0}'",
        "--ray-num-cpus", "4",
        ]
)

py_test(
    name = "test_impala_cartpole_v0_buffers_2_lstm",
    main = "train.py",
    srcs = ["train.py"],
    tags = ["quick_train"],
    args = [
        "--env", "CartPole-v0",
        "--run", "IMPALA",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"framework\": \"tf\", \"num_gpus\": 0, \"num_workers\": 2, \"min_iter_time_s\": 1, \"num_data_loader_buffers\": 2, \"replay_buffer_num_slots\": 100, \"replay_proportion\": 1.0, \"model\": {\"use_lstm\": true}}'",
        "--ray-num-cpus", "4",
        ]
)

py_test(
    name = "test_impala_pong_deterministic_v4_40k_ts_1G_obj_store",
    main = "train.py",
    srcs = ["train.py"],
    tags = ["quick_train"],
    size = "medium",
    args = [
        "--env", "PongDeterministic-v4",
        "--run", "IMPALA",
        "--stop", "'{\"timesteps_total\": 30000}'",
        "--ray-object-store-memory=1000000000",
        "--config", "'{\"framework\": \"tf\", \"num_workers\": 1, \"num_gpus\": 0, \"num_envs_per_worker\": 32, \"rollout_fragment_length\": 50, \"train_batch_size\": 50, \"learner_queue_size\": 1}'"
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
    data = ["tests/data/cartpole/small.json"],
    args = [
        "--env", "CartPole-v0",
        "--run", "MARWIL",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"framework\": \"tf\", \"input\": \"tests/data/cartpole\", \"learning_starts\": 0, \"input_evaluation\": [\"wis\", \"is\"], \"shuffle_buffer_size\": 10}'"
        ]
)

py_test(
    name = "test_marwil_cartpole_v0_torch",
    main = "train.py",
    srcs = ["train.py"],
    tags = ["quick_train", "external_files"],
    size = "small",
    # Include the json data file.
    data = ["tests/data/cartpole/small.json"],
    args = [
        "--env", "CartPole-v0",
        "--run", "MARWIL",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"framework\": \"torch\", \"input\": \"tests/data/cartpole\", \"learning_starts\": 0, \"input_evaluation\": [\"wis\", \"is\"], \"shuffle_buffer_size\": 10}'"
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
        "--config", "'{\"framework\": \"tf\", \"rollout_fragment_length\": 500, \"num_workers\": 1}'"
        ]
)

py_test(
    name = "test_pg_torch_frozenlake_v0",
    main = "train.py", srcs = ["train.py"],
    size = "small",
    tags = ["quick_train"],
    args = [
        "--env", "FrozenLake-v0",
        "--run", "PG",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"framework\": \"torch\", \"rollout_fragment_length\": 500, \"num_workers\": 1}'"
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
        "--config", "'{\"framework\": \"tf\", \"rollout_fragment_length\": 500, \"num_workers\": 1, \"model\": {\"use_lstm\": true, \"max_seq_len\": 100}}'"
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
        "--config", "'{\"framework\": \"tf\", \"rollout_fragment_length\": 500, \"num_workers\": 1, \"num_envs_per_worker\": 10}'"
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
        "--config", "'{\"framework\": \"tf\", \"rollout_fragment_length\": 500, \"num_workers\": 1}'"
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
        "--config", "'{\"framework\": \"tf\", \"num_sgd_iter\": 10, \"sgd_minibatch_size\": 64, \"train_batch_size\": 1000, \"num_workers\": 1}'"
        ]
)

py_test(
    name = "test_ppo_torch_frozenlake_v0",
    main = "train.py", srcs = ["train.py"],
    size = "small",
    tags = ["quick_train"],
    args = [
        "--env", "FrozenLake-v0",
        "--run", "PPO",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"framework\": \"torch\", \"num_sgd_iter\": 10, \"sgd_minibatch_size\": 64, \"train_batch_size\": 1000, \"num_workers\": 1}'"
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
        "--config", "'{\"framework\": \"tf\", \"simple_optimizer\": true, \"num_sgd_iter\": 2, \"model\": {\"use_lstm\": true}}'",
        "--ray-num-cpus", "4"
        ]
)

py_test(
    name = "test_ppo_tf_cartpole_v1_complete_episode_batches",
    main = "train.py", srcs = ["train.py"],
    tags = ["quick_train"],
    args = [
        "--env", "CartPole-v1",
        "--run", "PPO",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"framework\": \"tf\", \"kl_coeff\": 1.0, \"num_sgd_iter\": 10, \"lr\": 1e-4, \"sgd_minibatch_size\": 64, \"train_batch_size\": 2000, \"num_workers\": 1, \"use_gae\": false, \"batch_mode\": \"complete_episodes\"}'"
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
        "--config", "'{\"framework\": \"tf\", \"remote_worker_envs\": true, \"remote_env_batch_wait_ms\": 99999999, \"num_envs_per_worker\": 2, \"num_workers\": 1, \"train_batch_size\": 100, \"sgd_minibatch_size\": 50}'"
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
        "--config", "'{\"framework\": \"tf\", \"remote_worker_envs\": true, \"num_envs_per_worker\": 2, \"num_workers\": 1, \"train_batch_size\": 100, \"sgd_minibatch_size\": 50}'"
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
        "--config", "'{\"framework\": \"tf\", \"num_workers\": 2, \"num_gpus\": 0}'",
        "--ray-num-cpus", "4"
        ]
)

# --------------------------------------------------------------------
# Env tests
# rllib/env/
#
# Tag: env
# --------------------------------------------------------------------

sh_test(
    name = "env/tests/test_local_inference",
    tags = ["env"],
    size = "medium",
    srcs = ["env/tests/test_local_inference.sh"],
    data = glob(["examples/serving/*.py"]),
)

sh_test(
    name = "env/tests/test_remote_inference",
    tags = ["env"],
    size = "medium",
    srcs = ["env/tests/test_remote_inference.sh"],
    data = glob(["examples/serving/*.py"]),
)

py_test(
    name = "env/wrappers/tests/test_unity3d_env",
    tags = ["env"],
    size = "small",
    srcs = ["env/wrappers/tests/test_unity3d_env.py"]
)

py_test(
    name = "env/wrappers/tests/test_recsim_wrapper",
    tags = ["env"],
    size = "small",
    srcs = ["env/wrappers/tests/test_recsim_wrapper.py"]
)

py_test(
    name = "env/wrappers/tests/test_exception_wrapper",
    tags = ["env"],
    size = "small",
    srcs = ["env/wrappers/tests/test_exception_wrapper.py"]
)

py_test(
    name = "env/wrappers/tests/test_group_agents_wrapper",
    tags = ["env"],
    size = "small",
    srcs = ["env/wrappers/tests/test_group_agents_wrapper.py"]
)

# --------------------------------------------------------------------
# Models and Distributions
# rllib/models/
#
# Tag: models
# --------------------------------------------------------------------

py_test(
    name = "test_convtranspose2d_stack",
    tags = ["models"],
    size = "small",
    data = glob(["tests/data/images/obstacle_tower.png"]),
    srcs = ["models/tests/test_convtranspose2d_stack.py"]
)

py_test(
    name = "test_distributions",
    tags = ["models"],
    size = "medium",
    srcs = ["models/tests/test_distributions.py"]
)

py_test(
    name = "test_models",
    tags = ["models"],
    size = "small",
    srcs = ["models/tests/test_models.py"]
)

py_test(
    name = "test_preprocessors",
    tags = ["models"],
    size = "small",
    srcs = ["models/tests/test_preprocessors.py"]
)

# --------------------------------------------------------------------
# Evaluation components
# rllib/evaluation/
#
# Tag: evaluation
# --------------------------------------------------------------------
# mysteriously times out on travis.
#py_test(
#    name = "evaluation/tests/test_trajectory_view_api",
#    tags = ["evaluation"],
#    size = "medium",
#    srcs = ["evaluation/tests/test_trajectory_view_api.py"]
#)

py_test(
    name = "evaluation/tests/test_rollout_worker",
    tags = ["evaluation"],
    size = "medium",
    srcs = ["evaluation/tests/test_rollout_worker.py"]
)


# --------------------------------------------------------------------
# Optimizers and Memories
# rllib/execution/
#
# Tag: optimizers
# --------------------------------------------------------------------

py_test(
    name = "test_segment_tree",
    tags = ["optimizers"],
    size = "small",
    srcs = ["execution/tests/test_segment_tree.py"]
)

py_test(
    name = "test_prioritized_replay_buffer",
    tags = ["optimizers"],
    size = "small",
    srcs = ["execution/tests/test_prioritized_replay_buffer.py"]
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

py_test(
    name = "policy/tests/test_sample_batch",
    tags = ["policy"],
    size = "small",
    srcs = ["policy/tests/test_sample_batch.py"]
)

# --------------------------------------------------------------------
# Utils:
# rllib/utils/
#
# Tag: utils
# --------------------------------------------------------------------

py_test(
    name = "test_curiosity",
    tags = ["utils"],
    size = "large",
    srcs = ["utils/exploration/tests/test_curiosity.py"]
)

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
    name = "tests/test_attention_net_learning",
    tags = ["tests_dir", "tests_dir_A"],
    size = "large",
    srcs = ["tests/test_attention_net_learning.py"]
)

py_test(
    name = "tests/test_catalog",
    tags = ["tests_dir", "tests_dir_C"],
    size = "medium",
    srcs = ["tests/test_catalog.py"]
)

py_test(
    name = "tests/test_checkpoint_restore",
    tags = ["tests_dir", "tests_dir_C"],
    size = "enormous",
    srcs = ["tests/test_checkpoint_restore.py"]
)

py_test(
    name = "tests/test_dependency_tf",
    tags = ["tests_dir", "tests_dir_D"],
    size = "small",
    srcs = ["tests/test_dependency_tf.py"]
)

py_test(
    name = "tests/test_dependency_torch",
    tags = ["tests_dir", "tests_dir_D"],
    size = "small",
    srcs = ["tests/test_dependency_torch.py"]
)

py_test(
    name = "tests/test_eager_support_pg",
    main = "tests/test_eager_support.py",
    tags = ["tests_dir", "tests_dir_E"],
    size = "large",
    srcs = ["tests/test_eager_support.py"],
    args = ["TestEagerSupportPG"]
)

py_test(
    name = "tests/test_eager_support_off_policy",
    main = "tests/test_eager_support.py",
    tags = ["tests_dir", "tests_dir_E"],
    size = "large",
    srcs = ["tests/test_eager_support.py"],
    args = ["TestEagerSupportOffPolicy"]
)

py_test(
    name = "test_env_with_subprocess",
    main = "tests/test_env_with_subprocess.py",
    tags = ["tests_dir", "tests_dir_E"],
    size = "medium",
    srcs = ["tests/test_env_with_subprocess.py"]
)

py_test(
    name = "tests/test_evaluators",
    tags = ["tests_dir", "tests_dir_E"],
    size = "medium",
    srcs = ["tests/test_evaluators.py"]
)

py_test(
    name = "tests/test_exec_api",
    tags = ["tests_dir", "tests_dir_E"],
    size = "medium",
    srcs = ["tests/test_exec_api.py"]
)

py_test(
    name = "tests/test_execution",
    tags = ["tests_dir", "tests_dir_E"],
    size = "medium",
    srcs = ["tests/test_execution.py"]
)

py_test(
    name = "tests/test_export",
    tags = ["tests_dir", "tests_dir_E"],
    size = "medium",
    srcs = ["tests/test_export.py"]
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

#py_test(
#    name = "tests/test_ignore_worker_failure",
#    tags = ["tests_dir", "tests_dir_I"],
#    size = "large",
#    srcs = ["tests/test_ignore_worker_failure.py"]
#)

py_test(
    name = "tests/test_io",
    tags = ["tests_dir", "tests_dir_I"],
    size = "medium",
    srcs = ["tests/test_io.py"]
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
    size = "medium",
    srcs = ["tests/test_multi_agent_env.py"]
)

py_test(
    name = "tests/test_multi_agent_pendulum",
    tags = ["tests_dir", "tests_dir_M"],
    size = "large",
    srcs = ["tests/test_multi_agent_pendulum.py"]
)

py_test(
    name = "tests/test_nested_observation_spaces",
    main = "tests/test_nested_observation_spaces.py",
    tags = ["tests_dir", "tests_dir_N"],
    size = "small",
    srcs = ["tests/test_nested_observation_spaces.py"]
)

py_test(
    name = "tests/test_pettingzoo_env",
    tags = ["tests_dir", "tests_dir_P"],
    size = "medium",
    srcs = ["tests/test_pettingzoo_env.py"]
)

py_test(
    name = "tests/test_placement_groups",
    tags = ["tests_dir", "tests_dir_P"],
    size = "medium",
    srcs = ["tests/test_placement_groups.py"]
)

py_test(
    name = "tests/test_reproducibility",
    tags = ["tests_dir", "tests_dir_R"],
    size = "medium",
    srcs = ["tests/test_reproducibility.py"]
)

# Test train/rollout scripts (w/o confirming rollout performance).
py_test(
    name = "test_rollout_no_learning",
    main = "tests/test_rollout.py",
    tags = ["tests_dir", "tests_dir_R"],
    size = "large",
    data = ["train.py", "rollout.py"],
    srcs = ["tests/test_rollout.py"],
    args = ["TestRolloutSimple"]
)

# Test train/rollout scripts (and confirm `rllib rollout` performance is same
# as the final one from the `rllib train` run).
py_test(
    name = "test_rollout_w_learning",
    main = "tests/test_rollout.py",
    tags = ["tests_dir", "tests_dir_R"],
    size = "large",
    data = ["train.py", "rollout.py"],
    srcs = ["tests/test_rollout.py"],
    args = ["TestRolloutLearntPolicy"]
)

py_test(
    name = "tests/test_supported_multi_agent_pg",
    main = "tests/test_supported_multi_agent.py",
    tags = ["tests_dir", "tests_dir_S"],
    size = "medium",
    srcs = ["tests/test_supported_multi_agent.py"],
    args = ["TestSupportedMultiAgentPG"]
)

py_test(
    name = "tests/test_supported_multi_agent_off_policy",
    main = "tests/test_supported_multi_agent.py",
    tags = ["tests_dir", "tests_dir_S"],
    size = "medium",
    srcs = ["tests/test_supported_multi_agent.py"],
    args = ["TestSupportedMultiAgentOffPolicy"]
)

# py_test(
#     name = "tests/test_supported_spaces_pg",
#     main = "tests/test_supported_spaces.py",
#     tags = ["tests_dir", "tests_dir_S"],
#     size = "enormous",
#     srcs = ["tests/test_supported_spaces.py"],
#     args = ["TestSupportedSpacesPG"]
# )

py_test(
    name = "tests/test_supported_spaces_off_policy",
    main = "tests/test_supported_spaces.py",
    tags = ["tests_dir", "tests_dir_S"],
    size = "medium",
    srcs = ["tests/test_supported_spaces.py"],
    args = ["TestSupportedSpacesOffPolicy"]
)

py_test(
    name = "tests/test_supported_spaces_evolution_algos",
    main = "tests/test_supported_spaces.py",
    tags = ["tests_dir", "tests_dir_S"],
    size = "medium",
    srcs = ["tests/test_supported_spaces.py"],
    args = ["TestSupportedSpacesEvolutionAlgos"]
)

py_test(
    name = "tests/test_timesteps",
    tags = ["tests_dir", "tests_dir_T"],
    size = "small",
    srcs = ["tests/test_timesteps.py"]
)

py_test(
    name = "tests/test_trainer",
    tags = ["tests_dir", "tests_dir_T"],
    size = "small",
    srcs = ["tests/test_trainer.py"]
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
    name = "examples/attention_net_tf",
    main = "examples/attention_net.py",
    tags = ["examples", "examples_A"],
    size = "medium",
    srcs = ["examples/attention_net.py"],
    args = ["--as-test", "--stop-reward=80"]
)

py_test(
    name = "examples/attention_net_torch",
    main = "examples/attention_net.py",
    tags = ["examples", "examples_A"],
    size = "medium",
    srcs = ["examples/attention_net.py"],
    args = ["--as-test", "--stop-reward=80", "--framework torch"]
)

py_test(
    name = "examples/autoregressive_action_dist_tf",
    main = "examples/autoregressive_action_dist.py",
    tags = ["examples", "examples_A"],
    size = "medium",
    srcs = ["examples/autoregressive_action_dist.py"],
    args = ["--as-test", "--stop-reward=150", "--num-cpus=4"]
)

py_test(
    name = "examples/autoregressive_action_dist_torch",
    main = "examples/autoregressive_action_dist.py",
    tags = ["examples", "examples_A"],
    size = "medium",
    srcs = ["examples/autoregressive_action_dist.py"],
    args = ["--as-test", "--torch", "--stop-reward=150", "--num-cpus=4"]
)

py_test(
    name = "examples/batch_norm_model_ppo_tf",
    main = "examples/batch_norm_model.py",
    tags = ["examples", "examples_B"],
    size = "medium",
    srcs = ["examples/batch_norm_model.py"],
    args = ["--as-test", "--run=PPO", "--stop-reward=80"]
)

py_test(
    name = "examples/batch_norm_model_ppo_torch",
    main = "examples/batch_norm_model.py",
    tags = ["examples", "examples_B"],
    size = "medium",
    srcs = ["examples/batch_norm_model.py"],
    args = ["--as-test", "--torch", "--run=PPO", "--stop-reward=80"]
)

py_test(
    name = "examples/batch_norm_model_dqn_tf",
    main = "examples/batch_norm_model.py",
    tags = ["examples", "examples_B"],
    size = "medium",
    srcs = ["examples/batch_norm_model.py"],
    args = ["--as-test", "--run=DQN", "--stop-reward=70"]
)

py_test(
    name = "examples/batch_norm_model_dqn_torch",
    main = "examples/batch_norm_model.py",
    tags = ["examples", "examples_B"],
    size = "large",  # DQN learns much slower with BatchNorm.
    srcs = ["examples/batch_norm_model.py"],
    args = ["--as-test", "--torch", "--run=DQN", "--stop-reward=70"]
)

py_test(
    name = "examples/batch_norm_model_ddpg_tf",
    main = "examples/batch_norm_model.py",
    tags = ["examples", "examples_B"],
    size = "small",
    srcs = ["examples/batch_norm_model.py"],
    args = ["--run=DDPG", "--stop-iters=1"]
)

py_test(
    name = "examples/batch_norm_model_ddpg_torch",
    main = "examples/batch_norm_model.py",
    tags = ["examples", "examples_B"],
    size = "medium",
    srcs = ["examples/batch_norm_model.py"],
    args = ["--torch", "--run=DDPG", "--stop-iters=1"]
)

py_test(
    name = "examples/cartpole_lstm_impala_tf",
    main = "examples/cartpole_lstm.py",
    tags = ["examples", "examples_C"],
    size = "large",
    srcs = ["examples/cartpole_lstm.py"],
    args = ["--as-test", "--run=IMPALA", "--stop-reward=40", "--num-cpus=4"]
)

py_test(
    name = "examples/cartpole_lstm_impala_torch",
    main = "examples/cartpole_lstm.py",
    tags = ["examples", "examples_C"],
    size = "large",
    srcs = ["examples/cartpole_lstm.py"],
    args = ["--as-test", "--framework=torch", "--run=IMPALA", "--stop-reward=40", "--num-cpus=4"]
)

py_test(
    name = "examples/cartpole_lstm_ppo_tf",
    main = "examples/cartpole_lstm.py",
    tags = ["examples", "examples_C"],
    size = "large",
    srcs = ["examples/cartpole_lstm.py"],
    args = ["--as-test", "--framework=tf", "--run=PPO", "--stop-reward=40", "--num-cpus=4"]
)

py_test(
    name = "examples/cartpole_lstm_ppo_tf2",
    main = "examples/cartpole_lstm.py",
    tags = ["examples", "examples_C"],
    size = "large",
    srcs = ["examples/cartpole_lstm.py"],
    args = ["--as-test", "--framework=tf2", "--run=PPO", "--stop-reward=40", "--num-cpus=4"]
)

py_test(
    name = "examples/cartpole_lstm_ppo_torch",
    main = "examples/cartpole_lstm.py",
    tags = ["examples", "examples_C"],
    size = "large",
    srcs = ["examples/cartpole_lstm.py"],
    args = ["--as-test", "--framework=torch", "--run=PPO", "--stop-reward=40", "--num-cpus=4"]
)

py_test(
    name = "examples/cartpole_lstm_ppo_tf_with_prev_a_and_r",
    main = "examples/cartpole_lstm.py",
    tags = ["examples", "examples_C"],
    size = "large",
    srcs = ["examples/cartpole_lstm.py"],
    args = ["--as-test", "--run=PPO", "--stop-reward=40", "--use-prev-action",  "--use-prev-reward", "--num-cpus=4"]
)

py_test(
    name = "examples/centralized_critic_tf",
    main = "examples/centralized_critic.py",
    tags = ["examples", "examples_C"],
    size = "large",
    srcs = ["examples/centralized_critic.py"],
    args = ["--as-test", "--stop-reward=7.2"]
)

py_test(
    name = "examples/centralized_critic_torch",
    main = "examples/centralized_critic.py",
    tags = ["examples", "examples_C"],
    size = "large",
    srcs = ["examples/centralized_critic.py"],
    args = ["--as-test", "--torch", "--stop-reward=7.2"]
)

py_test(
    name = "examples/centralized_critic_2_tf",
    main = "examples/centralized_critic_2.py",
    tags = ["examples", "examples_C"],
    size = "medium",
    srcs = ["examples/centralized_critic_2.py"],
    args = ["--as-test", "--stop-reward=6.0"]
)

py_test(
    name = "examples/centralized_critic_2_torch",
    main = "examples/centralized_critic_2.py",
    tags = ["examples", "examples_C"],
    size = "medium",
    srcs = ["examples/centralized_critic_2.py"],
    args = ["--as-test", "--torch", "--stop-reward=6.0"]
)

py_test(
    name = "examples/checkpoint_by_custom_criteria",
    main = "examples/checkpoint_by_custom_criteria.py",
    tags = ["examples", "examples_C"],
    size = "medium",
    srcs = ["examples/checkpoint_by_custom_criteria.py"],
    args = ["--stop-iters=3 --num-cpus=3"]
)

py_test(
    name = "examples/complex_struct_space_tf", main = "examples/complex_struct_space.py",
    tags = ["examples", "examples_C"],
    size = "medium",
    srcs = ["examples/complex_struct_space.py"],
    args = ["--framework=tf"],
)

py_test(
    name = "examples/complex_struct_space_tf_eager", main = "examples/complex_struct_space.py",
    tags = ["examples", "examples_C"],
    size = "medium",
    srcs = ["examples/complex_struct_space.py"],
    args = ["--framework=tfe"],
)

py_test(
    name = "examples/complex_struct_space_torch", main = "examples/complex_struct_space.py",
    tags = ["examples", "examples_C"],
    size = "medium",
    srcs = ["examples/complex_struct_space.py"],
    args = ["--framework=torch"],
)

py_test(
    name = "examples/custom_env_tf",
    main = "examples/custom_env.py",
    tags = ["examples", "examples_C"],
    size = "medium",
    srcs = ["examples/custom_env.py"],
    args = ["--as-test"]
)

py_test(
    name = "examples/custom_env_torch",
    main = "examples/custom_env.py",
    tags = ["examples", "examples_C"],
    size = "medium",
    srcs = ["examples/custom_env.py"],
    args = ["--as-test", "--torch"]
)

py_test(
    name = "examples/custom_eval_tf",
    main = "examples/custom_eval.py",
    tags = ["examples", "examples_C"],
    size = "medium",
    srcs = ["examples/custom_eval.py"],
    args = ["--num-cpus=4", "--as-test"]
)

py_test(
    name = "examples/custom_eval_torch",
    main = "examples/custom_eval.py",
    tags = ["examples", "examples_C"],
    size = "medium",
    srcs = ["examples/custom_eval.py"],
    args = ["--num-cpus=4", "--as-test", "--torch"]
)

py_test(
    name = "examples/custom_fast_model_tf",
    main = "examples/custom_fast_model.py",
    tags = ["examples", "examples_C"],
    size = "small",
    srcs = ["examples/custom_fast_model.py"],
    args = ["--stop-iters=1", "--num-cpus=4"]
)

py_test(
    name = "examples/custom_fast_model_torch",
    main = "examples/custom_fast_model.py",
    tags = ["examples", "examples_C"],
    size = "small",
    srcs = ["examples/custom_fast_model.py"],
    args = ["--torch", "--stop-iters=1", "--num-cpus=4"]
)

py_test(
    name = "examples/custom_keras_model_a2c",
    main = "examples/custom_keras_model.py",
    tags = ["examples", "examples_C"],
    size = "large",
    srcs = ["examples/custom_keras_model.py"],
    args = ["--run=A2C", "--stop=50", "--num-cpus=4"]
)

py_test(
    name = "examples/custom_keras_model_dqn",
    main = "examples/custom_keras_model.py",
    tags = ["examples", "examples_C"],
    size = "medium",
    srcs = ["examples/custom_keras_model.py"],
    args = ["--run=DQN", "--stop=50"]
)

py_test(
    name = "examples/custom_keras_model_ppo",
    main = "examples/custom_keras_model.py",
    tags = ["examples", "examples_C"],
    size = "medium",
    srcs = ["examples/custom_keras_model.py"],
    args = ["--run=PPO", "--stop=50", "--num-cpus=4"]
)

py_test(
    name = "examples/custom_loss_tf",
    main = "examples/custom_loss.py",
    tags = ["examples", "examples_C"],
    size = "small",
    # Include the json data file.
    data = ["tests/data/cartpole/small.json"],
    srcs = ["examples/custom_loss.py"],
    args = ["--stop-iters=2", "--input-files=tests/data/cartpole"]
)

py_test(
    name = "examples/custom_loss_torch",
    main = "examples/custom_loss.py",
    tags = ["examples", "examples_C"],
    size = "small",
    # Include the json data file.
    data = ["tests/data/cartpole/small.json"],
    srcs = ["examples/custom_loss.py"],
    args = ["--torch", "--stop-iters=2", "--input-files=tests/data/cartpole"]
)

py_test(
    name = "examples/custom_metrics_and_callbacks",
    main = "examples/custom_metrics_and_callbacks.py",
    tags = ["examples", "examples_C"],
    size = "small",
    srcs = ["examples/custom_metrics_and_callbacks.py"],
    args = ["--stop-iters=2"]
)

py_test(
    name = "examples/custom_metrics_and_callbacks_legacy",
    main = "examples/custom_metrics_and_callbacks_legacy.py",
    tags = ["examples", "examples_C"],
    size = "small",
    srcs = ["examples/custom_metrics_and_callbacks_legacy.py"],
    args = ["--stop-iters=2"]
)

py_test(
    name = "examples/custom_model_api_tf",
    main = "examples/custom_model_api.py",
    tags = ["examples", "examples_C"],
    size = "small",
    srcs = ["examples/custom_model_api.py"],
)

py_test(
    name = "examples/custom_model_api_torch",
    main = "examples/custom_model_api.py",
    tags = ["examples", "examples_C"],
    size = "small",
    srcs = ["examples/custom_model_api.py"],
    args = ["--framework=torch"],
)

py_test(
    name = "examples/custom_observation_filters",
    main = "examples/custom_observation_filters.py",
    tags = ["examples", "examples_C"],
    size = "small",
    srcs = ["examples/custom_observation_filters.py"],
    args = ["--stop-iters=2"]
)

py_test(
    name = "examples/custom_rnn_model_repeat_after_me_tf",
    main = "examples/custom_rnn_model.py",
    tags = ["examples", "examples_C"],
    size = "medium",
    srcs = ["examples/custom_rnn_model.py"],
    args = ["--as-test", "--run=PPO", "--stop-reward=40", "--env=RepeatAfterMeEnv", "--num-cpus=4"]
)

py_test(
    name = "examples/custom_rnn_model_repeat_initial_obs_tf",
    main = "examples/custom_rnn_model.py",
    tags = ["examples", "examples_C"],
    size = "medium",
    srcs = ["examples/custom_rnn_model.py"],
    args = ["--as-test", "--run=PPO", "--stop-reward=10", "--stop-timesteps=300000", "--env=RepeatInitialObsEnv", "--num-cpus=4"]
)

py_test(
    name = "examples/custom_rnn_model_repeat_after_me_torch",
    main = "examples/custom_rnn_model.py",
    tags = ["examples", "examples_C"],
    size = "medium",
    srcs = ["examples/custom_rnn_model.py"],
    args = ["--as-test", "--torch", "--run=PPO", "--stop-reward=40", "--env=RepeatAfterMeEnv", "--num-cpus=4"]
)

py_test(
    name = "examples/custom_rnn_model_repeat_initial_obs_torch",
    main = "examples/custom_rnn_model.py",
    tags = ["examples", "examples_C"],
    size = "medium",
    srcs = ["examples/custom_rnn_model.py"],
    args = ["--as-test", "--torch", "--run=PPO", "--stop-reward=10", "--stop-timesteps=300000", "--env=RepeatInitialObsEnv", "--num-cpus=4"]
)

py_test(
    name = "examples/custom_tf_policy",
    tags = ["examples", "examples_C"],
    size = "medium",
    srcs = ["examples/custom_tf_policy.py"],
    args = ["--stop-iters=2", "--num-cpus=4"]
)

py_test(
    name = "examples/custom_torch_policy",
    tags = ["examples", "examples_C"],
    size = "small",
    srcs = ["examples/custom_torch_policy.py"],
    args = ["--stop-iters=2", "--num-cpus=4"]
)

py_test(
    name = "examples/eager_execution",
    tags = ["examples", "examples_E"],
    size = "small",
    srcs = ["examples/eager_execution.py"],
    args = ["--stop-iters=2"]
)

py_test(
    name = "examples/hierarchical_training_tf",
    main = "examples/hierarchical_training.py",
    tags = ["examples", "examples_H"],
    size = "medium",
    srcs = ["examples/hierarchical_training.py"],
    args = ["--stop-reward=0.0"]
)

py_test(
    name = "examples/hierarchical_training_torch",
    main = "examples/hierarchical_training.py",
    tags = ["examples", "examples_H"],
    size = "medium",
    srcs = ["examples/hierarchical_training.py"],
    args = ["--torch", "--stop-reward=0.0"]
)

# Do not run this test (MobileNetV2 is gigantic and takes forever for 1 iter).
# py_test(
#     name = "examples/mobilenet_v2_with_lstm_tf",
#     main = "examples/mobilenet_v2_with_lstm.py",
#     tags = ["examples", "examples_M"],
#     size = "small",
#     srcs = ["examples/mobilenet_v2_with_lstm.py"]
# )

py_test(
    name = "examples/multi_agent_cartpole_tf",
    main = "examples/multi_agent_cartpole.py",
    tags = ["examples", "examples_M"],
    size = "medium",
    srcs = ["examples/multi_agent_cartpole.py"],
    args = ["--as-test", "--stop-reward=70.0", "--num-cpus=4"]
)

py_test(
    name = "examples/multi_agent_cartpole_torch",
    main = "examples/multi_agent_cartpole.py",
    tags = ["examples", "examples_M"],
    size = "medium",
    srcs = ["examples/multi_agent_cartpole.py"],
    args = ["--as-test", "--framework=torch", "--stop-reward=70.0", "--num-cpus=4"]
)

py_test(
    name = "examples/multi_agent_custom_policy_tf",
    main = "examples/multi_agent_custom_policy.py",
    tags = ["examples", "examples_M"],
    size = "small",
    srcs = ["examples/multi_agent_custom_policy.py"],
    args = ["--as-test", "--stop-reward=80"]
)

py_test(
    name = "examples/multi_agent_custom_policy_torch",
    main = "examples/multi_agent_custom_policy.py",
    tags = ["examples", "examples_M"],
    size = "small",
    srcs = ["examples/multi_agent_custom_policy.py"],
    args = ["--as-test", "--torch", "--stop-reward=80"]
)

py_test(
    name = "examples/multi_agent_two_trainers_tf",
    main = "examples/multi_agent_two_trainers.py",
    tags = ["examples", "examples_M"],
    size = "medium",
    srcs = ["examples/multi_agent_two_trainers.py"],
    args = ["--as-test", "--stop-reward=70"]
)

py_test(
    name = "examples/multi_agent_two_trainers_torch",
    main = "examples/multi_agent_two_trainers.py",
    tags = ["examples", "examples_M"],
    size = "medium",
    srcs = ["examples/multi_agent_two_trainers.py"],
    args = ["--as-test", "--torch", "--stop-reward=70"]
)

# Taking out this test for now: Mixed torch- and tf- policies within the same
# Trainer never really worked.
# py_test(
#     name = "examples/multi_agent_two_trainers_mixed_torch_tf",
#     main = "examples/multi_agent_two_trainers.py",
#     tags = ["examples", "examples_M"],
#     size = "medium",
#     srcs = ["examples/multi_agent_two_trainers.py"],
#     args = ["--as-test", "--mixed-torch-tf", "--stop-reward=70"]
# )

py_test(
    name = "examples/nested_action_spaces_ppo_tf",
    main = "examples/nested_action_spaces.py",
    tags = ["examples", "examples_N"],
    size = "medium",
    srcs = ["examples/nested_action_spaces.py"],
    args = ["--as-test", "--stop-reward=-600", "--run=PPO"]
)

py_test(
    name = "examples/nested_action_spaces_ppo_torch",
    main = "examples/nested_action_spaces.py",
    tags = ["examples", "examples_N"],
    size = "medium",
    srcs = ["examples/nested_action_spaces.py"],
    args = ["--as-test", "--torch", "--stop-reward=-600", "--run=PPO"]
)

py_test(
    name = "examples/parametric_actions_cartpole_pg_tf",
    main = "examples/parametric_actions_cartpole.py",
    tags = ["examples", "examples_P"],
    size = "medium",
    srcs = ["examples/parametric_actions_cartpole.py"],
    args = ["--as-test", "--stop-reward=60.0", "--run=PG"]
)

py_test(
    name = "examples/parametric_actions_cartpole_dqn_tf",
    main = "examples/parametric_actions_cartpole.py",
    tags = ["examples", "examples_P"],
    size = "medium",
    srcs = ["examples/parametric_actions_cartpole.py"],
    args = ["--as-test", "--stop-reward=60.0", "--run=DQN"]
)

py_test(
    name = "examples/parametric_actions_cartpole_pg_torch",
    main = "examples/parametric_actions_cartpole.py",
    tags = ["examples", "examples_P"],
    size = "small",
    srcs = ["examples/parametric_actions_cartpole.py"],
    args = ["--as-test", "--torch", "--stop-reward=60.0", "--run=PG"]
)

py_test(
    name = "examples/parametric_actions_cartpole_dqn_torch",
    main = "examples/parametric_actions_cartpole.py",
    tags = ["examples", "examples_P"],
    size = "medium",
    srcs = ["examples/parametric_actions_cartpole.py"],
    args = ["--as-test", "--torch", "--stop-reward=60.0", "--run=DQN"]
)

py_test(
    name = "examples/rollout_worker_custom_workflow",
    tags = ["examples", "examples_R"],
    size = "small",
    srcs = ["examples/rollout_worker_custom_workflow.py"],
    args = ["--num-cpus=4"]
)

py_test(
    name = "examples/rock_paper_scissors_multiagent_tf",
    main = "examples/rock_paper_scissors_multiagent.py",
    tags = ["examples", "examples_R"],
    size = "medium",
    srcs = ["examples/rock_paper_scissors_multiagent.py"],
    args = ["--as-test"],
)

py_test(
    name = "examples/rock_paper_scissors_multiagent_torch",
    main = "examples/rock_paper_scissors_multiagent.py",
    tags = ["examples", "examples_R"],
    size = "medium",
    srcs = ["examples/rock_paper_scissors_multiagent.py"],
    args = ["--as-test", "--torch"],
)

py_test(
    name = "examples/trajectory_view_api_tf",
    main = "examples/trajectory_view_api.py",
    tags = ["examples", "examples_T"],
    size = "medium",
    srcs = ["examples/trajectory_view_api.py"],
    args = ["--as-test", "--framework=tf", "--stop-reward=100.0"]
)

py_test(
    name = "examples/trajectory_view_api_torch",
    main = "examples/trajectory_view_api.py",
    tags = ["examples", "examples_T"],
    size = "medium",
    srcs = ["examples/trajectory_view_api.py"],
    args = ["--as-test", "--framework=torch", "--stop-reward=100.0"]
)

py_test(
    name = "examples/two_trainer_workflow_tf",
    main = "examples/two_trainer_workflow.py",
    tags = ["examples", "examples_T"],
    size = "small",
    srcs = ["examples/two_trainer_workflow.py"],
    args = ["--as-test", "--stop-reward=100.0"]
)

py_test(
    name = "examples/two_trainer_workflow_torch",
    main = "examples/two_trainer_workflow.py",
    tags = ["examples", "examples_T"],
    size = "small",
    srcs = ["examples/two_trainer_workflow.py"],
    args = ["--as-test", "--torch", "--stop-reward=100.0"]
)

py_test(
    name = "examples/two_trainer_workflow_mixed_torch_tf",
    main = "examples/two_trainer_workflow.py",
    tags = ["examples", "examples_T"],
    size = "small",
    srcs = ["examples/two_trainer_workflow.py"],
    args = ["--as-test", "--mixed-torch-tf", "--stop-reward=100.0"]
)

py_test(
    name = "examples/two_step_game_maddpg",
    main = "examples/two_step_game.py",
    tags = ["examples", "examples_T"],
    size = "medium",
    srcs = ["examples/two_step_game.py"],
    args = ["--as-test", "--stop-reward=7.1", "--run=contrib/MADDPG"]
)

py_test(
    name = "examples/two_step_game_pg_tf",
    main = "examples/two_step_game.py",
    tags = ["examples", "examples_T"],
    size = "medium",
    srcs = ["examples/two_step_game.py"],
    args = ["--as-test", "--stop-reward=7", "--run=PG"]
)

py_test(
    name = "examples/two_step_game_pg_torch",
    main = "examples/two_step_game.py",
    tags = ["examples", "examples_T"],
    size = "medium",
    srcs = ["examples/two_step_game.py"],
    args = ["--as-test", "--torch", "--stop-reward=7", "--run=PG"]
)

py_test(
    name = "examples/two_step_game_qmix",
    main = "examples/two_step_game.py",
    tags = ["examples", "examples_T"],
    size = "medium",
    srcs = ["examples/two_step_game.py"],
    args = ["--as-test", "--torch", "--stop-reward=7", "--run=QMIX"]
)

py_test(
    name = "contrib/bandits/examples/lin_ts",
    main = "contrib/bandits/examples/simple_context_bandit.py",
    tags = ["examples", "examples_T"],
    size = "small",
    srcs = ["contrib/bandits/examples/simple_context_bandit.py"],
    args = ["--as-test", "--stop-reward=10", "--run=contrib/LinTS"],
)

py_test(
    name = "contrib/bandits/examples/lin_ucb",
    main = "contrib/bandits/examples/simple_context_bandit.py",
    tags = ["examples", "examples_U"],
    size = "small",
    srcs = ["contrib/bandits/examples/simple_context_bandit.py"],
    args = ["--as-test", "--stop-reward=10", "--run=contrib/LinUCB"],
)
