# --------------------------------------------------------------------
# BAZEL/Buildkite-CI test cases.
# --------------------------------------------------------------------

# To add new RLlib tests, first find the correct category of your new test
# within this file.

# All new tests - within their category - should be added alphabetically!
# Do not just add tests to the bottom of the file.

# Currently we have the following categories:

# - Learning tests/regression, tagged:
# -- "learning_tests_[discrete|continuous]": distinguish discrete
#    actions vs continuous actions.
# -- "crashing_cartpole" and "stateless_cartpole" to distinguish between
#    simple CartPole and more advanced variants of it.
# -- "fake_gpus": Tests that run using 2 fake GPUs.

# - Quick algo compilation/tune-train tests, tagged "quick_train".
#   NOTE: These should be obsoleted in favor of "algorithms_dir" tests as
#         they cover the same functionality.

# - Folder-bound tests, tagged with the name of the top-level dir:
#   - `env` directory tests.
#   - `evaluation` directory tests.
#   - `execution` directory tests.
#   - `models` directory tests.
#   - `offline` directory tests.
#   - `policy` directory tests.
#   - `utils` directory tests.

# - Algorithm tests, tagged "algorithms_dir".

# - Tests directory (everything in rllib/tests/...), tagged: "tests_dir"

# - Examples directory (everything in rllib/examples/...), tagged: "examples"

# - Memory leak tests tagged "memory_leak_tests".

# Note: There is a special directory in examples: "documentation" which contains
# all code that is linked to from within the RLlib docs. This code is tested
# separately via the "documentation" tag.

# Additional tags are:
# - "team:rllib": Indicating that all tests in this file are the responsibility of
#   the RLlib Team.
# - "needs_gpu": Indicating that a test needs to have a GPU in order to run.
# - "gpu": Indicating that a test may (but doesn't have to) be run in the GPU
#   pipeline, defined in .buildkite/pipeline.gpu.yml.
# - "multi-gpu": Indicating that a test will definitely be run in the Large GPU
#   pipeline, defined in .buildkite/pipeline.gpu.large.yml.
# - "no_gpu": Indicating that a test should not be run in the GPU pipeline due
#   to certain incompatibilities.
# - "no_tf_eager_tracing": Exclude this test from tf-eager tracing tests.
# - "torch_only": Only run this test case with framework=torch.

# Our .buildkite/pipeline.yml and .buildkite/pipeline.gpu.yml files execute all
# these tests in n different jobs.

load("//bazel:python.bzl", "py_test_module_list")

# --------------------------------------------------------------------
# Algorithms learning regression tests.
#
# Tag: learning_tests
#
# This will test all yaml files (via `rllib train`)
# inside rllib/tuned_examples/[algo-name] for actual learning success.
# --------------------------------------------------------------------

# A2C
# py_test(
#    name = "learning_tests_cartpole_a2c",
#    main = "tests/run_regression_tests.py",
#    tags = ["team:rllib", "", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete"],
#    size = "large",
#    srcs = ["tests/run_regression_tests.py"],
#    data = ["tuned_examples/a2c/cartpole-a2c.yaml"],
#    args = ["--dir=tuned_examples/a2c"]
# )

py_test(
    name = "learning_tests_cartpole_a2c_microbatch",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/a2c/cartpole-a2c-microbatch.yaml"],
    args = ["--dir=tuned_examples/a2c"]
)

py_test(
    name = "learning_tests_cartpole_a2c_fake_gpus",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete", "fake_gpus"],
    size = "medium",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/a2c/cartpole-a2c-fake-gpus.yaml"],
    args = ["--dir=tuned_examples/a2c"]
)

# A3C

# py_test(
#    name = "learning_tests_cartpole_a3c",
#    main = "tests/run_regression_tests.py",
#    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete"],
#    size = "large",
#    srcs = ["tests/run_regression_tests.py"],
#    data = ["tuned_examples/a3c/cartpole-a3c.yaml"],
#    args = ["--dir=tuned_examples/a3c"]
# )

# AlphaStar
py_test(
    name = "learning_tests_cartpole_alpha_star",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete"],
    size = "medium",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/alpha_star/multi-agent-cartpole-alpha-star.yaml"],
    args = ["--dir=tuned_examples/alpha_star", "--num-cpus=10"]
)

# AlphaZero
py_test(
    name = "learning_tests_cartpole_sparse_rewards_alpha_zero",
    tags = ["team:rllib", "exclusive", "torch_only", "learning_tests", "learning_tests_discrete"],
    main = "tests/run_regression_tests.py",
    size = "medium",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/alpha_zero/cartpole-sparse-rewards-alpha-zero.yaml"],
    args = ["--dir=tuned_examples/alpha_zero", "--num-cpus=8"]
)

# APEX-DQN
# py_test(
#    name = "learning_tests_cartpole_apex",
#    main = "tests/run_regression_tests.py",
#    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete"],
#    size = "large",
#    srcs = ["tests/run_regression_tests.py"],
#    data = [
#        "tuned_examples/apex_dqn/cartpole-apex.yaml",
#    ],
#    args = ["--dir=tuned_examples/apex_dqn", "--num-cpus=6"]
# )

# Once APEX supports multi-GPU.
# py_test(
#    name = "learning_cartpole_apex_fake_gpus",
#    main = "tests/run_regression_tests.py",
#    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete", "fake_gpus"],
#    size = "large",
#    srcs = ["tests/run_regression_tests.py"],
#    data = ["tuned_examples/apex_dqn/cartpole-apex-fake-gpus.yaml"],
#    args = ["--dir=tuned_examples/apex_dqn"]
# )

# APPO
py_test(
    name = "learning_tests_cartpole_appo_no_vtrace",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete"],
    size = "medium", # bazel may complain about it being too long sometimes - medium is on purpose as some frameworks take longer
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/appo/cartpole-appo.yaml"],
    args = ["--dir=tuned_examples/appo"]
)

# py_test(
#    name = "learning_tests_cartpole_appo_vtrace",
#    main = "tests/run_regression_tests.py",
#    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete"],
#    size = "large",
#    srcs = ["tests/run_regression_tests.py"],
#    data = ["tuned_examples/appo/cartpole-appo-vtrace.yaml"],
#    args = ["--dir=tuned_examples/appo"]
# )

py_test(
    name = "learning_tests_cartpole_separate_losses_appo",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "tf_only", "exclusive", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete"],
    size = "medium",
    srcs = ["tests/run_regression_tests.py"],
    data = [
        "tuned_examples/appo/cartpole-appo-vtrace-separate-losses.yaml"
    ],
    args = ["--dir=tuned_examples/appo"]
)

py_test(
    name = "learning_tests_multi_agent_cartpole_appo",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete"],
    size = "medium",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/appo/multi-agent-cartpole-appo.yaml"],
    args = ["--dir=tuned_examples/appo"]
)

py_test(
    name = "learning_tests_multi_agent_cartpole_w_100_policies_appo",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/appo/multi-agent-cartpole-w-100-policies-appo.py"],
    args = ["--dir=tuned_examples/appo"]
)

# py_test(
#    name = "learning_tests_frozenlake_appo",
#    main = "tests/run_regression_tests.py",
#    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_discrete"],
#    size = "large",
#    srcs = ["tests/run_regression_tests.py"],
#    data = ["tuned_examples/appo/frozenlake-appo-vtrace.yaml"],
#    args = ["--dir=tuned_examples/appo"]
# )

py_test(
    name = "learning_tests_cartpole_appo_fake_gpus",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete", "fake_gpus"],
    size = "medium",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/appo/cartpole-appo-vtrace-fake-gpus.yaml"],
    args = ["--dir=tuned_examples/appo"]
)

# ARS
py_test(
    name = "learning_tests_cartpole_ars",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete"],
    size = "medium",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/ars/cartpole-ars.yaml"],
    args = ["--dir=tuned_examples/ars"]
)

# CQL
py_test(
    name = "learning_tests_pendulum_cql",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_pendulum", "learning_tests_continuous"],
    size = "medium",
    srcs = ["tests/run_regression_tests.py"],
    # Include the zipped json data file as well.
    data = [
        "tuned_examples/cql/pendulum-cql.yaml",
        "tests/data/pendulum/enormous.zip",
    ],
    args = ["--dir=tuned_examples/cql"]
)


# CRR
py_test(
   name = "learning_tests_pendulum_crr",
   main = "tests/run_regression_tests.py",
   tags = ["team:rllib", "torch_only", "learning_tests", "learning_tests_pendulum", "learning_tests_continuous"],
   size = "large",
   srcs = ["tests/run_regression_tests.py"],
   # Include an offline json data file as well.
   data = [
       "tuned_examples/crr/pendulum-v1-crr.yaml",
       "tests/data/pendulum/pendulum_replay_v1.1.0.zip",
   ],
   args = ["--dir=tuned_examples/crr"]
)

py_test(
    name = "learning_tests_cartpole_crr",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "torch_only", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete"],
    size = "medium",
    srcs = ["tests/run_regression_tests.py"],
    # Include an offline json data file as well.
    data = [
        "tuned_examples/crr/cartpole-v1-crr.yaml",
        "tests/data/cartpole/large.json",
    ],
    args = ["--dir=tuned_examples/crr", '--framework=torch']
)

py_test(
    name = "learning_tests_cartpole_crr_expectation",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "torch_only", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    # Include an offline json data file as well.
    data = [
        "tuned_examples/crr/cartpole-v1-crr_expectation.yaml",
        "tests/data/cartpole/large.json",
    ],
    args = ["--dir=tuned_examples/crr", '--framework=torch']
)

# DDPG
# py_test(
#   name = "learning_tests_pendulum_ddpg",
#   main = "tests/run_regression_tests.py",
#   tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_pendulum", "learning_tests_continuous"],
#   size = "large",
#   srcs = ["tests/run_regression_tests.py"],
#   data = glob(["tuned_examples/ddpg/pendulum-ddpg.yaml"]),
#   args = ["--dir=tuned_examples/ddpg"]
# )

py_test(
   name = "learning_tests_pendulum_ddpg_fake_gpus",
   main = "tests/run_regression_tests.py",
   tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_pendulum", "learning_tests_continuous", "fake_gpus"],
   size = "large",
   srcs = ["tests/run_regression_tests.py"],
   data = ["tuned_examples/ddpg/pendulum-ddpg-fake-gpus.yaml"],
   args = ["--dir=tuned_examples/ddpg"]
)

# DDPPO
py_test(
    name = "learning_tests_cartpole_ddppo",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "torch_only", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete"],
    size = "small",
    srcs = ["tests/run_regression_tests.py"],
    data = glob(["tuned_examples/ddppo/cartpole-ddppo.yaml"]),
    args = ["--dir=tuned_examples/ddppo"]
)

py_test(
    name = "learning_tests_pendulum_ddppo",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "torch_only", "learning_tests", "learning_tests_pendulum", "learning_tests_continuous"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = glob(["tuned_examples/ddppo/pendulum-ddppo.yaml"]),
    args = ["--dir=tuned_examples/ddppo"]
)

# DQN
# py_test(
#    name = "learning_tests_cartpole_dqn",
#    main = "tests/run_regression_tests.py",
#    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete"],
#    size = "large",
#    srcs = ["tests/run_regression_tests.py"],
#    data = ["tuned_examples/dqn/cartpole-dqn.yaml"],
#    args = ["--dir=tuned_examples/dqn"]
# )

py_test(
    name = "learning_tests_cartpole_dqn_softq",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete"],
    size = "large", # bazel may complain about it being too long sometimes - large is on purpose as some frameworks take longer
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/dqn/cartpole-dqn-softq.yaml"],
    args = ["--dir=tuned_examples/dqn"]
)

# Does not work with tf-eager tracing due to Exploration's postprocessing
# method injecting a tensor into a new graph. Revisit when tf-eager tracing
# is better supported.
py_test(
    name = "learning_tests_cartpole_dqn_param_noise",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete", "no_tf_eager_tracing"],
    size = "medium",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/dqn/cartpole-dqn-param-noise.yaml"],
    args = ["--dir=tuned_examples/dqn"]
)

py_test(
    name = "learning_tests_cartpole_dqn_fake_gpus",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete", "fake_gpus"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/dqn/cartpole-dqn-fake-gpus.yaml"],
    args = ["--dir=tuned_examples/dqn"]
)

# DT
py_test(
   name = "learning_tests_pendulum_dt",
   main = "tests/run_regression_tests.py",
   tags = ["team:rllib", "torch_only", "learning_tests", "learning_tests_pendulum", "learning_tests_continuous"],
   size = "large",
   srcs = ["tests/run_regression_tests.py"],
   # Include an offline json data file as well.
   data = [
       "tuned_examples/dt/pendulum-v1-dt.yaml",
       "tests/data/pendulum/pendulum_expert_sac_50eps.zip",
   ],
   args = ["--dir=tuned_examples/dt"]
)

py_test(
    name = "learning_tests_cartpole_dt",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "torch_only", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete"],
    size = "medium",
    srcs = ["tests/run_regression_tests.py"],
    # Include an offline json data file as well.
    data = [
        "tuned_examples/dt/cartpole-v1-dt.yaml",
        "tests/data/cartpole/large.json",
    ],
    args = ["--dir=tuned_examples/dt"]
)

# Simple-Q
py_test(
    name = "learning_tests_cartpole_simpleq",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete"],
    size = "medium",
    srcs = ["tests/run_regression_tests.py"],
    data = [
        "tuned_examples/simple_q/cartpole-simpleq.yaml",
    ],
    args = ["--dir=tuned_examples/simple_q"]
)

py_test(
    name = "learning_tests_cartpole_simpleq_fake_gpus",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete", "fake_gpus"],
    size = "medium",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/simple_q/cartpole-simpleq-fake-gpus.yaml"],
    args = ["--dir=tuned_examples/simple_q"]
)

# ES
# py_test(
#    name = "learning_tests_cartpole_es",
#    main = "tests/run_regression_tests.py",
#    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete"],
#    size = "large",
#    srcs = ["tests/run_regression_tests.py"],
#    data = ["tuned_examples/es/cartpole-es.yaml"],
#    args = ["--dir=tuned_examples/es"]
# )

# IMPALA
# py_test(
#    name = "learning_tests_cartpole_impala",
#    main = "tests/run_regression_tests.py",
#    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete"],
#    size = "large",
#    srcs = ["tests/run_regression_tests.py"],
#    data = ["tuned_examples/impala/cartpole-impala.yaml"],
#    args = ["--dir=tuned_examples/impala"]
# )

py_test(
    name = "learning_tests_multi_agent_cartpole_impala",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete"],
    size = "medium",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/impala/multi-agent-cartpole-impala.yaml"],
    args = ["--dir=tuned_examples/impala"]
)

py_test(
    name = "learning_tests_cartpole_impala_fake_gpus",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete", "fake_gpus"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/impala/cartpole-impala-fake-gpus.yaml"],
    args = ["--dir=tuned_examples/impala"]
)

# MADDPG
py_test(
    name = "learning_tests_two_step_game_maddpg",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "tf_only", "no_tf_eager_tracing", "learning_tests", "learning_tests_discrete"],
    size = "large", # bazel may complain about it being too long sometimes - large is on purpose as some frameworks take longer
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/maddpg/two-step-game-maddpg.yaml"],
    args = ["--dir=tuned_examples/maddpg", "--framework=tf"]
)

# Working, but takes a long time to learn (>15min).
# Removed due to Higher API conflicts with Pytorch-Import tests
## MB-MPO
#py_test(
#    name = "learning_tests_pendulum_mbmpo",
#    main = "tests/run_regression_tests.py",
#    tags = ["team:rllib", "exclusive", "torch_only", "learning_tests", "learning_tests_pendulum", "learning_tests_continuous"],
#    size = "large",
#    srcs = ["tests/run_regression_tests.py"],
#    data = ["tuned_examples/mbmpo/pendulum-mbmpo.yaml"],
#    args = ["--dir=tuned_examples/mbmpo"]
#)

# PG
py_test(
    name = "learning_tests_cartpole_pg",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete"],
    size = "medium", # bazel may complain about it being too long sometimes - medium is on purpose as some frameworks take longer
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/pg/cartpole-pg.yaml"],
    args = ["--dir=tuned_examples/pg"]
)

py_test(
    name = "learning_tests_cartpole_crashing_pg",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete", "crashing_cartpole"],
    size = "large", # bazel may complain about it being too long sometimes - large is on purpose as some frameworks take longer
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/pg/cartpole-crashing-pg.yaml"],
    args = ["--dir=tuned_examples/pg"]
)

py_test(
    name = "learning_tests_cartpole_crashing_with_remote_envs_pg",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete", "crashing_cartpole"],
    size = "large", # bazel may complain about it being too long sometimes - large is on purpose as some frameworks take longer
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/pg/cartpole-crashing-with-remote-envs-pg.yaml"],
    args = ["--dir=tuned_examples/pg"]
)

py_test(
    name = "learning_tests_multi_agent_cartpole_crashing_restart_sub_envs_pg",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete", "crashing_cartpole"],
    size = "large", # bazel may complain about it being too long sometimes - large is on purpose as some frameworks take longer
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/pg/multi-agent-cartpole-crashing-restart-sub-envs-pg.yaml"],
    args = ["--dir=tuned_examples/pg"]
)

py_test(
    name = "learning_tests_multi_agent_cartpole_crashing_with_remote_envs_pg",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete", "crashing_cartpole"],
    size = "large", # bazel may complain about it being too long sometimes - large is on purpose as some frameworks take longer
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/pg/multi-agent-cartpole-crashing-with-remote-envs-pg.yaml"],
    args = ["--dir=tuned_examples/pg", "--num-cpus=14"]
)

py_test(
    name = "learning_tests_cartpole_pg_fake_gpus",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete", "fake_gpus"],
    size = "medium",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/pg/cartpole-pg-fake-gpus.yaml"],
    args = ["--dir=tuned_examples/pg"]
)

# PPO
# py_test(
#    name = "learning_tests_cartpole_ppo",
#    main = "tests/run_regression_tests.py",
#    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete"],
#    size = "large",
#    srcs = ["tests/run_regression_tests.py"],
#    data = ["tuned_examples/ppo/cartpole-ppo.yaml"],
#    args = ["--dir=tuned_examples/ppo"]
# )

py_test(
    name = "learning_tests_pendulum_ppo",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_pendulum", "learning_tests_continuous"],
    size = "large", # bazel may complain about it being too long sometimes - large is on purpose as some frameworks take longer
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/ppo/pendulum-ppo.yaml"],
    args = ["--dir=tuned_examples/ppo"]
)

py_test(
    name = "learning_tests_pendulum_ppo_with_rl_module",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_pendulum", "learning_tests_continuous", "torch_only"],
    size = "large", # bazel may complain about it being too long sometimes - large is on purpose as some frameworks take longer
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/ppo/pendulum-ppo-with-rl-module.yaml"],
    args = ["--dir=tuned_examples/ppo"]
)

py_test(
    name = "learning_tests_multi_agent_pendulum_ppo",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_pendulum", "learning_tests_continuous"],
    size = "large", # bazel may complain about it being too long sometimes - large is on purpose as some frameworks take longer
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/ppo/multi_agent_pendulum_ppo.py"],
    args = ["--dir=tuned_examples/ppo"]
)

py_test(
    name = "learning_tests_transformed_actions_pendulum_ppo",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_pendulum", "learning_tests_continuous"],
    size = "large", # bazel may complain about it being too long sometimes - large is on purpose as some frameworks take longer
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/ppo/pendulum-transformed-actions-ppo.yaml"],
    args = ["--dir=tuned_examples/ppo"]
)

py_test(
    name = "learning_tests_repeat_after_me_ppo",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_discrete"],
    size = "medium",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/ppo/repeatafterme-ppo-lstm.yaml"],
    args = ["--dir=tuned_examples/ppo"]
)

py_test(
    name = "learning_tests_cartpole_ppo_fake_gpus",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete", "fake_gpus"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/ppo/cartpole-ppo-fake-gpus.yaml"],
    args = ["--dir=tuned_examples/ppo"]
)

# QMIX
py_test(
    name = "learning_tests_two_step_game_qmix",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_discrete"],
    size = "medium", # bazel may complain about it being too long sometimes - medium is on purpose as some frameworks take longer
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/qmix/two-step-game-qmix.yaml"],
    args = ["--dir=tuned_examples/qmix", "--framework=torch"]
)

py_test(
    name = "learning_tests_two_step_game_qmix_vdn_mixer",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_discrete"],
    size = "medium", # bazel may complain about it being too long sometimes - medium is on purpose as some frameworks take longer
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/qmix/two-step-game-qmix-vdn-mixer.yaml"],
    args = ["--dir=tuned_examples/qmix", "--framework=torch"]
)

py_test(
    name = "learning_tests_two_step_game_qmix_no_mixer",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_discrete"],
    size = "medium", # bazel may complain about it being too long sometimes - medium is on purpose as some frameworks take longer
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/qmix/two-step-game-qmix-no-mixer.yaml"],
    args = ["--dir=tuned_examples/qmix", "--framework=torch"]
)

# R2D2
py_test(
    name = "learning_tests_stateless_cartpole_r2d2",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete", "stateless_cartpole"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/r2d2/stateless-cartpole-r2d2.yaml"],
    args = ["--dir=tuned_examples/r2d2"]
)

py_test(
    name = "learning_tests_stateless_cartpole_r2d2_fake_gpus",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_cartpole", "stateless_cartpole", "fake_gpus"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/r2d2/stateless-cartpole-r2d2-fake-gpus.yaml"],
    args = ["--dir=tuned_examples/r2d2"]
)

# SAC
py_test(
    name = "learning_tests_cartpole_sac",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/sac/cartpole-sac.yaml"],
    args = ["--dir=tuned_examples/sac"]
)

# py_test(
#    name = "learning_tests_cartpole_continuous_pybullet_sac",
#    main = "tests/run_regression_tests.py",
#    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_cartpole", "learning_tests_continuous"],
#    size = "large",
#    srcs = ["tests/run_regression_tests.py"],
#    data = ["tuned_examples/sac/cartpole-continuous-pybullet-sac.yaml"],
#    args = ["--dir=tuned_examples/sac"]
# )

# py_test(
#    name = "learning_tests_pendulum_sac",
#    main = "tests/run_regression_tests.py",
#    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_pendulum", "learning_tests_continuous"],
#    size = "large",
#    srcs = ["tests/run_regression_tests.py"],
#    data = ["tuned_examples/sac/pendulum-sac.yaml"],
#    args = ["--dir=tuned_examples/sac"]
# )

# py_test(
#    name = "learning_tests_transformed_actions_pendulum_sac",
#    main = "tests/run_regression_tests.py",
#    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_pendulum", "learning_tests_continuous"],
#    size = "large",
#    srcs = ["tests/run_regression_tests.py"],
#    data = ["tuned_examples/sac/pendulum-transformed-actions-sac.yaml"],
#    args = ["--dir=tuned_examples/sac"]
# )

# py_test(
#    name = "learning_tests_pendulum_sac_fake_gpus",
#    main = "tests/run_regression_tests.py",
#    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_pendulum", "learning_tests_continuous", "fake_gpus"],
#    size = "large",
#    srcs = ["tests/run_regression_tests.py"],
#    data = ["tuned_examples/sac/pendulum-sac-fake-gpus.yaml"],
#    args = ["--dir=tuned_examples/sac"]
# )

# SlateQ
# py_test(
#    name = "learning_tests_interest_evolution_10_candidates_recsim_env_slateq",
#    main = "tests/run_regression_tests.py",
#    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_discrete"],
#    size = "large",
#    srcs = ["tests/run_regression_tests.py"],
#    data = ["tuned_examples/slateq/interest-evolution-10-candidates-recsim-env-slateq.yaml"],
#    args = ["--dir=tuned_examples/slateq"]
# )

py_test(
    name = "learning_tests_interest_evolution_10_candidates_recsim_env_slateq_fake_gpus",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_discrete", "fake_gpus"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/slateq/interest-evolution-10-candidates-recsim-env-slateq.yaml"],
    args = ["--dir=tuned_examples/slateq"]
)

# TD3
# py_test(
#    name = "learning_tests_pendulum_td3",
#    main = "tests/run_regression_tests.py",
#    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_pendulum", "learning_tests_continuous"],
#    size = "large",
#    srcs = ["tests/run_regression_tests.py"],
#    data = ["tuned_examples/ddpg/pendulum-td3.yaml"],
#    args = ["--dir=tuned_examples/ddpg"]
# )


# --------------------------------------------------------------------
# Algorithms (Compilation, Losses, simple functionality tests)
# rllib/algorithms/
#
# Tag: algorithms_dir
# --------------------------------------------------------------------

# Generic (all Algorithms)

py_test(
    name = "test_algorithm",
    tags = ["team:rllib", "algorithms_dir", "algorithms_dir_generic"],
    size = "large",
    srcs = ["algorithms/tests/test_algorithm.py"],
    data = ["tests/data/cartpole/small.json"],
)

py_test(
    name = "test_algorithm_config",
    tags = ["team:rllib", "algorithms_dir", "algorithms_dir_generic"],
    size = "small",
    srcs = ["algorithms/tests/test_algorithm_config.py"],
)

py_test(
    name = "test_algorithm_export_checkpoint",
    tags = ["team:rllib", "algorithms_dir", "algorithms_dir_generic"],
    size = "medium",
    srcs = ["algorithms/tests/test_algorithm_export_checkpoint.py"],
)

py_test(
    name = "test_callbacks",
    tags = ["team:rllib", "algorithms_dir", "algorithms_dir_generic"],
    size = "large",
    srcs = ["algorithms/tests/test_callbacks.py"]
)

py_test(
    name = "test_memory_leaks_generic",
    main = "algorithms/tests/test_memory_leaks.py",
    tags = ["team:rllib", "algorithms_dir"],
    size = "medium",
    srcs = ["algorithms/tests/test_memory_leaks.py"]
)

py_test(
    name = "test_node_failure",
    tags = ["team:rllib", "tests_dir", "exclusive"],
    size = "medium",
    srcs = ["tests/test_node_failure.py"],
)

py_test(
    name = "test_registry",
    tags = ["team:rllib", "algorithms_dir", "algorithms_dir_generic"],
    size = "small",
    srcs = ["algorithms/tests/test_registry.py"],
)

py_test(
    name = "test_worker_failures",
    tags = ["team:rllib", "algorithms_dir", "algorithms_dir_generic", "exclusive"],
    size = "large",
    srcs = ["algorithms/tests/test_worker_failures.py"]
)

# Specific Algorithms

# A2C
py_test(
    name = "test_a2c",
    tags = ["team:rllib", "algorithms_dir"],
    size = "large",
    srcs = ["algorithms/a2c/tests/test_a2c.py"]
)

# A3C
py_test(
    name = "test_a3c",
    tags = ["team:rllib", "algorithms_dir"],
    size = "large",
    srcs = ["algorithms/a3c/tests/test_a3c.py"]
)

# AlphaStar
py_test(
    name = "test_alpha_star",
    tags = ["team:rllib", "algorithms_dir"],
    size = "large",
    srcs = ["algorithms/alpha_star/tests/test_alpha_star.py"]
)

# AlphaZero
py_test(
    name = "test_alpha_zero",
    tags = ["team:rllib", "algorithms_dir"],
    size = "medium",
    srcs = ["algorithms/alpha_zero/tests/test_alpha_zero.py"]
)

# APEX-DQN
py_test(
    name = "test_apex_dqn",
    tags = ["team:rllib", "algorithms_dir"],
    size = "large",
    srcs = ["algorithms/apex_dqn/tests/test_apex_dqn.py"]
)

# APEX-DDPG
py_test(
    name = "test_apex_ddpg",
    tags = ["team:rllib", "algorithms_dir"],
    size = "medium",
    srcs = ["algorithms/apex_ddpg/tests/test_apex_ddpg.py"]
)

# APPO
py_test(
    name = "test_appo",
    tags = ["team:rllib", "algorithms_dir"],
    size = "large",
    srcs = ["algorithms/appo/tests/test_appo.py"]
)

# ARS
py_test(
    name = "test_ars",
    tags = ["team:rllib", "algorithms_dir"],
    size = "medium",
    srcs = ["algorithms/ars/tests/test_ars.py"]
)

# Bandits
py_test(
    name = "test_bandits",
    tags = ["team:rllib", "algorithms_dir"],
    size = "large",
    srcs = ["algorithms/bandit/tests/test_bandits.py"],
)

# BC
py_test(
    name = "test_bc",
    tags = ["team:rllib", "algorithms_dir"],
    size = "medium",
    # Include the json data file.
    data = ["tests/data/cartpole/large.json"],
    srcs = ["algorithms/bc/tests/test_bc.py"]
)

# CQL
py_test(
    name = "test_cql",
    tags = ["team:rllib", "algorithms_dir"],
    size = "large",
    data = ["tests/data/pendulum/small.json"],
    srcs = ["algorithms/cql/tests/test_cql.py"]
)

# CRR
py_test(
    name = "test_crr",
    tags = ["team:rllib", "algorithms_dir"],
    size = "medium",
    srcs = ["algorithms/crr/tests/test_crr.py"],
    data = ["tests/data/pendulum/large.json"],
)

# DDPG
py_test(
    name = "test_ddpg",
    tags = ["team:rllib", "algorithms_dir"],
    size = "large",
    srcs = ["algorithms/ddpg/tests/test_ddpg.py"]
)

# DDPPO
py_test(
    name = "test_ddppo",
    tags = ["team:rllib", "algorithms_dir"],
    size = "medium",
    srcs = ["algorithms/ddppo/tests/test_ddppo.py"]
)

# DQN
py_test(
    name = "test_dqn",
    tags = ["team:rllib", "algorithms_dir"],
    size = "large",
    srcs = ["algorithms/dqn/tests/test_dqn.py"]
)

# DQN Reproducibility
py_test(
    name = "test_repro_dqn",
    tags = ["team:rllib", "algorithms_dir", "gpu"],
    size = "large",
    srcs = ["algorithms/dqn/tests/test_repro_dqn.py"]
)

# Dreamer
py_test(
    name = "test_dreamer",
    tags = ["team:rllib", "algorithms_dir"],
    size = "medium",
    srcs = ["algorithms/dreamer/tests/test_dreamer.py"]
)

# DT
py_test(
    name = "test_segmentation_buffer",
    tags = ["team:rllib", "algorithms_dir"],
    size = "small",
    srcs = ["algorithms/dt/tests/test_segmentation_buffer.py"]
)

py_test(
    name = "test_dt_model",
    tags = ["team:rllib", "algorithms_dir"],
    size = "small",
    srcs = ["algorithms/dt/tests/test_dt_model.py"]
)

py_test(
    name = "test_dt_policy",
    tags = ["team:rllib", "algorithms_dir"],
    size = "small",
    srcs = ["algorithms/dt/tests/test_dt_policy.py"]
)

py_test(
    name = "test_dt",
    tags = ["team:rllib", "algorithms_dir"],
    size = "medium",
    srcs = ["algorithms/dt/tests/test_dt.py"],
    data = ["tests/data/pendulum/large.json"],
)

# ES
py_test(
    name = "test_es",
    tags = ["team:rllib", "algorithms_dir"],
    size = "medium",
    srcs = ["algorithms/es/tests/test_es.py"]
)

# Impala
py_test(
    name = "test_impala",
    tags = ["team:rllib", "algorithms_dir"],
    size = "large",
    srcs = ["algorithms/impala/tests/test_impala.py"]
)
py_test(
    name = "test_vtrace",
    tags = ["team:rllib", "algorithms_dir"],
    size = "small",
    srcs = ["algorithms/impala/tests/test_vtrace.py"]
)

# MARWIL
py_test(
    name = "test_marwil",
    tags = ["team:rllib", "algorithms_dir"],
    size = "large",
    # Include the json data file.
    data = [
        "tests/data/cartpole/large.json",
        "tests/data/pendulum/large.json",
        "tests/data/cartpole/small.json",
    ],
    srcs = ["algorithms/marwil/tests/test_marwil.py"]
)

# MADDPG
py_test(
    name = "test_maddpg",
    tags = ["team:rllib", "algorithms_dir"],
    size = "medium",
    srcs = ["algorithms/maddpg/tests/test_maddpg.py"]
)

# MAML
py_test(
    name = "test_maml",
    tags = ["team:rllib", "algorithms_dir"],
    size = "medium",
    srcs = ["algorithms/maml/tests/test_maml.py"]
)

# MBMPO
py_test(
    name = "test_mbmpo",
    tags = ["team:rllib", "algorithms_dir"],
    size = "medium",
    srcs = ["algorithms/mbmpo/tests/test_mbmpo.py"]
)

# PG
py_test(
    name = "test_pg",
    tags = ["team:rllib", "algorithms_dir"],
    size = "large",
    srcs = ["algorithms/pg/tests/test_pg.py"]
)

# PPO
py_test(
    name = "test_ppo",
    tags = ["team:rllib", "algorithms_dir"],
    size = "large",
    srcs = ["algorithms/ppo/tests/test_ppo.py"]
)

py_test(
    name = "test_ppo_with_rl_module",
    tags = ["team:rllib", "algorithms_dir"],
    size = "large",
    srcs = ["algorithms/ppo/tests/test_ppo_with_rl_module.py"]
)

# PPO Reproducibility
py_test(
    name = "test_repro_ppo",
    tags = ["team:rllib", "algorithms_dir", "gpu"],
    size = "large",
    srcs = ["algorithms/ppo/tests/test_repro_ppo.py"]
)

# QMix
py_test(
    name = "test_qmix",
    tags = ["team:rllib", "algorithms_dir"],
    size = "medium",
    srcs = ["algorithms/qmix/tests/test_qmix.py"]
)

# R2D2
py_test(
    name = "test_r2d2",
    tags = ["team:rllib", "algorithms_dir"],
    size = "medium",
    srcs = ["algorithms/r2d2/tests/test_r2d2.py"]
)

# RandomAgent
py_test(
    name = "test_random_agent",
    tags = ["team:rllib", "algorithms_dir"],
    size = "small",
    srcs = ["algorithms/random_agent/random_agent.py"]
)

# RNNSAC
py_test(
    name = "test_rnnsac",
    tags = ["team:rllib", "algorithms_dir"],
    size = "small",
    srcs = ["algorithms/sac/tests/test_rnnsac.py"]
)

# SAC
py_test(
    name = "test_sac",
    tags = ["team:rllib", "algorithms_dir"],
    size = "large",
    srcs = ["algorithms/sac/tests/test_sac.py"]
)

# SimpleQ
py_test(
    name = "test_simple_q",
    tags = ["team:rllib", "algorithms_dir"],
    size = "medium",
    srcs = ["algorithms/simple_q/tests/test_simple_q.py"]
)

# SimpleQ Reproducibility
py_test(
    name = "test_repro_simple_q",
    tags = ["team:rllib", "algorithms_dir", "gpu"],
    size = "large",
    srcs = ["algorithms/simple_q/tests/test_repro_simple_q.py"]
)

# SlateQ
py_test(
    name = "test_slateq",
    tags = ["team:rllib", "algorithms_dir"],
    size = "medium",
    srcs = ["algorithms/slateq/tests/test_slateq.py"]
)

# TD3
py_test(
    name = "test_td3",
    tags = ["team:rllib", "algorithms_dir"],
    size = "medium",
    srcs = ["algorithms/td3/tests/test_td3.py"]
)


# --------------------------------------------------------------------
# Memory leak tests
#
# Tag: memory_leak_tests
# --------------------------------------------------------------------

py_test(
    name = "test_memory_leak_a3c",
    tags = ["team:rllib", "memory_leak_tests"],
    main = "utils/tests/run_memory_leak_tests.py",
    size = "large",
    srcs = ["utils/tests/run_memory_leak_tests.py"],
    data = ["tuned_examples/a3c/memory-leak-test-a3c.yaml"],
    args = ["--dir=tuned_examples/a3c"]
)

py_test(
    name = "test_memory_leak_appo",
    tags = ["team:rllib", "memory_leak_tests"],
    main = "utils/tests/run_memory_leak_tests.py",
    size = "large",
    srcs = ["utils/tests/run_memory_leak_tests.py"],
    data = ["tuned_examples/appo/memory-leak-test-appo.yaml"],
    args = ["--dir=tuned_examples/appo"]
)

py_test(
    name = "test_memory_leak_ddpg",
    tags = ["team:rllib", "memory_leak_tests"],
    main = "utils/tests/run_memory_leak_tests.py",
    size = "large",
    srcs = ["utils/tests/run_memory_leak_tests.py"],
    data = ["tuned_examples/ddpg/memory-leak-test-ddpg.yaml"],
    args = ["--dir=tuned_examples/ddpg"]
)

py_test(
    name = "test_memory_leak_dqn",
    tags = ["team:rllib", "memory_leak_tests"],
    main = "utils/tests/run_memory_leak_tests.py",
    size = "large",
    srcs = ["utils/tests/run_memory_leak_tests.py"],
    data = ["tuned_examples/dqn/memory-leak-test-dqn.yaml"],
    args = ["--dir=tuned_examples/dqn"]
)

py_test(
    name = "test_memory_leak_impala",
    tags = ["team:rllib", "memory_leak_tests"],
    main = "utils/tests/run_memory_leak_tests.py",
    size = "large",
    srcs = ["utils/tests/run_memory_leak_tests.py"],
    data = ["tuned_examples/impala/memory-leak-test-impala.yaml"],
    args = ["--dir=tuned_examples/impala"]
)

py_test(
    name = "test_memory_leak_ppo",
    tags = ["team:rllib", "memory_leak_tests"],
    main = "utils/tests/run_memory_leak_tests.py",
    size = "large",
    srcs = ["utils/tests/run_memory_leak_tests.py"],
    data = ["tuned_examples/ppo/memory-leak-test-ppo.yaml"],
    args = ["--dir=tuned_examples/ppo"]
)

py_test(
    name = "test_memory_leak_sac",
    tags = ["team:rllib", "memory_leak_tests"],
    main = "utils/tests/run_memory_leak_tests.py",
    size = "large",
    srcs = ["utils/tests/run_memory_leak_tests.py"],
    data = ["tuned_examples/sac/memory-leak-test-sac.yaml"],
    args = ["--dir=tuned_examples/sac"]
)

# --------------------------------------------------------------------
# Algorithms (quick training test iterations via `rllib train`)
#
# Tag: quick_train
#
# These are not(!) learning tests, we only test here compilation and
# support for certain envs, spaces, setups.
# Should all be very short tests with label: "quick_train".
# --------------------------------------------------------------------

# A2C/A3C

py_test(
    name = "test_a3c_torch_pong_deterministic_v4",
    main = "train.py", srcs = ["train.py"],
    tags = ["team:rllib", "quick_train"],
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
    tags = ["team:rllib", "quick_train"],
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
    tags = ["team:rllib", "quick_train"],
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
    tags = ["team:rllib", "quick_train"],
    args = [
        "--env", "MountainCarContinuous-v0",
        "--run", "DDPG",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"framework\": \"tf\", \"num_workers\": 1}'"
        ]
)

py_test(
    name = "test_apex_ddpg_pendulum_v1_complete_episode_batches",
    main = "train.py", srcs = ["train.py"],
    tags = ["team:rllib", "quick_train"],
    args = [
        "--env", "Pendulum-v1",
        "--run", "APEX_DDPG",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"framework\": \"tf\", \"num_workers\": 2, \"optimizer\": {\"num_replay_buffer_shards\": 1}, \"num_steps_sampled_before_learning_starts\": 100, \"min_time_s_per_iteration\": 1, \"batch_mode\": \"complete_episodes\"}'",
        "--ray-num-cpus", "4",
        ]
)

# DQN/APEX

py_test(
    name = "test_dqn_frozenlake_v1",
    main = "train.py", srcs = ["train.py"],
    size = "medium",
    tags = ["team:rllib", "quick_train"],
    args = [
        "--env", "FrozenLake-v1",
        "--run", "DQN",
        "--config", "'{\"framework\": \"tf\"}'",
        "--stop", "'{\"training_iteration\": 1}'"
        ]
)

py_test(
    name = "test_dqn_cartpole_v1_no_dueling",
    main = "train.py", srcs = ["train.py"],
    size = "medium",
    tags = ["team:rllib", "quick_train"],
    args = [
        "--env", "CartPole-v1",
        "--run", "DQN",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"framework\": \"tf\", \"lr\": 1e-3, \"exploration_config\": {\"epsilon_timesteps\": 10000, \"final_epsilon\": 0.02}, \"dueling\": false, \"hiddens\": [], \"model\": {\"fcnet_hiddens\": [64], \"fcnet_activation\": \"relu\"}}'"
        ]
)

py_test(
    name = "test_dqn_cartpole_v1",
    main = "train.py", srcs = ["train.py"],
    tags = ["team:rllib", "quick_train"],
    args = [
        "--env", "CartPole-v1",
        "--run", "DQN",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"framework\": \"tf\", \"num_workers\": 2}'",
        "--ray-num-cpus", "4"
        ]
)

py_test(
    name = "test_dqn_cartpole_v1_with_offline_input_and_softq",
    main = "train.py", srcs = ["train.py"],
    tags = ["team:rllib", "quick_train", "external_files"],
    size = "medium",
    # Include the json data file.
    data = ["tests/data/cartpole/small.json"],
    args = [
        "--env", "CartPole-v1",
        "--run", "DQN",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"framework\": \"tf\", \"input\": \"tests/data/cartpole\", \"num_steps_sampled_before_learning_starts\": 0, \"off_policy_estimation_methods\": {\"wis\": {\"type\": \"ray.rllib.offline.estimators.weighted_importance_sampling.WeightedImportanceSampling\"}, \"is\": {\"type\": \"ray.rllib.offline.estimators.importance_sampling.ImportanceSampling\"}}, \"exploration_config\": {\"type\": \"SoftQ\"}}'"
        ]
)

py_test(
    name = "test_dqn_pong_deterministic_v4",
    main = "train.py", srcs = ["train.py"],
    tags = ["team:rllib", "quick_train"],
    args = [
        "--env", "PongDeterministic-v4",
        "--run", "DQN",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"framework\": \"tf\", \"lr\": 1e-4, \"exploration_config\": {\"epsilon_timesteps\": 200000, \"final_epsilon\": 0.01}, \"replay_buffer_config\": {\"capacity\": 10000}, \"num_steps_sampled_before_learning_starts\": 10000, \"rollout_fragment_length\": 4, \"target_network_update_freq\": 1000, \"gamma\": 0.99}'"
        ]
)

# IMPALA

py_test(
    name = "test_impala_buffers_2",
    main = "train.py", srcs = ["train.py"],
    tags = ["team:rllib", "quick_train"],
    args = [
        "--env", "CartPole-v1",
        "--run", "IMPALA",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"framework\": \"tf\", \"num_gpus\": 0, \"num_workers\": 2, \"min_time_s_per_iteration\": 1, \"num_multi_gpu_tower_stacks\": 2, \"replay_buffer_num_slots\": 100, \"replay_proportion\": 1.0}'",
        "--ray-num-cpus", "4",
        ]
)

py_test(
    name = "test_impala_cartpole_v1_buffers_2_lstm",
    main = "train.py",
    srcs = ["train.py"],
    tags = ["team:rllib", "quick_train"],
    args = [
        "--env", "CartPole-v1",
        "--run", "IMPALA",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"framework\": \"tf\", \"num_gpus\": 0, \"num_workers\": 2, \"min_time_s_per_iteration\": 1, \"num_multi_gpu_tower_stacks\": 2, \"replay_buffer_num_slots\": 100, \"replay_proportion\": 1.0, \"model\": {\"use_lstm\": true}}'",
        "--ray-num-cpus", "4",
        ]
)

py_test(
    name = "test_impala_pong_deterministic_v4_40k_ts_1G_obj_store",
    main = "train.py",
    srcs = ["train.py"],
    tags = ["team:rllib", "quick_train"],
    size = "large", # bazel may complain about it being too long sometimes - large is on purpose as some frameworks take longer
    args = [
        "--env", "PongDeterministic-v4",
        "--run", "IMPALA",
        "--stop", "'{\"timesteps_total\": 30000}'",
        "--ray-object-store-memory=1000000000",
        "--config", "'{\"framework\": \"tf\", \"num_workers\": 1, \"num_gpus\": 0, \"num_envs_per_worker\": 32, \"rollout_fragment_length\": 50, \"train_batch_size\": 50, \"learner_queue_size\": 1}'"
        ]
)

# PG

py_test(
    name = "test_pg_tf_cartpole_v1_lstm",
    main = "train.py", srcs = ["train.py"],
    tags = ["team:rllib", "quick_train"],
    args = [
        "--env", "CartPole-v1",
        "--run", "PG",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"framework\": \"tf\", \"train_batch_size\": 500, \"num_workers\": 1, \"model\": {\"use_lstm\": true, \"max_seq_len\": 100}}'"
        ]
)

py_test(
    name = "test_pg_tf_cartpole_v1_multi_envs_per_worker",
    main = "train.py", srcs = ["train.py"],
    size = "medium",
    tags = ["team:rllib", "quick_train"],
    args = [
        "--env", "CartPole-v1",
        "--run", "PG",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"framework\": \"tf\", \"train_batch_size\": 5000, \"num_workers\": 1, \"num_envs_per_worker\": 10}'"
        ]
)


py_test(
    name = "test_pg_tf_pong_v0",
    main = "train.py", srcs = ["train.py"],
    tags = ["team:rllib", "quick_train"],
    args = [
        "--env", "Pong-v0",
        "--run", "PG",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"framework\": \"tf\", \"train_batch_size\": 500, \"num_workers\": 1}'"
        ]
)

# PPO/APPO

py_test(
    name = "test_ppo_tf_cartpole_v1_complete_episode_batches",
    main = "train.py", srcs = ["train.py"],
    tags = ["team:rllib", "quick_train"],
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
    tags = ["team:rllib", "quick_train"],
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
    tags = ["team:rllib", "quick_train"],
    args = [
        "--env", "CartPole-v1",
        "--run", "PPO",
        "--stop", "'{\"training_iteration\": 2}'",
        "--config", "'{\"framework\": \"tf\", \"remote_worker_envs\": true, \"num_envs_per_worker\": 2, \"num_workers\": 1, \"train_batch_size\": 100, \"sgd_minibatch_size\": 50}'"
        ]
)

py_test(
    name = "test_appo_tf_pendulum_v1_no_gpus",
    main = "train.py", srcs = ["train.py"],
    tags = ["team:rllib", "quick_train"],
    args = [
        "--env", "Pendulum-v1",
        "--run", "APPO",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"framework\": \"tf\", \"num_workers\": 2, \"num_gpus\": 0}'",
        "--ray-num-cpus", "4"
        ]
)

# --------------------------------------------------------------------
# Connector tests
# rllib/connector/
#
# Tag: connector
# --------------------------------------------------------------------

py_test(
    name = "connectors/tests/test_connector",
    tags = ["team:rllib", "connector"],
    size = "small",
    srcs = ["connectors/tests/test_connector.py"]
)

py_test(
    name = "connectors/tests/test_action",
    tags = ["team:rllib", "connector"],
    size = "small",
    srcs = ["connectors/tests/test_action.py"]
)

py_test(
    name = "connectors/tests/test_agent",
    tags = ["team:rllib", "connector"],
    size = "small",
    srcs = ["connectors/tests/test_agent.py"]
)

# --------------------------------------------------------------------
# Env tests
# rllib/env/
#
# Tag: env
# --------------------------------------------------------------------

py_test(
    name = "env/tests/test_env_with_subprocess",
    tags = ["team:rllib", "env"],
    size = "medium",
    srcs = ["env/tests/test_env_with_subprocess.py"]
)

py_test(
    name = "env/tests/test_external_env",
    tags = ["team:rllib", "env"],
    size = "large",
    srcs = ["env/tests/test_external_env.py"]
)

py_test(
    name = "env/tests/test_external_multi_agent_env",
    tags = ["team:rllib", "env"],
    size = "small",
    srcs = ["env/tests/test_external_multi_agent_env.py"]
)

sh_test(
    name = "env/tests/test_local_inference_cartpole",
    tags = ["team:rllib", "env"],
    size = "medium",
    srcs = ["env/tests/test_policy_client_server_setup.sh"],
    args = ["local", "cartpole", "8800"],
    data = glob(["examples/serving/*.py"]),
)

sh_test(
    name = "env/tests/test_local_inference_cartpole_w_2_concurrent_episodes",
    tags = ["team:rllib", "env"],
    size = "medium",
    srcs = ["env/tests/test_policy_client_server_setup.sh"],
    args = ["local", "cartpole-dummy-2-episodes", "8830"],
    data = glob(["examples/serving/*.py"]),
)

sh_test(
    name = "env/tests/test_local_inference_unity3d",
    tags = ["team:rllib", "env"],
    size = "large", # bazel may complain about it being too long sometimes - large is on purpose as some frameworks take longer
    srcs = ["env/tests/test_policy_client_server_setup.sh"],
    args = ["local", "unity3d", "8850"],
    data = glob(["examples/serving/*.py"]),
)

py_test(
    name = "env/tests/test_multi_agent_env",
    tags = ["team:rllib", "tests_dir"],
    size = "medium",
    srcs = ["env/tests/test_multi_agent_env.py"]
)

sh_test(
    name = "env/tests/test_remote_inference_cartpole",
    tags = ["team:rllib", "env", "exclusive"],
    size = "large", # bazel may complain about it being too long sometimes - large is on purpose as some frameworks take longer
    srcs = ["env/tests/test_policy_client_server_setup.sh"],
    args = ["remote", "cartpole", "8810"],
    data = glob(["examples/serving/*.py"]),
)

sh_test(
    name = "env/tests/test_remote_inference_cartpole_lstm",
    tags = ["team:rllib", "env", "exclusive"],
    size = "large", # bazel may complain about it being too long sometimes - large is on purpose as some frameworks take longer
    srcs = ["env/tests/test_policy_client_server_setup.sh"],
    args = ["remote", "cartpole_lstm", "8820"],
    data = glob(["examples/serving/*.py"]),
)

sh_test(
    name = "env/tests/test_remote_inference_cartpole_w_2_concurrent_episodes",
    tags = ["team:rllib", "env", "exclusive"],
    size = "large", # bazel may complain about it being too long sometimes - large is on purpose as some frameworks take longer
    srcs = ["env/tests/test_policy_client_server_setup.sh"],
    args = ["remote", "cartpole-dummy-2-episodes", "8840"],
    data = glob(["examples/serving/*.py"]),
)

sh_test(
    name = "env/tests/test_remote_inference_unity3d",
    tags = ["team:rllib", "env", "exclusive"],
    size = "small",
    srcs = ["env/tests/test_policy_client_server_setup.sh"],
    args = ["remote", "unity3d", "8860"],
    data = glob(["examples/serving/*.py"]),
)

py_test(
    name = "env/tests/test_remote_worker_envs",
    tags = ["team:rllib", "env"],
    size = "medium",
    srcs = ["env/tests/test_remote_worker_envs.py"]
)

py_test(
    name = "env/wrappers/tests/test_exception_wrapper",
    tags = ["team:rllib", "env"],
    size = "small",
    srcs = ["env/wrappers/tests/test_exception_wrapper.py"]
)

py_test(
    name = "env/wrappers/tests/test_group_agents_wrapper",
    tags = ["team:rllib", "env"],
    size = "small",
    srcs = ["env/wrappers/tests/test_group_agents_wrapper.py"]
)

py_test(
    name = "env/wrappers/tests/test_recsim_wrapper",
    tags = ["team:rllib", "env"],
    size = "small",
    srcs = ["env/wrappers/tests/test_recsim_wrapper.py"]
)

py_test(
    name = "env/wrappers/tests/test_unity3d_env",
    tags = ["team:rllib", "env"],
    size = "small",
    srcs = ["env/wrappers/tests/test_unity3d_env.py"]
)

# --------------------------------------------------------------------
# Evaluation components
# rllib/evaluation/
#
# Tag: evaluation
# --------------------------------------------------------------------
py_test(
    name = "evaluation/tests/test_agent_collector",
    tags = ["team:rllib", "evaluation"],
    size = "small",
    srcs = ["evaluation/tests/test_agent_collector.py"]
)

py_test(
    name = "evaluation/tests/test_envs_that_crash",
    tags = ["team:rllib", "evaluation"],
    size = "medium",
    srcs = ["evaluation/tests/test_envs_that_crash.py"]
)

py_test(
    name = "evaluation/tests/test_episode",
    tags = ["team:rllib", "evaluation"],
    size = "small",
    srcs = ["evaluation/tests/test_episode.py"]
)

py_test(
    name = "evaluation/tests/test_env_runner_v2",
    tags = ["team:rllib", "evaluation"],
    size = "small",
    srcs = ["evaluation/tests/test_env_runner_v2.py"]
)

py_test(
    name = "evaluation/tests/test_episode_v2",
    tags = ["team:rllib", "evaluation"],
    size = "small",
    srcs = ["evaluation/tests/test_episode_v2.py"]
)

py_test(
    name = "evaluation/tests/test_postprocessing",
    tags = ["team:rllib", "evaluation"],
    size = "small",
    srcs = ["evaluation/tests/test_postprocessing.py"]
)

py_test(
    name = "evaluation/tests/test_worker_set",
    tags = ["team:rllib", "evaluation", "exclusive"],
    size = "small",
    srcs = ["evaluation/tests/test_worker_set.py"]
)

py_test(
    name = "evaluation/tests/test_rollout_worker",
    tags = ["team:rllib", "evaluation", "exclusive"],
    size = "large",
    srcs = ["evaluation/tests/test_rollout_worker.py"]
)

py_test(
    name = "evaluation/tests/test_trajectory_view_api",
    tags = ["team:rllib", "evaluation"],
    size = "large",
    srcs = ["evaluation/tests/test_trajectory_view_api.py"]
)

# --------------------------------------------------------------------
# Execution Utils
# rllib/execution/
#
# Tag: execution
# --------------------------------------------------------------------

py_test(
    name = "test_async_requests_manager",
    tags = ["team:rllib", "execution", "exclusive"],
    size = "medium",
    srcs = ["execution/tests/test_async_requests_manager.py"]
)

# --------------------------------------------------------------------
# RLlib core
# rllib/core/
#
# Tag: core
# --------------------------------------------------------------------

py_test(
    name = "test_torch_rl_module",
    tags = ["team:rllib", "core"],
    size = "medium",
    srcs = ["core/rl_module/torch/tests/test_torch_rl_module.py"]
)

py_test(
    name = "test_marl_module",
    tags = ["team:rllib", "core"],
    size = "medium",
    srcs = ["core/rl_module/tests/test_marl_module.py"]
)

py_test(
    name = "test_rl_optimizer",
    tags = ["team:rllib", "core"],
    size = "medium",
    srcs = ["core/optim/tests/test_rl_optimizer.py"]
)

# --------------------------------------------------------------------
# Models and Distributions
# rllib/models/
#
# Tag: models
# --------------------------------------------------------------------

py_test(
    name = "test_attention_nets",
    tags = ["team:rllib", "models"],
    size = "large",
    srcs = ["models/tests/test_attention_nets.py"]
)

py_test(
    name = "test_check_specs",
    tags = ["team:rllib", "models"],
    size = "small",
    srcs = ["models/specs/tests/test_check_specs.py"]
)

py_test(
    name = "test_conv2d_default_stacks",
    tags = ["team:rllib", "models"],
    size = "small",
    srcs = ["models/tests/test_conv2d_default_stacks.py"]
)

py_test(
    name = "test_convtranspose2d_stack",
    tags = ["team:rllib", "models"],
    size = "small",
    data = glob(["tests/data/images/obstacle_tower.png"]),
    srcs = ["models/tests/test_convtranspose2d_stack.py"]
)

py_test(
    name = "test_action_distributions",
    tags = ["team:rllib", "models"],
    size = "medium",
    srcs = ["models/tests/test_action_distributions.py"]
)

py_test(
    name = "test_distributions",
    tags = ["team:rllib", "models"],
    size = "small",
    srcs = ["models/tests/test_distributions.py"]
)

py_test(
    name = "test_lstms",
    tags = ["team:rllib", "models"],
    size = "large",
    srcs = ["models/tests/test_lstms.py"]
)

py_test(
    name = "test_models",
    tags = ["team:rllib", "models"],
    size = "medium",
    srcs = ["models/tests/test_models.py"]
)

py_test(
    name = "test_preprocessors",
    tags = ["team:rllib", "models"],
    size = "medium",
    srcs = ["models/tests/test_preprocessors.py"]
)

# Test Tensor specs
py_test(
    name = "test_tensor_spec",
    tags = ["team:rllib", "models"],
    size = "small",
    srcs = ["models/specs/tests/test_tensor_spec.py"]
)

# test abstract base models
py_test(
    name = "test_base_model",
    tags = ["team:rllib", "models"],
    size = "small",
    srcs = ["models/tests/test_base_model.py"]
)

# test torch base models
py_test(
    name = "test_torch_model",
    tags = ["team:rllib", "models"],
    size = "small",
    srcs = ["models/tests/test_torch_model.py"]
)

# test ModelSpec
py_test(
    name = "test_model_spec",
    tags = ["team:rllib", "models"],
    size = "small",
    srcs = ["models/specs/tests/test_model_spec.py"]
)


# --------------------------------------------------------------------
# Offline
# rllib/offline/
#
# Tag: offline
# --------------------------------------------------------------------

py_test(
    name = "test_dataset_reader",
    tags = ["team:rllib", "offline"],
    size = "small",
    srcs = ["offline/tests/test_dataset_reader.py"],
    data = [
        "tests/data/pendulum/large.json",
        "tests/data/pendulum/enormous.zip",
    ],
)

py_test(
    name = "test_feature_importance",
    tags = ["team:rllib", "offline", "torch_only"],
    size = "medium",
    srcs = ["offline/tests/test_feature_importance.py"]
)

py_test(
    name = "test_json_reader",
    tags = ["team:rllib", "offline"],
    size = "small",
    srcs = ["offline/tests/test_json_reader.py"],
    data = ["tests/data/pendulum/large.json"],
)

py_test(
    name = "test_ope",
    tags = ["team:rllib", "offline"],
    size = "medium",
    srcs = ["offline/estimators/tests/test_ope.py"],
    data = ["tests/data/cartpole/small.json"],
)

py_test(
    name = "test_ope_math",
    tags = ["team:rllib", "offline"],
    size = "small",
    srcs = ["offline/estimators/tests/test_ope_math.py"]
)

py_test(
    name = "test_dm_learning",
    tags = ["team:rllib", "offline"],
    size = "large",
    srcs = ["offline/estimators/tests/test_dm_learning.py"],
)

py_test(
    name = "test_dr_learning",
    tags = ["team:rllib", "offline"],
    size = "large",
    srcs = ["offline/estimators/tests/test_dr_learning.py"],
)

# --------------------------------------------------------------------
# Policies
# rllib/policy/
#
# Tag: policy
# --------------------------------------------------------------------

py_test(
    name = "policy/tests/test_compute_log_likelihoods",
    tags = ["team:rllib", "policy"],
    size = "medium",
    srcs = ["policy/tests/test_compute_log_likelihoods.py"]
)

py_test(
    name = "policy/tests/test_export_checkpoint_and_model",
    tags = ["team:rllib", "policy"],
    size = "large",
    srcs = ["policy/tests/test_export_checkpoint_and_model.py"]
)

py_test(
    name = "policy/tests/test_multi_agent_batch",
    tags = ["team:rllib", "policy"],
    size = "small",
    srcs = ["policy/tests/test_multi_agent_batch.py"]
)

py_test(
    name = "policy/tests/test_policy",
    tags = ["team:rllib", "policy"],
    size = "medium",
    srcs = ["policy/tests/test_policy.py"]
)

py_test(
    name = "policy/tests/test_policy_map",
    tags = ["team:rllib", "policy"],
    size = "medium",
    srcs = ["policy/tests/test_policy_map.py"]
)

py_test(
    name = "policy/tests/test_policy_state_swapping",
    tags = ["team:rllib", "policy", "gpu"],
    size = "medium",
    srcs = ["policy/tests/test_policy_state_swapping.py"]
)

py_test(
    name = "policy/tests/test_rnn_sequencing",
    tags = ["team:rllib", "policy"],
    size = "small",
    srcs = ["policy/tests/test_rnn_sequencing.py"]
)

py_test(
    name = "policy/tests/test_sample_batch",
    tags = ["team:rllib", "policy", "gpu"],
    size = "small",
    srcs = ["policy/tests/test_sample_batch.py"]
)

py_test(
    name = "policy/tests/test_view_requirement",
    tags = ["team:rllib", "policy"],
    size = "small",
    srcs = ["policy/tests/test_view_requirement.py"]
)


# --------------------------------------------------------------------
# Utils:
# rllib/utils/
#
# Tag: utils
# --------------------------------------------------------------------

# Checkpoint Utils
py_test(
    name = "test_checkpoint_utils",
    tags = ["team:rllib", "utils"],
    size = "small",
    srcs = ["utils/tests/test_checkpoint_utils.py"]
)

py_test(
    name = "test_errors",
    tags = ["team:rllib", "utils"],
    size = "medium",
    srcs = ["utils/tests/test_errors.py"]
)

py_test(
    name = "test_nested_dict",
    tags = ["team:rllib", "utils"],
    size = "small",
    srcs = ["utils/tests/test_nested_dict.py"]
)

py_test(
    name = "test_serialization",
    tags = ["team:rllib", "utils"],
    size = "small",
    srcs = ["utils/tests/test_serialization.py"]
)

py_test(
    name = "test_curiosity",
    tags = ["team:rllib", "utils"],
    size = "large",
    srcs = ["utils/exploration/tests/test_curiosity.py"]
)

py_test(
    name = "test_explorations",
    tags = ["team:rllib", "utils"],
    size = "large",
    srcs = ["utils/exploration/tests/test_explorations.py"]
)

py_test(
    name = "test_parameter_noise",
    tags = ["team:rllib", "utils"],
    size = "medium",
    srcs = ["utils/exploration/tests/test_parameter_noise.py"]
)

py_test(
    name = "test_random_encoder",
    tags = ["team:rllib", "utils"],
    size = "large",
    srcs = ["utils/exploration/tests/test_random_encoder.py"]
)

py_test(
    name = "utils/tests/test_torch_utils",
    tags = ["team:rllib", "policutils"],
    size = "small",
    srcs = ["utils/tests/test_torch_utils.py"]
)

# Schedules
py_test(
    name = "test_schedules",
    tags = ["team:rllib", "utils"],
    size = "small",
    srcs = ["utils/schedules/tests/test_schedules.py"]
)

py_test(
    name = "test_framework_agnostic_components",
    tags = ["team:rllib", "utils"],
    size = "small",
    data = glob(["utils/tests/**"]),
    srcs = ["utils/tests/test_framework_agnostic_components.py"]
)

# Spaces/Space utils.
py_test(
    name = "test_space_utils",
    tags = ["team:rllib", "utils"],
    size = "small",
    srcs = ["utils/spaces/tests/test_space_utils.py"]
)

# TaskPool
py_test(
    name = "test_taskpool",
    tags = ["team:rllib", "utils"],
    size = "small",
    srcs = ["utils/tests/test_taskpool.py"]
)

# ReplayBuffers
py_test(
    name = "test_multi_agent_mixin_replay_buffer",
    tags = ["team:rllib", "utils"],
    size = "small",
    srcs = ["utils/replay_buffers/tests/test_multi_agent_mixin_replay_buffer.py"]
)

py_test(
    name = "test_multi_agent_prioritized_replay_buffer",
    tags = ["team:rllib", "utils"],
    size = "small",
    srcs = ["utils/replay_buffers/tests/test_multi_agent_prioritized_replay_buffer.py"]
)

py_test(
    name = "test_multi_agent_replay_buffer",
    tags = ["team:rllib", "utils"],
    size = "small",
    srcs = ["utils/replay_buffers/tests/test_multi_agent_replay_buffer.py"]
)

py_test(
    name = "test_prioritized_replay_buffer_replay_buffer_api",
    tags = ["team:rllib", "utils"],
    size = "small",
    srcs = ["utils/replay_buffers/tests/test_prioritized_replay_buffer_replay_buffer_api.py"]
)

py_test(
    name = "test_replay_buffer",
    tags = ["team:rllib", "utils"],
    size = "small",
    srcs = ["utils/replay_buffers/tests/test_replay_buffer.py"]
)

py_test(
    name = "test_fifo_replay_buffer",
    tags = ["team:rllib", "utils"],
    size = "small",
    srcs = ["utils/replay_buffers/tests/test_fifo_replay_buffer.py"]
)

py_test(
    name = "test_reservoir_buffer",
    tags = ["team:rllib", "utils"],
    size = "small",
    srcs = ["utils/replay_buffers/tests/test_reservoir_buffer.py"]
)

py_test(
    name = "test_segment_tree_replay_buffer_api",
    tags = ["team:rllib", "utils"],
    size = "small",
    srcs = ["utils/replay_buffers/tests/test_segment_tree_replay_buffer_api.py"]
)

py_test(
    name = "test_check_env",
    tags = ["team:rllib", "utils"],
    size = "small",
    srcs = ["utils/tests/test_check_env.py"]
)

py_test(
    name = "test_check_multi_agent",
    tags = ["team:rllib", "utils"],
    size = "small",
    srcs = ["utils/tests/test_check_multi_agent.py"]
)

py_test(
    name = "test_actor_manager",
    tags = ["team:rllib", "utils", "exclusive"],
    size = "medium",
    srcs = ["utils/tests/test_actor_manager.py"],
    data = ["utils/tests/random_numbers.pkl"],
)

# --------------------------------------------------------------------
# rllib/tests/ directory
#
# Tag: tests_dir
#
# NOTE: Add tests alphabetically into this list.
# --------------------------------------------------------------------

py_test(
    name = "tests/backward_compat/test_backward_compat",
    tags = ["team:rllib", "tests_dir"],
    size = "medium",
    srcs = ["tests/backward_compat/test_backward_compat.py"],
    data = glob(["tests/backward_compat/checkpoints/**"]),
)

py_test(
    name = "tests/test_algorithm_imports",
    tags = ["team:rllib", "tests_dir"],
    size = "small",
    srcs = ["tests/test_algorithm_imports.py"]
)

py_test(
    name = "tests/test_catalog",
    tags = ["team:rllib", "tests_dir"],
    size = "medium",
    srcs = ["tests/test_catalog.py"]
)

py_test(
    name = "tests/test_checkpoint_restore_pg",
    main = "tests/test_algorithm_checkpoint_restore.py",
    tags = ["team:rllib", "tests_dir"],
    size = "large",
    srcs = ["tests/test_algorithm_checkpoint_restore.py"],
    args = ["TestCheckpointRestorePG"]
)

py_test(
    name = "tests/test_checkpoint_restore_off_policy",
    main = "tests/test_algorithm_checkpoint_restore.py",
    tags = ["team:rllib", "tests_dir"],
    size = "large",
    srcs = ["tests/test_algorithm_checkpoint_restore.py"],
    args = ["TestCheckpointRestoreOffPolicy"]
)

py_test(
    name = "tests/test_checkpoint_restore_evolution_algos",
    main = "tests/test_algorithm_checkpoint_restore.py",
    tags = ["team:rllib", "tests_dir"],
    size = "medium",
    srcs = ["tests/test_algorithm_checkpoint_restore.py"],
    args = ["TestCheckpointRestoreEvolutionAlgos"]
)

py_test(
    name = "policy/tests/test_policy_checkpoint_restore",
    main = "policy/tests/test_policy_checkpoint_restore.py",
    tags = ["team:rllib", "tests_dir"],
    size = "large",
    data = glob([
        "tests/data/checkpoints/APPO_CartPole-v1-connector-enabled/**",
    ]),
    srcs = ["policy/tests/test_policy_checkpoint_restore.py"],
)

py_test(
    name = "tests/test_custom_resource",
    tags = ["team:rllib", "tests_dir"],
    size = "large", # bazel may complain about it being too long sometimes - large is on purpose as some frameworks take longer
    srcs = ["tests/test_custom_resource.py"]
)

py_test(
    name = "tests/test_dependency_tf",
    tags = ["team:rllib", "tests_dir"],
    size = "small",
    srcs = ["tests/test_dependency_tf.py"]
)

py_test(
    name = "tests/test_dependency_torch",
    tags = ["team:rllib", "tests_dir"],
    size = "small",
    srcs = ["tests/test_dependency_torch.py"]
)

py_test(
    name = "tests/test_eager_support_pg",
    main = "tests/test_eager_support.py",
    tags = ["team:rllib", "tests_dir"],
    size = "small",
    srcs = ["tests/test_eager_support.py"],
    args = ["TestEagerSupportPG"]
)

py_test(
    name = "tests/test_eager_support_off_policy",
    main = "tests/test_eager_support.py",
    tags = ["team:rllib", "tests_dir"],
    size = "small",
    srcs = ["tests/test_eager_support.py"],
    args = ["TestEagerSupportOffPolicy"]
)

py_test(
    name = "tests/test_filters",
    tags = ["team:rllib", "tests_dir"],
    size = "small",
    srcs = ["tests/test_filters.py"]
)

py_test(
    name = "tests/test_gpus",
    tags = ["team:rllib", "tests_dir"],
    size = "large",
    srcs = ["tests/test_gpus.py"]
)

py_test(
    name = "tests/test_io",
    tags = ["team:rllib", "tests_dir"],
    size = "large",
    srcs = ["tests/test_io.py"]
)

py_test(
    name = "tests/test_local",
    tags = ["team:rllib", "tests_dir"],
    size = "small",
    srcs = ["tests/test_local.py"]
)

py_test(
    name = "tests/test_lstm",
    tags = ["team:rllib", "tests_dir"],
    size = "medium",
    srcs = ["tests/test_lstm.py"]
)

py_test(
    name = "tests/test_model_imports",
    tags = ["team:rllib", "tests_dir", "model_imports"],
    size = "medium",
    data = glob(["tests/data/model_weights/**"]),
    srcs = ["tests/test_model_imports.py"]
)

py_test(
    name = "tests/test_nested_action_spaces",
    main = "tests/test_nested_action_spaces.py",
    tags = ["team:rllib", "tests_dir"],
    size = "medium",
    srcs = ["tests/test_nested_action_spaces.py"]
)

py_test(
    name = "tests/test_nested_observation_spaces",
    main = "tests/test_nested_observation_spaces.py",
    tags = ["team:rllib", "tests_dir"],
    size = "medium",
    srcs = ["tests/test_nested_observation_spaces.py"]
)

py_test(
    name = "tests/test_nn_framework_import_errors",
    tags = ["team:rllib", "tests_dir"],
    size = "small",
    srcs = ["tests/test_nn_framework_import_errors.py"]
)

py_test(
    name = "tests/test_pettingzoo_env",
    tags = ["team:rllib", "tests_dir"],
    size = "medium",
    srcs = ["tests/test_pettingzoo_env.py"]
)

py_test(
    name = "tests/test_placement_groups",
    tags = ["team:rllib", "tests_dir"],
    size = "large", # bazel may complain about it being too long sometimes - large is on purpose as some frameworks take longer
    srcs = ["tests/test_placement_groups.py"]
)

py_test(
    name = "tests/test_ray_client",
    tags = ["team:rllib", "tests_dir"],
    size = "large",
    srcs = ["tests/test_ray_client.py"]
)

py_test(
    name = "tests/test_reproducibility",
    tags = ["team:rllib", "tests_dir"],
    size = "medium",
    srcs = ["tests/test_reproducibility.py"]
)

# Test [train|evaluate].py scripts (w/o confirming evaluation performance).
py_test(
    name = "test_rllib_evaluate_1",
    main = "tests/test_rllib_train_and_evaluate.py",
    tags = ["team:rllib", "tests_dir"],
    size = "large",
    data = ["train.py", "evaluate.py"],
    srcs = ["tests/test_rllib_train_and_evaluate.py"],
    args = ["TestEvaluate1"]
)

py_test(
    name = "test_rllib_evaluate_2",
    main = "tests/test_rllib_train_and_evaluate.py",
    tags = ["team:rllib", "tests_dir"],
    size = "large",
    data = ["train.py", "evaluate.py"],
    srcs = ["tests/test_rllib_train_and_evaluate.py"],
    args = ["TestEvaluate2"]
)

py_test(
    name = "test_rllib_evaluate_3",
    main = "tests/test_rllib_train_and_evaluate.py",
    tags = ["team:rllib", "tests_dir"],
    size = "large",
    data = ["train.py", "evaluate.py"],
    srcs = ["tests/test_rllib_train_and_evaluate.py"],
    args = ["TestEvaluate3"]
)

py_test(
    name = "test_rllib_evaluate_4",
    main = "tests/test_rllib_train_and_evaluate.py",
    tags = ["team:rllib", "tests_dir"],
    size = "large",
    data = ["train.py", "evaluate.py"],
    srcs = ["tests/test_rllib_train_and_evaluate.py"],
    args = ["TestEvaluate4"]
)

# Test [train|evaluate].py scripts (and confirm `rllib evaluate` performance is same
# as the final one from the `rllib train` run).
py_test(
    name = "test_rllib_train_and_evaluate",
    main = "tests/test_rllib_train_and_evaluate.py",
    tags = ["team:rllib", "tests_dir"],
    size = "large",
    data = ["train.py", "evaluate.py"],
    srcs = ["tests/test_rllib_train_and_evaluate.py"],
    args = ["TestTrainAndEvaluate"]
)

py_test(
    name = "tests/test_supported_multi_agent_pg",
    main = "tests/test_supported_multi_agent.py",
    tags = ["team:rllib", "tests_dir"],
    size = "large",
    srcs = ["tests/test_supported_multi_agent.py"],
    args = ["TestSupportedMultiAgentPG"]
)

py_test(
    name = "tests/test_supported_multi_agent_off_policy",
    main = "tests/test_supported_multi_agent.py",
    tags = ["team:rllib", "tests_dir"],
    size = "large",
    srcs = ["tests/test_supported_multi_agent.py"],
    args = ["TestSupportedMultiAgentOffPolicy"]
)

py_test(
     name = "tests/test_supported_spaces_pg",
     main = "tests/test_supported_spaces.py",
     tags = ["team:rllib", "tests_dir"],
     size = "large",
     srcs = ["tests/test_supported_spaces.py"],
     args = ["TestSupportedSpacesPG"]
 )

py_test(
    name = "tests/test_supported_spaces_off_policy",
    main = "tests/test_supported_spaces.py",
    tags = ["team:rllib", "tests_dir"],
    size = "medium",
    srcs = ["tests/test_supported_spaces.py"],
    args = ["TestSupportedSpacesOffPolicy"]
)

py_test(
    name = "tests/test_supported_spaces_evolution_algos",
    main = "tests/test_supported_spaces.py",
    tags = ["team:rllib", "tests_dir"],
    size = "large",
    srcs = ["tests/test_supported_spaces.py"],
    args = ["TestSupportedSpacesEvolutionAlgos"]
)

py_test(
    name = "tests/test_timesteps",
    tags = ["team:rllib", "tests_dir"],
    size = "small",
    srcs = ["tests/test_timesteps.py"]
)

# --------------------------------------------------------------------
# examples/ directory (excluding examples/documentation/...)
#
# Tag: examples
#
# NOTE: Add tests alphabetically into this list.
# --------------------------------------------------------------------

py_test(
    name = "examples/action_masking_tf",
    main = "examples/action_masking.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "small",
    srcs = ["examples/action_masking.py"],
    args = ["--stop-iter=2"]
)

py_test(
    name = "examples/action_masking_torch",
    main = "examples/action_masking.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "small",
    srcs = ["examples/action_masking.py"],
    args = ["--stop-iter=2", "--framework=torch"]
)

py_test(
    name = "examples/attention_net_tf",
    main = "examples/attention_net.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/attention_net.py"],
    args = ["--as-test", "--stop-reward=70"]
)

py_test(
    name = "examples/attention_net_torch",
    main = "examples/attention_net.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/attention_net.py"],
    args = ["--as-test", "--stop-reward=70", "--framework torch"]
)

py_test(
    name = "examples/autoregressive_action_dist_tf",
    main = "examples/autoregressive_action_dist.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/autoregressive_action_dist.py"],
    args = ["--as-test", "--stop-reward=150", "--num-cpus=4"]
)

py_test(
    name = "examples/autoregressive_action_dist_torch",
    main = "examples/autoregressive_action_dist.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/autoregressive_action_dist.py"],
    args = ["--as-test", "--framework=torch", "--stop-reward=150", "--num-cpus=4"]
)

py_test(
    name = "examples/bare_metal_policy_with_custom_view_reqs",
    main = "examples/bare_metal_policy_with_custom_view_reqs.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "small",
    srcs = ["examples/bare_metal_policy_with_custom_view_reqs.py"],
)

py_test(
    name = "examples/batch_norm_model_ppo_tf",
    main = "examples/batch_norm_model.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/batch_norm_model.py"],
    args = ["--as-test", "--run=PPO", "--stop-reward=80"]
)

py_test(
    name = "examples/batch_norm_model_ppo_torch",
    main = "examples/batch_norm_model.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/batch_norm_model.py"],
    args = ["--as-test", "--framework=torch", "--run=PPO", "--stop-reward=80"]
)

py_test(
    name = "examples/batch_norm_model_dqn_tf",
    main = "examples/batch_norm_model.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/batch_norm_model.py"],
    args = ["--as-test", "--run=DQN", "--stop-reward=70"]
)

py_test(
    name = "examples/batch_norm_model_dqn_torch",
    main = "examples/batch_norm_model.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",  # DQN learns much slower with BatchNorm.
    srcs = ["examples/batch_norm_model.py"],
    args = ["--as-test", "--framework=torch", "--run=DQN", "--stop-reward=70"]
)

py_test(
    name = "examples/batch_norm_model_ddpg_tf",
    main = "examples/batch_norm_model.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "small",
    srcs = ["examples/batch_norm_model.py"],
    args = ["--run=DDPG", "--stop-iters=1"]
)

py_test(
    name = "examples/batch_norm_model_ddpg_torch",
    main = "examples/batch_norm_model.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "small",
    srcs = ["examples/batch_norm_model.py"],
    args = ["--framework=torch", "--run=DDPG", "--stop-iters=1"]
)

py_test(
    name = "examples/cartpole_lstm_impala_tf",
    main = "examples/cartpole_lstm.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/cartpole_lstm.py"],
    args = ["--as-test", "--run=IMPALA", "--stop-reward=40", "--num-cpus=4"]
)

py_test(
    name = "examples/cartpole_lstm_impala_torch",
    main = "examples/cartpole_lstm.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/cartpole_lstm.py"],
    args = ["--as-test", "--framework=torch", "--run=IMPALA", "--stop-reward=40", "--num-cpus=4"]
)

py_test(
    name = "examples/cartpole_lstm_ppo_tf",
    main = "examples/cartpole_lstm.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/cartpole_lstm.py"],
    args = ["--as-test", "--framework=tf", "--run=PPO", "--stop-reward=40", "--num-cpus=4"]
)

py_test(
    name = "examples/cartpole_lstm_ppo_tf2",
    main = "examples/cartpole_lstm.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/cartpole_lstm.py"],
    args = ["--as-test", "--framework=tf2", "--run=PPO", "--stop-reward=40", "--num-cpus=4"]
)

py_test(
    name = "examples/cartpole_lstm_ppo_torch",
    main = "examples/cartpole_lstm.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/cartpole_lstm.py"],
    args = ["--as-test", "--framework=torch", "--run=PPO", "--stop-reward=40", "--num-cpus=4"]
)

py_test(
    name = "examples/cartpole_lstm_ppo_tf_with_prev_a_and_r",
    main = "examples/cartpole_lstm.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/cartpole_lstm.py"],
    args = ["--as-test", "--run=PPO", "--stop-reward=40", "--use-prev-action",  "--use-prev-reward", "--num-cpus=4"]
)

py_test(
    name = "examples/centralized_critic_tf",
    main = "examples/centralized_critic.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/centralized_critic.py"],
    args = ["--as-test", "--stop-reward=7.2"]
)

py_test(
    name = "examples/centralized_critic_torch",
    main = "examples/centralized_critic.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/centralized_critic.py"],
    args = ["--as-test", "--framework=torch", "--stop-reward=7.2"]
)

py_test(
    name = "examples/centralized_critic_2_tf",
    main = "examples/centralized_critic_2.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/centralized_critic_2.py"],
    args = ["--as-test", "--stop-reward=6.0"]
)

py_test(
    name = "examples/centralized_critic_2_torch",
    main = "examples/centralized_critic_2.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/centralized_critic_2.py"],
    args = ["--as-test", "--framework=torch", "--stop-reward=6.0"]
)

py_test(
    name = "examples/checkpoint_by_custom_criteria",
    main = "examples/checkpoint_by_custom_criteria.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/checkpoint_by_custom_criteria.py"],
    args = ["--stop-iters=3 --num-cpus=3"]
)

py_test(
    name = "examples/complex_struct_space_tf",
    main = "examples/complex_struct_space.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "small",
    srcs = ["examples/complex_struct_space.py"],
    args = ["--framework=tf"],
)

py_test(
    name = "examples/complex_struct_space_tf_eager",
    main = "examples/complex_struct_space.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "small",
    srcs = ["examples/complex_struct_space.py"],
    args = ["--framework=tf2"],
)

py_test(
    name = "examples/complex_struct_space_torch",
    main = "examples/complex_struct_space.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "small",
    srcs = ["examples/complex_struct_space.py"],
    args = ["--framework=torch"],
)

py_test(
    name = "examples/curriculum_learning",
    main = "examples/curriculum_learning.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/curriculum_learning.py"],
    args = ["--as-test", "--stop-reward=800.0"]
)

py_test(
    name = "examples/custom_env_tf",
    main = "examples/custom_env.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/custom_env.py"],
    args = ["--as-test"]
)

py_test(
    name = "examples/custom_env_torch",
    main = "examples/custom_env.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/custom_env.py"],
    args = ["--as-test", "--framework=torch"]
)

py_test(
    name = "examples/custom_eval_tf",
    main = "examples/custom_eval.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "small",
    srcs = ["examples/custom_eval.py"],
    args = ["--num-cpus=4", "--as-test"]
)

py_test(
    name = "examples/custom_eval_torch",
    main = "examples/custom_eval.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "small",
    srcs = ["examples/custom_eval.py"],
    args = ["--num-cpus=4", "--as-test", "--framework=torch"]
)

py_test(
    name = "examples/custom_eval_parallel_to_training_torch",
    main = "examples/custom_eval.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "small",
    srcs = ["examples/custom_eval.py"],
    args = ["--num-cpus=4", "--as-test", "--framework=torch", "--evaluation-parallel-to-training"]
)

py_test(
    name = "examples/custom_experiment",
    main = "examples/custom_experiment.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/custom_experiment.py"],
    args = ["--train-iterations=10"]
)

py_test(
    name = "examples/custom_fast_model_tf",
    main = "examples/custom_fast_model.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/custom_fast_model.py"],
    args = ["--stop-iters=1"]
)

py_test(
    name = "examples/custom_fast_model_torch",
    main = "examples/custom_fast_model.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/custom_fast_model.py"],
    args = ["--stop-iters=1", "--framework=torch"]
)

py_test(
    name = "examples/custom_keras_model_a2c",
    main = "examples/custom_keras_model.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "small",
    srcs = ["examples/custom_keras_model.py"],
    args = ["--run=A2C", "--stop=50", "--num-cpus=4"]
)

py_test(
    name = "examples/custom_keras_model_dqn",
    main = "examples/custom_keras_model.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "small",
    srcs = ["examples/custom_keras_model.py"],
    args = ["--run=DQN", "--stop=50"]
)

py_test(
    name = "examples/custom_keras_model_ppo",
    main = "examples/custom_keras_model.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "small",
    srcs = ["examples/custom_keras_model.py"],
    args = ["--run=PPO", "--stop=50", "--num-cpus=4"]
)

py_test(
    name = "examples/custom_metrics_and_callbacks",
    main = "examples/custom_metrics_and_callbacks.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "small",
    srcs = ["examples/custom_metrics_and_callbacks.py"],
    args = ["--stop-iters=2"]
)

py_test(
    name = "examples/custom_model_api_tf",
    main = "examples/custom_model_api.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "small",
    srcs = ["examples/custom_model_api.py"],
)

py_test(
    name = "examples/custom_model_api_torch",
    main = "examples/custom_model_api.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "small",
    srcs = ["examples/custom_model_api.py"],
    args = ["--framework=torch"],
)

py_test(
    name = "examples/custom_model_loss_and_metrics_ppo_tf",
    main = "examples/custom_model_loss_and_metrics.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "small",
    # Include the json data file.
    data = ["tests/data/cartpole/small.json"],
    srcs = ["examples/custom_model_loss_and_metrics.py"],
    args = ["--run=PPO", "--stop-iters=1", "--input-files=tests/data/cartpole"]
)

py_test(
    name = "examples/custom_model_loss_and_metrics_ppo_torch",
    main = "examples/custom_model_loss_and_metrics.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "small",
    # Include the json data file.
    data = ["tests/data/cartpole/small.json"],
    srcs = ["examples/custom_model_loss_and_metrics.py"],
    args = ["--run=PPO", "--framework=torch", "--stop-iters=1", "--input-files=tests/data/cartpole"]
)

py_test(
    name = "examples/custom_model_loss_and_metrics_pg_tf",
    main = "examples/custom_model_loss_and_metrics.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "small",
    # Include the json data file.
    data = ["tests/data/cartpole/small.json"],
    srcs = ["examples/custom_model_loss_and_metrics.py"],
    args = ["--run=PG", "--stop-iters=1", "--input-files=tests/data/cartpole"]
)

py_test(
    name = "examples/custom_model_loss_and_metrics_pg_torch",
    main = "examples/custom_model_loss_and_metrics.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "small",
    # Include the json data file.
    data = ["tests/data/cartpole/small.json"],
    srcs = ["examples/custom_model_loss_and_metrics.py"],
    args = ["--run=PG", "--framework=torch", "--stop-iters=1", "--input-files=tests/data/cartpole"]
)

py_test(
    name = "examples/custom_observation_filters",
    main = "examples/custom_observation_filters.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/custom_observation_filters.py"],
    args = ["--stop-iters=3"]
)

py_test(
    name = "examples/custom_rnn_model_repeat_after_me_tf",
    main = "examples/custom_rnn_model.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/custom_rnn_model.py"],
    args = ["--as-test", "--run=PPO", "--stop-reward=40", "--env=RepeatAfterMeEnv", "--num-cpus=4"]
)

py_test(
    name = "examples/custom_rnn_model_repeat_initial_obs_tf",
    main = "examples/custom_rnn_model.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/custom_rnn_model.py"],
    args = ["--as-test", "--run=PPO", "--stop-reward=10", "--stop-timesteps=300000", "--env=RepeatInitialObsEnv", "--num-cpus=4"]
)

py_test(
    name = "examples/custom_rnn_model_repeat_after_me_torch",
    main = "examples/custom_rnn_model.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/custom_rnn_model.py"],
    args = ["--as-test", "--framework=torch", "--run=PPO", "--stop-reward=40", "--env=RepeatAfterMeEnv", "--num-cpus=4"]
)

py_test(
    name = "examples/custom_rnn_model_repeat_initial_obs_torch",
    main = "examples/custom_rnn_model.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/custom_rnn_model.py"],
    args = ["--as-test", "--framework=torch", "--run=PPO", "--stop-reward=10", "--stop-timesteps=300000", "--env=RepeatInitialObsEnv", "--num-cpus=4"]
)

py_test(
    name = "examples/custom_tf_policy",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "small",
    srcs = ["examples/custom_tf_policy.py"],
    args = ["--stop-iters=2", "--num-cpus=4"]
)

py_test(
    name = "examples/custom_torch_policy",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "small",
    srcs = ["examples/custom_torch_policy.py"],
    args = ["--stop-iters=2", "--num-cpus=4"]
)

py_test(
    name = "examples/custom_train_fn",
    main = "examples/custom_train_fn.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/custom_train_fn.py"],
)

py_test(
    name = "examples/custom_vector_env_tf",
    main = "examples/custom_vector_env.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/custom_vector_env.py"],
    args = ["--as-test", "--stop-reward=40.0"]
)

py_test(
    name = "examples/custom_vector_env_torch",
    main = "examples/custom_vector_env.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/custom_vector_env.py"],
    args = ["--as-test", "--framework=torch", "--stop-reward=40.0"]
)

py_test(
    name = "examples/deterministic_training_tf",
    main = "examples/deterministic_training.py",
    tags = ["team:rllib", "exclusive", "multi_gpu", "examples"],
    size = "medium",
    srcs = ["examples/deterministic_training.py"],
    args = ["--as-test", "--stop-iters=1", "--framework=tf", "--num-gpus=1", "--num-gpus-per-worker=1"]
)

py_test(
    name = "examples/deterministic_training_tf2",
    main = "examples/deterministic_training.py",
    tags = ["team:rllib", "exclusive", "multi_gpu", "examples"],
    size = "medium",
    srcs = ["examples/deterministic_training.py"],
    args = ["--as-test", "--stop-iters=1", "--framework=tf2", "--num-gpus=1", "--num-gpus-per-worker=1"]
)

py_test(
    name = "examples/deterministic_training_torch",
    main = "examples/deterministic_training.py",
    tags = ["team:rllib", "exclusive", "multi_gpu", "examples"],
    size = "medium",
    srcs = ["examples/deterministic_training.py"],
    args = ["--as-test", "--stop-iters=1", "--framework=torch", "--num-gpus=1", "--num-gpus-per-worker=1"]
)

py_test(
    name = "examples/eager_execution",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "small",
    srcs = ["examples/eager_execution.py"],
    args = ["--stop-iters=2"]
)

py_test(
    name = "examples/export/cartpole_dqn_export",
    main = "examples/export/cartpole_dqn_export.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "small",
    srcs = ["examples/export/cartpole_dqn_export.py"],
)

py_test(
    name = "examples/export/onnx_tf",
    main = "examples/export/onnx_tf.py",
    tags = ["team:rllib", "exclusive", "examples", "no_main"],
    size = "small",
    srcs = ["examples/export/onnx_tf.py"],
    args = ["--framework=tf"],
)

py_test(
    name = "examples/export/onnx_tf2",
    main = "examples/export/onnx_tf.py",
    tags = ["team:rllib", "exclusive", "examples", "no_main"],
    size = "small",
    srcs = ["examples/export/onnx_tf.py"],
    args = ["--framework=tf2"],
)

py_test(
    name = "examples/export/onnx_torch",
    main = "examples/export/onnx_torch.py",
    tags = ["team:rllib", "exclusive", "examples", "no_main"],
    size = "small",
    srcs = ["examples/export/onnx_torch.py"],
)

py_test(
    name = "examples/fractional_gpus",
    main = "examples/fractional_gpus.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/fractional_gpus.py"],
    args = ["--as-test", "--stop-reward=40.0", "--num-gpus=0", "--num-workers=0"]
)

py_test(
    name = "examples/hierarchical_training_tf",
    main = "examples/hierarchical_training.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/hierarchical_training.py"],
    args = ["--stop-reward=0.0"]
)

py_test(
    name = "examples/hierarchical_training_torch",
    main = "examples/hierarchical_training.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/hierarchical_training.py"],
    args = ["--framework=torch", "--stop-reward=0.0"]
)

# Do not run this test (MobileNetV2 is gigantic and takes forever for 1 iter).
# py_test(
#     name = "examples/mobilenet_v2_with_lstm_tf",
#     main = "examples/mobilenet_v2_with_lstm.py",
#     tags = ["team:rllib", "examples"],
#     size = "small",
#     srcs = ["examples/mobilenet_v2_with_lstm.py"]
# )

py_test(
    name = "examples/multi_agent_cartpole_tf",
    main = "examples/multi_agent_cartpole.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "small",
    srcs = ["examples/multi_agent_cartpole.py"],
    args = ["--as-test", "--stop-reward=70.0", "--num-cpus=4"]
)

py_test(
    name = "examples/multi_agent_cartpole_torch",
    main = "examples/multi_agent_cartpole.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "small",
    srcs = ["examples/multi_agent_cartpole.py"],
    args = ["--as-test", "--framework=torch", "--stop-reward=70.0", "--num-cpus=4"]
)

py_test(
    name = "examples/multi_agent_custom_policy_tf",
    main = "examples/multi_agent_custom_policy.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "small",
    srcs = ["examples/multi_agent_custom_policy.py"],
    args = ["--as-test", "--stop-reward=80"]
)

py_test(
    name = "examples/multi_agent_custom_policy_torch",
    main = "examples/multi_agent_custom_policy.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "small",
    srcs = ["examples/multi_agent_custom_policy.py"],
    args = ["--as-test", "--framework=torch", "--stop-reward=80"]
)

py_test(
    name = "examples/multi_agent_different_spaces_for_agents_tf2",
    main = "examples/multi_agent_different_spaces_for_agents.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/multi_agent_different_spaces_for_agents.py"],
    args = ["--stop-iters=4", "--framework=tf2", "--eager-tracing"]
)

py_test(
    name = "examples/multi_agent_different_spaces_for_agents_torch",
    main = "examples/multi_agent_different_spaces_for_agents.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/multi_agent_different_spaces_for_agents.py"],
    args = ["--stop-iters=4", "--framework=torch"]
)

py_test(
    name = "examples/multi_agent_two_trainers_tf",
    main = "examples/multi_agent_two_trainers.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/multi_agent_two_trainers.py"],
    args = ["--as-test", "--stop-reward=70"]
)

py_test(
    name = "examples/multi_agent_two_trainers_torch",
    main = "examples/multi_agent_two_trainers.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "small",
    srcs = ["examples/multi_agent_two_trainers.py"],
    args = ["--as-test", "--framework=torch", "--stop-reward=70"]
)

py_test(
    name = "examples/offline_rl_torch",
    main = "examples/offline_rl.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/offline_rl.py"],
    args = ["--as-test", "--stop-reward=-300", "--stop-iters=1"]
)

# Taking out this test for now: Mixed torch- and tf- policies within the same
# Trainer never really worked.
# py_test(
#     name = "examples/multi_agent_two_trainers_mixed_torch_tf",
#     main = "examples/multi_agent_two_trainers.py",
#     tags = ["team:rllib", "exclusive", "examples"],
#     size = "medium",
#     srcs = ["examples/multi_agent_two_trainers.py"],
#     args = ["--as-test", "--mixed-torch-tf", "--stop-reward=70"]
# )

py_test(
    name = "examples/nested_action_spaces_ppo_tf",
    main = "examples/nested_action_spaces.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/nested_action_spaces.py"],
    args = ["--as-test", "--stop-reward=-600", "--run=PPO"]
)

py_test(
    name = "examples/nested_action_spaces_ppo_torch",
    main = "examples/nested_action_spaces.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/nested_action_spaces.py"],
    args = ["--as-test", "--framework=torch", "--stop-reward=-600", "--run=PPO"]
)

py_test(
    name = "examples/parallel_evaluation_and_training_13_episodes_tf",
    main = "examples/parallel_evaluation_and_training.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/parallel_evaluation_and_training.py"],
    args = ["--as-test", "--stop-reward=50.0", "--num-cpus=6", "--evaluation-duration=13"]
)

py_test(
    name = "examples/parallel_evaluation_and_training_auto_episodes_tf",
    main = "examples/parallel_evaluation_and_training.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/parallel_evaluation_and_training.py"],
    args = ["--as-test", "--stop-reward=50.0", "--num-cpus=6", "--evaluation-duration=auto"]
)

py_test(
    name = "examples/parallel_evaluation_and_training_211_ts_tf2",
    main = "examples/parallel_evaluation_and_training.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/parallel_evaluation_and_training.py"],
    args = ["--as-test", "--framework=tf2", "--stop-reward=30.0", "--num-cpus=6", "--evaluation-num-workers=3", "--evaluation-duration=211", "--evaluation-duration-unit=timesteps"]
)

py_test(
    name = "examples/parallel_evaluation_and_training_auto_ts_torch",
    main = "examples/parallel_evaluation_and_training.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/parallel_evaluation_and_training.py"],
    args = ["--as-test", "--framework=torch", "--stop-reward=30.0", "--num-cpus=6", "--evaluation-num-workers=3", "--evaluation-duration=auto", "--evaluation-duration-unit=timesteps"]
)

py_test(
    name = "examples/parametric_actions_cartpole_pg_tf",
    main = "examples/parametric_actions_cartpole.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "small",
    srcs = ["examples/parametric_actions_cartpole.py"],
    args = ["--as-test", "--stop-reward=60.0", "--run=PG"]
)

py_test(
    name = "examples/parametric_actions_cartpole_dqn_tf",
    main = "examples/parametric_actions_cartpole.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/parametric_actions_cartpole.py"],
    args = ["--as-test", "--stop-reward=60.0", "--run=DQN"]
)

py_test(
    name = "examples/parametric_actions_cartpole_pg_torch",
    main = "examples/parametric_actions_cartpole.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "small",
    srcs = ["examples/parametric_actions_cartpole.py"],
    args = ["--as-test", "--framework=torch", "--stop-reward=60.0", "--run=PG"]
)

py_test(
    name = "examples/parametric_actions_cartpole_dqn_torch",
    main = "examples/parametric_actions_cartpole.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/parametric_actions_cartpole.py"],
    args = ["--as-test", "--framework=torch", "--stop-reward=60.0", "--run=DQN"]
)

py_test(
    name = "examples/parametric_actions_cartpole_embeddings_learnt_by_model",
    main = "examples/parametric_actions_cartpole_embeddings_learnt_by_model.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/parametric_actions_cartpole_embeddings_learnt_by_model.py"],
    args = ["--as-test", "--stop-reward=80.0"]
)

py_test(
    name = "examples/inference_and_serving/policy_inference_after_training_tf",
    main = "examples/inference_and_serving/policy_inference_after_training.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/inference_and_serving/policy_inference_after_training.py"],
    args = ["--stop-iters=3", "--framework=tf"]
)

py_test(
    name = "examples/inference_and_serving/policy_inference_after_training_torch",
    main = "examples/inference_and_serving/policy_inference_after_training.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/inference_and_serving/policy_inference_after_training.py"],
    args = ["--stop-iters=3", "--framework=torch"]
)

py_test(
    name = "examples/inference_and_serving/policy_inference_after_training_with_attention_tf",
    main = "examples/inference_and_serving/policy_inference_after_training_with_attention.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/inference_and_serving/policy_inference_after_training_with_attention.py"],
    args = ["--stop-iters=2", "--framework=tf"]
)

py_test(
    name = "examples/inference_and_serving/policy_inference_after_training_with_attention_torch",
    main = "examples/inference_and_serving/policy_inference_after_training_with_attention.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/inference_and_serving/policy_inference_after_training_with_attention.py"],
    args = ["--stop-iters=2", "--framework=torch"]
)

py_test(
    name = "examples/inference_and_serving/policy_inference_after_training_with_dt_torch",
    main = "examples/inference_and_serving/policy_inference_after_training_with_dt.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/inference_and_serving/policy_inference_after_training_with_dt.py"],
    data = ["tests/data/cartpole/large.json"],
    args = ["--input-files=tests/data/cartpole/large.json"]
)

py_test(
    name = "examples/inference_and_serving/policy_inference_after_training_with_lstm_tf",
    main = "examples/inference_and_serving/policy_inference_after_training_with_lstm.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/inference_and_serving/policy_inference_after_training_with_lstm.py"],
    args = ["--stop-iters=1", "--framework=tf"]
)

py_test(
    name = "examples/inference_and_serving/policy_inference_after_training_with_lstm_torch",
    main = "examples/inference_and_serving/policy_inference_after_training_with_lstm.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/inference_and_serving/policy_inference_after_training_with_lstm.py"],
    args = ["--stop-iters=1", "--framework=torch"]
)

py_test(
    name = "examples/preprocessing_disabled_tf",
    main = "examples/preprocessing_disabled.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/preprocessing_disabled.py"],
    args = ["--stop-iters=2"]
)

py_test(
    name = "examples/preprocessing_disabled_torch",
    main = "examples/preprocessing_disabled.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/preprocessing_disabled.py"],
    args = ["--framework=torch", "--stop-iters=2"]
)

py_test(
    name = "examples/recommender_system_with_recsim_and_slateq_tf2",
    main = "examples/recommender_system_with_recsim_and_slateq.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "large",
    srcs = ["examples/recommender_system_with_recsim_and_slateq.py"],
    args = ["--stop-iters=2", "--num-steps-sampled-before-learning_starts=100", "--framework=tf2", "--use-tune", "--random-test-episodes=10", "--env-num-candidates=50", "--env-slate-size=2"],
)

py_test(
    name = "examples/remote_envs_with_inference_done_on_main_node_tf",
    main = "examples/remote_envs_with_inference_done_on_main_node.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/remote_envs_with_inference_done_on_main_node.py"],
    args = ["--as-test"],
)

py_test(
    name = "examples/remote_envs_with_inference_done_on_main_node_torch",
    main = "examples/remote_envs_with_inference_done_on_main_node.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/remote_envs_with_inference_done_on_main_node.py"],
    args = ["--as-test", "--framework=torch"],
)

# py_test(
#    name = "examples/remote_base_env_with_custom_api",
#    tags = ["team:rllib", "exclusive", "examples"],
#    size = "medium",
#    srcs = ["examples/remote_base_env_with_custom_api.py"],
#    args = ["--stop-iters=3"]
# )

py_test(
    name = "examples/replay_buffer_api",
    tags = ["team:rllib", "examples"],
    size = "large",
    srcs = ["examples/replay_buffer_api.py"],
)

py_test(
    name = "examples/restore_1_of_n_agents_from_checkpoint",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/restore_1_of_n_agents_from_checkpoint.py"],
    args = ["--pre-training-iters=1", "--stop-iters=1", "--num-cpus=4"]
)

py_test(
    name = "examples/rnnsac_stateless_cartpole",
    tags = ["team:rllib", "exclusive", "gpu"],
    size = "medium",
    srcs = ["examples/rnnsac_stateless_cartpole.py"]
)

py_test(
    name = "examples/rollout_worker_custom_workflow",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "small",
    srcs = ["examples/rollout_worker_custom_workflow.py"],
    args = ["--num-cpus=4"]
)

py_test(
    name = "examples/rock_paper_scissors_multiagent_tf",
    main = "examples/rock_paper_scissors_multiagent.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/rock_paper_scissors_multiagent.py"],
    args = ["--as-test"],
)

py_test(
    name = "examples/rock_paper_scissors_multiagent_torch",
    main = "examples/rock_paper_scissors_multiagent.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/rock_paper_scissors_multiagent.py"],
    args = ["--as-test", "--framework=torch"],
)

py_test(
    name = "examples/self_play_with_open_spiel_connect_4_tf",
    main = "examples/self_play_with_open_spiel.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "small",
    srcs = ["examples/self_play_with_open_spiel.py"],
    args = ["--framework=tf", "--env=connect_four", "--win-rate-threshold=0.6", "--stop-iters=2", "--num-episodes-human-play=0"]
)

py_test(
    name = "examples/self_play_with_open_spiel_connect_4_torch",
    main = "examples/self_play_with_open_spiel.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/self_play_with_open_spiel.py"],
    args = ["--framework=torch", "--env=connect_four", "--win-rate-threshold=0.6", "--stop-iters=2", "--num-episodes-human-play=0"]
)

py_test(
    name = "examples/self_play_league_based_with_open_spiel_markov_soccer_tf",
    main = "examples/self_play_league_based_with_open_spiel.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/self_play_league_based_with_open_spiel.py"],
    args = ["--framework=tf", "--env=markov_soccer", "--win-rate-threshold=0.6", "--stop-iters=2", "--num-episodes-human-play=0"]
)

py_test(
    name = "examples/self_play_league_based_with_open_spiel_markov_soccer_torch",
    main = "examples/self_play_league_based_with_open_spiel.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/self_play_league_based_with_open_spiel.py"],
    args = ["--framework=torch", "--env=markov_soccer", "--win-rate-threshold=0.6", "--stop-iters=2", "--num-episodes-human-play=0"]
)

py_test(
    name = "examples/trajectory_view_api_tf",
    main = "examples/trajectory_view_api.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/trajectory_view_api.py"],
    args = ["--as-test", "--framework=tf", "--stop-reward=100.0"]
)

py_test(
    name = "examples/trajectory_view_api_torch",
    main = "examples/trajectory_view_api.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/trajectory_view_api.py"],
    args = ["--as-test", "--framework=torch", "--stop-reward=100.0"]
)

py_test(
    name = "examples/tune/framework",
    main = "examples/tune/framework.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/tune/framework.py"],
    args = ["--smoke-test"]
)

py_test(
    name = "examples/two_trainer_workflow_tf",
    main = "examples/two_trainer_workflow.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/two_trainer_workflow.py"],
    args = ["--as-test", "--stop-reward=450.0"]
)

py_test(
    name = "examples/two_trainer_workflow_torch",
    main = "examples/two_trainer_workflow.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/two_trainer_workflow.py"],
    args = ["--as-test", "--torch", "--stop-reward=450.0"]
)

py_test(
    name = "examples/two_trainer_workflow_mixed_torch_tf",
    main = "examples/two_trainer_workflow.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/two_trainer_workflow.py"],
    args = ["--as-test", "--mixed-torch-tf", "--stop-reward=450.0"]
)

py_test(
    name = "examples/two_step_game_pg_tf",
    main = "examples/two_step_game.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "small",
    srcs = ["examples/two_step_game.py"],
    args = ["--as-test", "--stop-reward=7", "--run=PG"]
)

py_test(
    name = "examples/two_step_game_pg_torch",
    main = "examples/two_step_game.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "small",
    srcs = ["examples/two_step_game.py"],
    args = ["--as-test", "--framework=torch", "--stop-reward=7", "--run=PG"]
)


py_test(
    name = "examples/bandit/lin_ts_train_wheel_env",
    main = "examples/bandit/lin_ts_train_wheel_env.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "small",
    srcs = ["examples/bandit/lin_ts_train_wheel_env.py"],
)

py_test(
    name = "examples/bandit/tune_lin_ts_train_wheel_env",
    main = "examples/bandit/tune_lin_ts_train_wheel_env.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "small",
    srcs = ["examples/bandit/tune_lin_ts_train_wheel_env.py"],
)

py_test(
    name = "examples/bandit/tune_lin_ucb_train_recommendation",
    main = "examples/bandit/tune_lin_ucb_train_recommendation.py",
    tags = ["team:rllib","exclusive",  "examples"],
    size = "small",
    srcs = ["examples/bandit/tune_lin_ucb_train_recommendation.py"],
)

py_test(
    name = "examples/bandit/tune_lin_ucb_train_recsim_env",
    main = "examples/bandit/tune_lin_ucb_train_recsim_env.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "small",
    srcs = ["examples/bandit/tune_lin_ucb_train_recsim_env.py"],
)

py_test(
    name = "examples/connectors/run_connector_policy",
    main = "examples/connectors/run_connector_policy.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "small",
    srcs = ["examples/connectors/run_connector_policy.py"],
    data = glob([
        "tests/data/checkpoints/APPO_CartPole-v0_checkpoint-6-08062022",
    ]),
    args = ["--checkpoint_file=tests/data/checkpoints/APPO_CartPole-v0_checkpoint-6-08062022"]
)

py_test(
    name = "examples/connectors/adapt_connector_policy",
    main = "examples/connectors/adapt_connector_policy.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "small",
    srcs = ["examples/connectors/adapt_connector_policy.py"],
    data = glob([
        "tests/data/checkpoints/APPO_CartPole-v0_checkpoint-6-07092022",
    ]),
    args = ["--checkpoint_file=tests/data/checkpoints/APPO_CartPole-v0_checkpoint-6-07092022"]
)

py_test(
    name = "examples/connectors/self_play_with_policy_checkpoint",
    main = "examples/connectors/self_play_with_policy_checkpoint.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "small",
    srcs = ["examples/connectors/self_play_with_policy_checkpoint.py"],
    data = glob([
        "tests/data/checkpoints/PPO_open_spiel_checkpoint-6",
    ]),
    args = [
        "--checkpoint_file=tests/data/checkpoints/PPO_open_spiel_checkpoint-6",
        "--train_iteration=1"  # Smoke test.
    ]
)

# --------------------------------------------------------------------
# examples/documentation directory
#
# Tag: documentation
#
# NOTE: Add tests alphabetically to this list.
# --------------------------------------------------------------------

py_test(
    name = "examples/documentation/custom_gym_env",
    main = "examples/documentation/custom_gym_env.py",
    tags = ["team:rllib", "documentation", "no_main"],
    size = "medium",
    srcs = ["examples/documentation/custom_gym_env.py"],
)

py_test(
    name = "examples/documentation/saving_and_loading_algos_and_policies",
    main = "examples/documentation/saving_and_loading_algos_and_policies.py",
    tags = ["team:rllib", "documentation", "no_main"],
    size = "medium",
    srcs = ["examples/documentation/saving_and_loading_algos_and_policies.py"],
)

py_test(
    name = "examples/documentation/replay_buffer_demo",
    main = "examples/documentation/replay_buffer_demo.py",
    tags = ["team:rllib", "documentation", "no_main"],
    size = "medium",
    srcs = ["examples/documentation/replay_buffer_demo.py"],
)

py_test(
    name = "examples/documentation/rllib_on_ray_readme",
    main = "examples/documentation/rllib_on_ray_readme.py",
    tags = ["team:rllib", "documentation", "no_main"],
    size = "medium",
    srcs = ["examples/documentation/rllib_on_ray_readme.py"],
)

py_test(
    name = "examples/documentation/rllib_on_rllib_readme",
    main = "examples/documentation/rllib_on_rllib_readme.py",
    tags = ["team:rllib", "documentation", "no_main"],
    size = "medium",
    srcs = ["examples/documentation/rllib_on_rllib_readme.py"],
)

# --------------------------------------------------------------------
# Manual/disabled tests
# --------------------------------------------------------------------
py_test_module_list(
  files = [
    "tests/test_dnc.py",
    "tests/test_perf.py",
    "env/wrappers/tests/test_kaggle_wrapper.py",
    "examples/env/tests/test_cliff_walking_wall_env.py",
    "examples/env/tests/test_coin_game_non_vectorized_env.py",
    "examples/env/tests/test_coin_game_vectorized_env.py",
    "examples/env/tests/test_matrix_sequential_social_dilemma.py",
    "examples/env/tests/test_wrappers.py",
    "utils/tests/test_utils.py",
  ],
  size = "large",
  extra_srcs = [],
  deps = [],
  tags = ["manual", "team:rllib", "no_main"],
)
