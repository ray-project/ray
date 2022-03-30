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
# -- "fake_gpus": Tests that run using 2 fake GPUs.

# - Quick agent compilation/tune-train tests, tagged "quick_train".
#   NOTE: These should be obsoleted in favor of "trainers_dir" tests as
#         they cover the same functionaliy.

# - Folder-bound tests, tagged with the name of the top-level dir:
#   - `env` directory tests.
#   - `evaluation` directory tests.
#   - `execution` directory tests.
#   - `models` directory tests.
#   - `policy` directory tests.
#   - `utils` directory tests.

# - Trainer ("agents") tests, tagged "trainers_dir".

# - Tests directory (everything in rllib/tests/...), tagged: "tests_dir" and
#   "tests_dir_[A-Z]"

# - Examples directory (everything in rllib/examples/...), tagged: "examples" and
#   "examples_[A-Z]"

# Note: The "examples" and "tests_dir" tags have further sub-tags going by the
# starting letter of the test name (e.g. "examples_A", or "tests_dir_F") for
# split-up purposes in buildkite.

# Note: There is a special directory in examples: "documentation" which contains
# all code that is linked to from within the RLlib docs. This code is tested
# separately via the "documentation" tag.

# Additional tags are:
# - "team:ml": Indicating that all tests in this file are the responsibility of
#   the ML Team.
# - "needs_gpu": Indicating that a test needs to have a GPU in order to run.
# - "gpu": Indicating that a test may (but doesn't have to) be run in the GPU
#   pipeline, defined in .buildkite/pipeline.gpu.yaml.
# - "multi-gpu": Indicating that a test will definitely be run in the Large GPU
#   pipeline, defined in .buildkite/pipeline.gpu.large.yaml.
# - "no_gpu": Indicating that a test should not be run in the GPU pipeline due
#   to certain incompatibilities.
# - "no_tf_eager_tracing": Exclude this test from tf-eager tracing tests.
# - "torch_only": Only run this test case with framework=torch.

# Our .buildkite/pipeline.yml and .buildkite/pipeline.gpu.yml files execute all
# these tests in n different jobs.

load("//bazel:python.bzl", "py_test_module_list")

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
    name = "learning_tests_cartpole_a2c",
    main = "tests/run_regression_tests.py",
    tags = ["team:ml", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/a3c/cartpole-a2c.yaml"],
    args = ["--yaml-dir=tuned_examples/a3c"]
)

py_test(
    name = "learning_tests_cartpole_a2c_fake_gpus",
    main = "tests/run_regression_tests.py",
    tags = ["team:ml", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete", "fake_gpus"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/a3c/cartpole-a2c-fake-gpus.yaml"],
    args = ["--yaml-dir=tuned_examples/a3c"]
)

py_test(
    name = "learning_tests_cartpole_a3c",
    main = "tests/run_regression_tests.py",
    tags = ["team:ml", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/a3c/cartpole-a3c.yaml"],
    args = ["--yaml-dir=tuned_examples/a3c"]
)

# AlphaStar
py_test(
    name = "learning_tests_cartpole_alpha_star",
    main = "tests/run_regression_tests.py",
    tags = ["team:ml", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/alpha_star/multi-agent-cartpole-alpha-star.yaml"],
    args = ["--yaml-dir=tuned_examples/alpha_star", "--num-cpus=20"]
)

# APEX-DQN
py_test(
    name = "learning_tests_cartpole_apex",
    main = "tests/run_regression_tests.py",
    tags = ["team:ml", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = [
        "tuned_examples/dqn/cartpole-apex.yaml",
    ],
    args = ["--yaml-dir=tuned_examples/dqn", "--num-cpus=6"]
)

# Once APEX supports multi-GPU.
# py_test(
#    name = "learning_cartpole_apex_fake_gpus",
#    main = "tests/run_regression_tests.py",
#    tags = ["team:ml", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete", "fake_gpus"],
#    size = "large",
#    srcs = ["tests/run_regression_tests.py"],
#    data = ["tuned_examples/dqn/cartpole-apex-fake-gpus.yaml"],
#    args = ["--yaml-dir=tuned_examples/dqn"]
# )

# APPO
py_test(
    name = "learning_tests_cartpole_appo_no_vtrace",
    main = "tests/run_regression_tests.py",
    tags = ["team:ml", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/ppo/cartpole-appo.yaml"],
    args = ["--yaml-dir=tuned_examples/ppo"]
)

py_test(
    name = "learning_tests_cartpole_appo_vtrace",
    main = "tests/run_regression_tests.py",
    tags = ["team:ml", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/ppo/cartpole-appo-vtrace.yaml"],
    args = ["--yaml-dir=tuned_examples/ppo"]
)

py_test(
    name = "learning_tests_cartpole_separate_losses_appo",
    main = "tests/run_regression_tests.py",
    tags = ["team:ml", "tf_only", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = [
        "tuned_examples/ppo/cartpole-appo-vtrace-separate-losses.yaml"
    ],
    args = ["--yaml-dir=tuned_examples/ppo"]
)

py_test(
    name = "learning_tests_frozenlake_appo",
    main = "tests/run_regression_tests.py",
    tags = ["team:ml", "learning_tests", "learning_tests_discrete"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/ppo/frozenlake-appo-vtrace.yaml"],
    args = ["--yaml-dir=tuned_examples/ppo"]
)

py_test(
    name = "learning_tests_cartpole_appo_fake_gpus",
    main = "tests/run_regression_tests.py",
    tags = ["team:ml", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete", "fake_gpus"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/ppo/cartpole-appo-vtrace-fake-gpus.yaml"],
    args = ["--yaml-dir=tuned_examples/ppo"]
)

# ARS
py_test(
    name = "learning_tests_cartpole_ars",
    main = "tests/run_regression_tests.py",
    tags = ["team:ml", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/ars/cartpole-ars.yaml"],
    args = ["--yaml-dir=tuned_examples/ars"]
)

# CQL
py_test(
    name = "learning_tests_pendulum_cql",
    main = "tests/run_regression_tests.py",
    tags = ["team:ml", "learning_tests", "learning_tests_pendulum", "learning_tests_continuous"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    # Include the zipped json data file as well.
    data = [
        "tuned_examples/cql/pendulum-cql.yaml",
        "tests/data/pendulum/enormous.zip",
    ],
    args = ["--yaml-dir=tuned_examples/cql"]
)

# DDPG
# py_test(
#    name = "learning_tests_pendulum_ddpg",
#    main = "tests/run_regression_tests.py",
#    tags = ["team:ml", "learning_tests", "learning_tests_pendulum", "learning_tests_continuous"],
#    size = "large",
#    srcs = ["tests/run_regression_tests.py"],
#    data = glob(["tuned_examples/ddpg/pendulum-ddpg.yaml"]),
#    args = ["--yaml-dir=tuned_examples/ddpg"]
# )

# py_test(
#    name = "learning_tests_pendulum_ddpg_fake_gpus",
#    main = "tests/run_regression_tests.py",
#    tags = ["team:ml", "learning_tests", "learning_tests_pendulum", "learning_tests_continuous", "fake_gpus"],
#    size = "large",
#    srcs = ["tests/run_regression_tests.py"],
#    data = ["tuned_examples/ddpg/pendulum-ddpg-fake-gpus.yaml"],
#    args = ["--yaml-dir=tuned_examples/ddpg"]
# )

# DDPPO
py_test(
    name = "learning_tests_cartpole_ddppo",
    main = "tests/run_regression_tests.py",
    tags = ["team:ml", "torch_only", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = glob(["tuned_examples/ppo/cartpole-ddppo.yaml"]),
    args = ["--yaml-dir=tuned_examples/ppo"]
)

# DQN
py_test(
    name = "learning_tests_cartpole_dqn",
    main = "tests/run_regression_tests.py",
    tags = ["team:ml", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/dqn/cartpole-dqn.yaml"],
    args = ["--yaml-dir=tuned_examples/dqn"]
)

py_test(
    name = "learning_tests_cartpole_dqn_softq",
    main = "tests/run_regression_tests.py",
    tags = ["team:ml", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/dqn/cartpole-dqn-softq.yaml"],
    args = ["--yaml-dir=tuned_examples/dqn"]
)

# Does not work with tf-eager tracing due to Exploration's postprocessing
# method injecting a tensor into a new graph. Revisit when tf-eager tracing
# is better supported.
py_test(
    name = "learning_tests_cartpole_dqn_param_noise",
    main = "tests/run_regression_tests.py",
    tags = ["team:ml", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete", "no_tf_eager_tracing"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/dqn/cartpole-dqn-param-noise.yaml"],
    args = ["--yaml-dir=tuned_examples/dqn"]
)

py_test(
    name = "learning_tests_cartpole_dqn_fake_gpus",
    main = "tests/run_regression_tests.py",
    tags = ["team:ml", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete", "fake_gpus"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/dqn/cartpole-dqn-fake-gpus.yaml"],
    args = ["--yaml-dir=tuned_examples/dqn"]
)

# Simple-Q
py_test(
    name = "learning_tests_cartpole_simpleq",
    main = "tests/run_regression_tests.py",
    tags = ["team:ml", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = [
        "tuned_examples/dqn/cartpole-simpleq.yaml",
    ],
    args = ["--yaml-dir=tuned_examples/dqn"]
)

py_test(
    name = "learning_tests_cartpole_simpleq_fake_gpus",
    main = "tests/run_regression_tests.py",
    tags = ["team:ml", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete", "fake_gpus"],
    size = "medium",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/dqn/cartpole-simpleq-fake-gpus.yaml"],
    args = ["--yaml-dir=tuned_examples/dqn"]
)

# ES
py_test(
    name = "learning_tests_cartpole_es",
    main = "tests/run_regression_tests.py",
    tags = ["team:ml", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/es/cartpole-es.yaml"],
    args = ["--yaml-dir=tuned_examples/es"]
)

# IMPALA
py_test(
    name = "learning_tests_cartpole_impala",
    main = "tests/run_regression_tests.py",
    tags = ["team:ml", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/impala/cartpole-impala.yaml"],
    args = ["--yaml-dir=tuned_examples/impala"]
)

py_test(
    name = "learning_tests_cartpole_impala_fake_gpus",
    main = "tests/run_regression_tests.py",
    tags = ["team:ml", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete", "fake_gpus"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/impala/cartpole-impala-fake-gpus.yaml"],
    args = ["--yaml-dir=tuned_examples/impala"]
)

# Working, but takes a long time to learn (>15min).
# Removed due to Higher API conflicts with Pytorch-Import tests
## MB-MPO
#py_test(
#    name = "learning_tests_pendulum_mbmpo",
#    main = "tests/run_regression_tests.py",
#    tags = ["team:ml", "torch_only", "learning_tests", "learning_tests_pendulum", "learning_tests_continuous"],
#    size = "large",
#    srcs = ["tests/run_regression_tests.py"],
#    data = ["tuned_examples/mbmpo/pendulum-mbmpo.yaml"],
#    args = ["--yaml-dir=tuned_examples/mbmpo"]
#)

# PG
py_test(
    name = "learning_tests_cartpole_pg",
    main = "tests/run_regression_tests.py",
    tags = ["team:ml", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/pg/cartpole-pg.yaml"],
    args = ["--yaml-dir=tuned_examples/pg"]
)

py_test(
    name = "learning_tests_cartpole_pg_fake_gpus",
    main = "tests/run_regression_tests.py",
    tags = ["team:ml", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete", "fake_gpus"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/pg/cartpole-pg-fake-gpus.yaml"],
    args = ["--yaml-dir=tuned_examples/pg"]
)

# PPO
py_test(
    name = "learning_tests_cartpole_ppo",
    main = "tests/run_regression_tests.py",
    tags = ["team:ml", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/ppo/cartpole-ppo.yaml"],
    args = ["--yaml-dir=tuned_examples/ppo"]
)

py_test(
    name = "learning_tests_pendulum_ppo",
    main = "tests/run_regression_tests.py",
    tags = ["team:ml", "learning_tests", "learning_tests_pendulum", "learning_tests_continuous"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/ppo/pendulum-ppo.yaml"],
    args = ["--yaml-dir=tuned_examples/ppo"]
)

py_test(
    name = "learning_tests_transformed_actions_pendulum_ppo",
    main = "tests/run_regression_tests.py",
    tags = ["team:ml", "learning_tests", "learning_tests_pendulum", "learning_tests_continuous"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/ppo/pendulum-transformed-actions-ppo.yaml"],
    args = ["--yaml-dir=tuned_examples/ppo"]
)

py_test(
    name = "learning_tests_repeat_after_me_ppo",
    main = "tests/run_regression_tests.py",
    tags = ["team:ml", "learning_tests", "learning_tests_discrete"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/ppo/repeatafterme-ppo-lstm.yaml"],
    args = ["--yaml-dir=tuned_examples/ppo"]
)

py_test(
    name = "learning_tests_cartpole_ppo_fake_gpus",
    main = "tests/run_regression_tests.py",
    tags = ["team:ml", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete", "fake_gpus"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/ppo/cartpole-ppo-fake-gpus.yaml"],
    args = ["--yaml-dir=tuned_examples/ppo"]
)

# QMIX
py_test(
    name = "learning_tests_two_step_game_qmix",
    main = "tests/run_regression_tests.py",
    tags = ["team:ml", "learning_tests", "learning_tests_discrete"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/qmix/two-step-game-qmix.yaml"],
    args = ["--yaml-dir=tuned_examples/qmix", "--framework=torch"]
)

py_test(
    name = "learning_tests_two_step_game_qmix_vdn_mixer",
    main = "tests/run_regression_tests.py",
    tags = ["team:ml", "learning_tests", "learning_tests_discrete"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/qmix/two-step-game-qmix-vdn-mixer.yaml"],
    args = ["--yaml-dir=tuned_examples/qmix", "--framework=torch"]
)

py_test(
    name = "learning_tests_two_step_game_qmix_no_mixer",
    main = "tests/run_regression_tests.py",
    tags = ["team:ml", "learning_tests", "learning_tests_discrete"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/qmix/two-step-game-qmix-no-mixer.yaml"],
    args = ["--yaml-dir=tuned_examples/qmix", "--framework=torch"]
)

# R2D2
py_test(
    name = "learning_tests_stateless_cartpole_r2d2",
    main = "tests/run_regression_tests.py",
    tags = ["team:ml", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/dqn/stateless-cartpole-r2d2.yaml"],
    args = ["--yaml-dir=tuned_examples/dqn"]
)

py_test(
    name = "learning_tests_stateless_cartpole_r2d2_fake_gpus",
    main = "tests/run_regression_tests.py",
    tags = ["team:ml", "learning_tests", "learning_tests_cartpole", "fake_gpus"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/dqn/stateless-cartpole-r2d2-fake-gpus.yaml"],
    args = ["--yaml-dir=tuned_examples/dqn"]
)

# SAC
py_test(
    name = "learning_tests_cartpole_sac",
    main = "tests/run_regression_tests.py",
    tags = ["team:ml", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/sac/cartpole-sac.yaml"],
    args = ["--yaml-dir=tuned_examples/sac"]
)

py_test(
    name = "learning_tests_cartpole_continuous_pybullet_sac",
    main = "tests/run_regression_tests.py",
    tags = ["team:ml", "learning_tests", "learning_tests_cartpole", "learning_tests_continuous"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/sac/cartpole-continuous-pybullet-sac.yaml"],
    args = ["--yaml-dir=tuned_examples/sac"]
)

# py_test(
#    name = "learning_tests_pendulum_sac",
#    main = "tests/run_regression_tests.py",
#    tags = ["team:ml", "learning_tests", "learning_tests_pendulum", "learning_tests_continuous"],
#    size = "large",
#    srcs = ["tests/run_regression_tests.py"],
#    data = ["tuned_examples/sac/pendulum-sac.yaml"],
#    args = ["--yaml-dir=tuned_examples/sac"]
# )

# py_test(
#    name = "learning_tests_transformed_actions_pendulum_sac",
#    main = "tests/run_regression_tests.py",
#    tags = ["team:ml", "learning_tests", "learning_tests_pendulum", "learning_tests_continuous"],
#    size = "large",
#    srcs = ["tests/run_regression_tests.py"],
#    data = ["tuned_examples/sac/pendulum-transformed-actions-sac.yaml"],
#    args = ["--yaml-dir=tuned_examples/sac"]
# )

# py_test(
#    name = "learning_tests_pendulum_sac_fake_gpus",
#    main = "tests/run_regression_tests.py",
#    tags = ["team:ml", "learning_tests", "learning_tests_pendulum", "learning_tests_continuous", "fake_gpus"],
#    size = "large",
#    srcs = ["tests/run_regression_tests.py"],
#    data = ["tuned_examples/sac/pendulum-sac-fake-gpus.yaml"],
#    args = ["--yaml-dir=tuned_examples/sac"]
# )

# SlateQ
py_test(
    name = "learning_tests_interest_evolution_10_candidates_recsim_env_slateq",
    main = "tests/run_regression_tests.py",
    tags = ["team:ml", "learning_tests", "learning_tests_discrete"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/slateq/interest-evolution-10-candidates-recsim-env-slateq.yaml"],
    args = ["--yaml-dir=tuned_examples/slateq"]
)

py_test(
    name = "learning_tests_interest_evolution_10_candidates_recsim_env_slateq_fake_gpus",
    main = "tests/run_regression_tests.py",
    tags = ["team:ml", "learning_tests", "learning_tests_discrete", "fake_gpus"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/slateq/interest-evolution-10-candidates-recsim-env-slateq.yaml"],
    args = ["--yaml-dir=tuned_examples/slateq"]
)

# TD3
py_test(
    name = "learning_tests_pendulum_td3",
    main = "tests/run_regression_tests.py",
    tags = ["team:ml", "learning_tests", "learning_tests_pendulum", "learning_tests_continuous"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/ddpg/pendulum-td3.yaml"],
    args = ["--yaml-dir=tuned_examples/ddpg"]
)


# --------------------------------------------------------------------
# Agents (Compilation, Losses, simple agent functionality tests)
# rllib/agents/
#
# Tag: trainers_dir
# --------------------------------------------------------------------

# Generic (all Trainers)
py_test(
    name = "test_callbacks",
    tags = ["team:ml", "trainers_dir"],
    size = "medium",
    srcs = ["agents/tests/test_callbacks.py"]
)

py_test(
    name = "test_trainer",
    tags = ["team:ml", "trainers_dir"],
    size = "large",
    srcs = ["agents/tests/test_trainer.py"]
)

# A2/3CTrainer
py_test(
    name = "test_a2c",
    tags = ["team:ml", "trainers_dir"],
    size = "large",
    srcs = ["agents/a3c/tests/test_a2c.py"]
)

py_test(
    name = "test_a3c",
    tags = ["team:ml", "trainers_dir"],
    size = "large",
    srcs = ["agents/a3c/tests/test_a3c.py"]
)

# AlphaStar
py_test(
    name = "test_alpha_star",
    tags = ["team:ml", "trainers_dir"],
    size = "large",
    srcs = ["agents/alpha_star/tests/test_alpha_star.py"]
)

# APEXTrainer (DQN)
py_test(
    name = "test_apex_dqn",
    tags = ["team:ml", "trainers_dir"],
    size = "large",
    srcs = ["agents/dqn/tests/test_apex_dqn.py"]
)

# APEXDDPGTrainer
py_test(
    name = "test_apex_ddpg",
    tags = ["team:ml", "trainers_dir"],
    size = "medium",
    srcs = ["agents/ddpg/tests/test_apex_ddpg.py"]
)

# ARS
py_test(
    name = "test_ars",
    tags = ["team:ml", "trainers_dir"],
    size = "medium",
    srcs = ["agents/ars/tests/test_ars.py"]
)

# Bandits
py_test(
    name = "test_bandits",
    tags = ["team:ml", "trainers_dir"],
    size = "medium",
    srcs = ["agents/bandit/tests/test_bandits.py"],
)

# CQLTrainer
py_test(
    name = "test_cql",
    tags = ["team:ml", "trainers_dir"],
    size = "medium",
    srcs = ["agents/cql/tests/test_cql.py"]
)

# DDPGTrainer
py_test(
    name = "test_ddpg",
    tags = ["team:ml", "trainers_dir"],
    size = "large",
    srcs = ["agents/ddpg/tests/test_ddpg.py"]
)

# DQNTrainer
py_test(
    name = "test_dqn",
    tags = ["team:ml", "trainers_dir"],
    size = "large",
    srcs = ["agents/dqn/tests/test_dqn.py"]
)

# Dreamer
py_test(
    name = "test_dreamer",
    tags = ["team:ml", "trainers_dir"],
    size = "small",
    srcs = ["agents/dreamer/tests/test_dreamer.py"]
)

# ES
py_test(
    name = "test_es",
    tags = ["team:ml", "trainers_dir"],
    size = "medium",
    srcs = ["agents/es/tests/test_es.py"]
)

# IMPALA
py_test(
    name = "test_impala",
    tags = ["team:ml", "trainers_dir"],
    size = "large",
    srcs = ["agents/impala/tests/test_impala.py"]
)
py_test(
    name = "test_vtrace",
    tags = ["team:ml", "trainers_dir"],
    size = "small",
    srcs = ["agents/impala/tests/test_vtrace.py"]
)

# MARWILTrainer
py_test(
    name = "test_marwil",
    tags = ["team:ml", "trainers_dir"],
    size = "large",
    # Include the json data file.
    data = ["tests/data/cartpole/large.json"],
    srcs = ["agents/marwil/tests/test_marwil.py"]
)

# BCTrainer (sub-type of MARWIL)
py_test(
    name = "test_bc",
    tags = ["team:ml", "trainers_dir"],
    size = "large",
    # Include the json data file.
    data = ["tests/data/cartpole/large.json"],
    srcs = ["agents/marwil/tests/test_bc.py"]
)

# MAMLTrainer
py_test(
    name = "test_maml",
    tags = ["team:ml", "trainers_dir"],
    size = "medium",
    srcs = ["agents/maml/tests/test_maml.py"]
)

# MBMPOTrainer
py_test(
    name = "test_mbmpo",
    tags = ["team:ml", "trainers_dir"],
    size = "medium",
    srcs = ["agents/mbmpo/tests/test_mbmpo.py"]
)

# PGTrainer
py_test(
    name = "test_pg",
    tags = ["team:ml", "trainers_dir"],
    size = "large",
    srcs = ["agents/pg/tests/test_pg.py"]
)

# PPOTrainer
py_test(
    name = "test_ppo",
    tags = ["team:ml", "trainers_dir"],
    size = "large",
    srcs = ["agents/ppo/tests/test_ppo.py"]
)

# PPO: DDPPO
py_test(
    name = "test_ddppo",
    tags = ["team:ml", "trainers_dir"],
    size = "medium",
    srcs = ["agents/ppo/tests/test_ddppo.py"]
)

# PPO: APPO
py_test(
    name = "test_appo",
    tags = ["team:ml", "trainers_dir"],
    size = "large",
    srcs = ["agents/ppo/tests/test_appo.py"]
)

# QMixTrainer
py_test(
    name = "test_qmix",
    tags = ["team:ml", "trainers_dir"],
    size = "medium",
    srcs = ["agents/qmix/tests/test_qmix.py"]
)

# R2D2Trainer
py_test(
    name = "test_r2d2",
    tags = ["team:ml", "trainers_dir"],
    size = "large",
    srcs = ["agents/dqn/tests/test_r2d2.py"]
)

# RNNSACTrainer
py_test(
    name = "test_rnnsac",
    tags = ["team:ml", "trainers_dir"],
    size = "medium",
    srcs = ["agents/sac/tests/test_rnnsac.py"]
)

# SACTrainer
py_test(
    name = "test_sac",
    tags = ["team:ml", "trainers_dir"],
    size = "large",
    srcs = ["agents/sac/tests/test_sac.py"]
)

# SimpleQTrainer
py_test(
    name = "test_simple_q",
    tags = ["team:ml", "trainers_dir"],
    size = "medium",
    srcs = ["agents/dqn/tests/test_simple_q.py"]
)

# SlateQTrainer
py_test(
    name = "test_slateq",
    tags = ["team:ml", "trainers_dir"],
    size = "medium",
    srcs = ["agents/slateq/tests/test_slateq.py"]
)

# TD3Trainer
py_test(
    name = "test_td3",
    tags = ["team:ml", "trainers_dir"],
    size = "large",
    srcs = ["agents/ddpg/tests/test_td3.py"]
)

# --------------------------------------------------------------------
# contrib Agents
# --------------------------------------------------------------------

py_test(
    name = "random_agent",
    tags = ["team:ml", "trainers_dir"],
    main = "contrib/random_agent/random_agent.py",
    size = "small",
    srcs = ["contrib/random_agent/random_agent.py"]
)

py_test(
    name = "alpha_zero_cartpole",
    tags = ["team:ml", "trainers_dir"],
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
    name = "test_a3c_torch_pong_deterministic_v4",
    main = "train.py", srcs = ["train.py"],
    tags = ["team:ml", "quick_train"],
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
    tags = ["team:ml", "quick_train"],
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
    tags = ["team:ml", "quick_train"],
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
    tags = ["team:ml", "quick_train"],
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
    tags = ["team:ml", "quick_train"],
    args = [
        "--env", "Pendulum-v1",
        "--run", "APEX_DDPG",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"framework\": \"tf\", \"num_workers\": 2, \"optimizer\": {\"num_replay_buffer_shards\": 1}, \"learning_starts\": 100, \"min_time_s_per_reporting\": 1, \"batch_mode\": \"complete_episodes\"}'",
        "--ray-num-cpus", "4",
        ]
)

# DQN/APEX

py_test(
    name = "test_dqn_frozenlake_v1",
    main = "train.py", srcs = ["train.py"],
    size = "small",
    tags = ["team:ml", "quick_train"],
    args = [
        "--env", "FrozenLake-v1",
        "--run", "DQN",
        "--config", "'{\"framework\": \"tf\"}'",
        "--stop", "'{\"training_iteration\": 1}'"
        ]
)

py_test(
    name = "test_dqn_cartpole_v0_no_dueling",
    main = "train.py", srcs = ["train.py"],
    size = "small",
    tags = ["team:ml", "quick_train"],
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
    tags = ["team:ml", "quick_train"],
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
    tags = ["team:ml", "quick_train", "external_files"],
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
    tags = ["team:ml", "quick_train"],
    args = [
        "--env", "PongDeterministic-v4",
        "--run", "DQN",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"framework\": \"tf\", \"lr\": 1e-4, \"exploration_config\": {\"epsilon_timesteps\": 200000, \"final_epsilon\": 0.01}, \"buffer_size\": 10000, \"rollout_fragment_length\": 4, \"learning_starts\": 10000, \"target_network_update_freq\": 1000, \"gamma\": 0.99, \"prioritized_replay\": true}'"
        ]
)

# IMPALA

py_test(
    name = "test_impala_buffers_2",
    main = "train.py", srcs = ["train.py"],
    tags = ["team:ml", "quick_train"],
    args = [
        "--env", "CartPole-v0",
        "--run", "IMPALA",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"framework\": \"tf\", \"num_gpus\": 0, \"num_workers\": 2, \"min_time_s_per_reporting\": 1, \"num_multi_gpu_tower_stacks\": 2, \"replay_buffer_num_slots\": 100, \"replay_proportion\": 1.0}'",
        "--ray-num-cpus", "4",
        ]
)

py_test(
    name = "test_impala_cartpole_v0_buffers_2_lstm",
    main = "train.py",
    srcs = ["train.py"],
    tags = ["team:ml", "quick_train"],
    args = [
        "--env", "CartPole-v0",
        "--run", "IMPALA",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"framework\": \"tf\", \"num_gpus\": 0, \"num_workers\": 2, \"min_time_s_per_reporting\": 1, \"num_multi_gpu_tower_stacks\": 2, \"replay_buffer_num_slots\": 100, \"replay_proportion\": 1.0, \"model\": {\"use_lstm\": true}}'",
        "--ray-num-cpus", "4",
        ]
)

py_test(
    name = "test_impala_pong_deterministic_v4_40k_ts_1G_obj_store",
    main = "train.py",
    srcs = ["train.py"],
    tags = ["team:ml", "quick_train"],
    size = "medium",
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
    name = "test_pg_tf_cartpole_v0_lstm",
    main = "train.py", srcs = ["train.py"],
    tags = ["team:ml", "quick_train"],
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
    tags = ["team:ml", "quick_train"],
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
    tags = ["team:ml", "quick_train"],
    args = [
        "--env", "Pong-v0",
        "--run", "PG",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"framework\": \"tf\", \"rollout_fragment_length\": 500, \"num_workers\": 1}'"
        ]
)

# PPO/APPO

py_test(
    name = "test_ppo_tf_cartpole_v1_complete_episode_batches",
    main = "train.py", srcs = ["train.py"],
    tags = ["team:ml", "quick_train"],
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
    tags = ["team:ml", "quick_train"],
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
    tags = ["team:ml", "quick_train"],
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
    tags = ["team:ml", "quick_train"],
    args = [
        "--env", "Pendulum-v1",
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
    name = "env/tests/test_local_inference_cartpole",
    tags = ["team:ml", "env"],
    size = "medium",
    srcs = ["env/tests/test_policy_client_server_setup.sh"],
    args = ["local", "cartpole"],
    data = glob(["examples/serving/*.py"]),
)

sh_test(
    name = "env/tests/test_remote_inference_cartpole",
    tags = ["team:ml", "env"],
    size = "medium",
    srcs = ["env/tests/test_policy_client_server_setup.sh"],
    args = ["remote", "cartpole"],
    data = glob(["examples/serving/*.py"]),
)

sh_test(
    name = "env/tests/test_local_inference_cartpole_w_2_concurrent_episodes",
    tags = ["team:ml", "env"],
    size = "medium",
    srcs = ["env/tests/test_policy_client_server_setup.sh"],
    args = ["local", "cartpole-dummy-2-episodes"],
    data = glob(["examples/serving/*.py"]),
)

sh_test(
    name = "env/tests/test_remote_inference_cartpole_w_2_concurrent_episodes",
    tags = ["team:ml", "env"],
    size = "medium",
    srcs = ["env/tests/test_policy_client_server_setup.sh"],
    args = ["remote", "cartpole-dummy-2-episodes"],
    data = glob(["examples/serving/*.py"]),
)

sh_test(
    name = "env/tests/test_local_inference_unity3d",
    tags = ["team:ml", "env"],
    size = "medium",
    srcs = ["env/tests/test_policy_client_server_setup.sh"],
    args = ["local", "unity3d"],
    data = glob(["examples/serving/*.py"]),
)

sh_test(
    name = "env/tests/test_remote_inference_unity3d",
    tags = ["team:ml", "env"],
    size = "medium",
    srcs = ["env/tests/test_policy_client_server_setup.sh"],
    args = ["remote", "unity3d"],
    data = glob(["examples/serving/*.py"]),
)

py_test(
    name = "env/tests/test_record_env_wrapper",
    tags = ["team:ml", "env"],
    size = "small",
    srcs = ["env/tests/test_record_env_wrapper.py"]
)

py_test(
    name = "env/tests/test_remote_worker_envs",
    tags = ["team:ml", "env"],
    size = "medium",
    srcs = ["env/tests/test_remote_worker_envs.py"]
)

py_test(
    name = "env/wrappers/tests/test_unity3d_env",
    tags = ["team:ml", "env"],
    size = "small",
    srcs = ["env/wrappers/tests/test_unity3d_env.py"]
)

py_test(
    name = "env/wrappers/tests/test_recsim_wrapper",
    tags = ["team:ml", "env"],
    size = "small",
    srcs = ["env/wrappers/tests/test_recsim_wrapper.py"]
)

py_test(
    name = "env/wrappers/tests/test_exception_wrapper",
    tags = ["team:ml", "env"],
    size = "small",
    srcs = ["env/wrappers/tests/test_exception_wrapper.py"]
)

py_test(
    name = "env/wrappers/tests/test_group_agents_wrapper",
    tags = ["team:ml", "env"],
    size = "small",
    srcs = ["env/wrappers/tests/test_group_agents_wrapper.py"]
)

# --------------------------------------------------------------------
# Evaluation components
# rllib/evaluation/
#
# Tag: evaluation
# --------------------------------------------------------------------

py_test(
    name = "evaluation/tests/test_postprocessing",
    tags = ["team:ml", "evaluation"],
    size = "small",
    srcs = ["evaluation/tests/test_postprocessing.py"]
)

py_test(
    name = "evaluation/tests/test_rollout_worker",
    tags = ["team:ml", "evaluation"],
    size = "medium",
    srcs = ["evaluation/tests/test_rollout_worker.py"]
)

py_test(
    name = "evaluation/tests/test_trajectory_view_api",
    tags = ["team:ml", "evaluation"],
    size = "medium",
    srcs = ["evaluation/tests/test_trajectory_view_api.py"]
)

py_test(
    name = "evaluation/tests/test_episode",
    tags = ["team:ml", "evaluation"],
    size = "small",
    srcs = ["evaluation/tests/test_episode.py"]
)

# --------------------------------------------------------------------
# Optimizers and Memories
# rllib/execution/
#
# Tag: execution
# --------------------------------------------------------------------

py_test(
    name = "test_segment_tree",
    tags = ["team:ml", "execution"],
    size = "small",
    srcs = ["execution/tests/test_segment_tree.py"]
)

py_test(
    name = "test_prioritized_replay_buffer",
    tags = ["team:ml", "execution"],
    size = "small",
    srcs = ["execution/tests/test_prioritized_replay_buffer.py"]
)

# --------------------------------------------------------------------
# Models and Distributions
# rllib/models/
#
# Tag: models
# --------------------------------------------------------------------

py_test(
    name = "test_attention_nets",
    tags = ["team:ml", "models"],
    size = "large",
    srcs = ["models/tests/test_attention_nets.py"]
)

py_test(
    name = "test_conv2d_default_stacks",
    tags = ["team:ml", "models"],
    size = "medium",
    srcs = ["models/tests/test_conv2d_default_stacks.py"]
)

py_test(
    name = "test_convtranspose2d_stack",
    tags = ["team:ml", "models"],
    size = "small",
    data = glob(["tests/data/images/obstacle_tower.png"]),
    srcs = ["models/tests/test_convtranspose2d_stack.py"]
)

py_test(
    name = "test_distributions",
    tags = ["team:ml", "models"],
    size = "medium",
    srcs = ["models/tests/test_distributions.py"]
)

py_test(
    name = "test_lstms",
    tags = ["team:ml", "models"],
    size = "large",
    srcs = ["models/tests/test_lstms.py"]
)

py_test(
    name = "test_models",
    tags = ["team:ml", "models"],
    size = "medium",
    srcs = ["models/tests/test_models.py"]
)

py_test(
    name = "test_preprocessors",
    tags = ["team:ml", "models"],
    size = "large",
    srcs = ["models/tests/test_preprocessors.py"]
)

# --------------------------------------------------------------------
# Policies
# rllib/policy/
#
# Tag: policy
# --------------------------------------------------------------------

py_test(
    name = "policy/tests/test_compute_log_likelihoods",
    tags = ["team:ml", "policy"],
    size = "medium",
    srcs = ["policy/tests/test_compute_log_likelihoods.py"]
)

py_test(
    name = "policy/tests/test_multi_agent_batch",
    tags = ["team:ml", "policy"],
    size = "small",
    srcs = ["policy/tests/test_multi_agent_batch.py"]
)

py_test(
    name = "policy/tests/test_policy",
    tags = ["team:ml", "policy"],
    size = "medium",
    srcs = ["policy/tests/test_policy.py"]
)

py_test(
    name = "policy/tests/test_rnn_sequencing",
    tags = ["team:ml", "policy"],
    size = "small",
    srcs = ["policy/tests/test_rnn_sequencing.py"]
)

py_test(
    name = "policy/tests/test_sample_batch",
    tags = ["team:ml", "policy"],
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
    tags = ["team:ml", "utils"],
    size = "large",
    srcs = ["utils/exploration/tests/test_curiosity.py"]
)

py_test(
    name = "test_explorations",
    tags = ["team:ml", "utils"],
    size = "large",
    srcs = ["utils/exploration/tests/test_explorations.py"]
)

py_test(
    name = "test_parameter_noise",
    tags = ["team:ml", "utils"],
    size = "medium",
    srcs = ["utils/exploration/tests/test_parameter_noise.py"]
)

py_test(
    name = "test_random_encoder",
    tags = ["team:ml", "utils"],
    size = "large",
    srcs = ["utils/exploration/tests/test_random_encoder.py"]
)

# Schedules
py_test(
    name = "test_schedules",
    tags = ["team:ml", "utils"],
    size = "small",
    srcs = ["utils/schedules/tests/test_schedules.py"]
)

py_test(
    name = "test_framework_agnostic_components",
    tags = ["team:ml", "utils"],
    size = "small",
    data = glob(["utils/tests/**"]),
    srcs = ["utils/tests/test_framework_agnostic_components.py"]
)

# Spaces/Space utils.
py_test(
    name = "test_space_utils",
    tags = ["team:ml", "utils"],
    size = "large",
    srcs = ["utils/spaces/tests/test_space_utils.py"]
)

# TaskPool
py_test(
    name = "test_taskpool",
    tags = ["team:ml", "utils"],
    size = "small",
    srcs = ["utils/tests/test_taskpool.py"]
)

# ReplayBuffers
py_test(
    name = "test_multi_agent_mixin_replay_buffer",
    tags = ["team:ml", "utils"],
    size = "small",
    srcs = ["utils/replay_buffers/tests/test_multi_agent_mixin_replay_buffer.py"]
)

py_test(
    name = "test_multi_agent_prioritized_replay_buffer",
    tags = ["team:ml", "utils"],
    size = "small",
    srcs = ["utils/replay_buffers/tests/test_multi_agent_prioritized_replay_buffer.py"]
)

py_test(
    name = "test_multi_agent_replay_buffer",
    tags = ["team:ml", "utils"],
    size = "small",
    srcs = ["utils/replay_buffers/tests/test_multi_agent_replay_buffer.py"]
)

py_test(
    name = "test_prioritized_replay_buffer_replay_buffer_api",
    tags = ["team:ml", "utils"],
    size = "small",
    srcs = ["utils/replay_buffers/tests/test_prioritized_replay_buffer_replay_buffer_api.py"]
)

py_test(
    name = "test_replay_buffer",
    tags = ["team:ml", "utils"],
    size = "small",
    srcs = ["utils/replay_buffers/tests/test_replay_buffer.py"]
)

py_test(
    name = "test_reservoir_buffer",
    tags = ["team:ml", "utils"],
    size = "small",
    srcs = ["utils/replay_buffers/tests/test_reservoir_buffer.py"]
)

py_test(
    name = "test_segment_tree_replay_buffer_api",
    tags = ["team:ml", "utils"],
    size = "small",
    srcs = ["utils/replay_buffers/tests/test_segment_tree_replay_buffer_api.py"]
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
    name = "tests/test_catalog",
    tags = ["team:ml", "tests_dir", "tests_dir_C"],
    size = "medium",
    srcs = ["tests/test_catalog.py"]
)

py_test(
    name = "tests/test_checkpoint_restore_pg",
    main = "tests/test_checkpoint_restore.py",
    tags = ["team:ml", "tests_dir", "tests_dir_C"],
    size = "large",
    srcs = ["tests/test_checkpoint_restore.py"],
    args = ["TestCheckpointRestorePG"]
)

py_test(
    name = "tests/test_checkpoint_restore_off_policy",
    main = "tests/test_checkpoint_restore.py",
    tags = ["team:ml", "tests_dir", "tests_dir_C"],
    size = "large",
    srcs = ["tests/test_checkpoint_restore.py"],
    args = ["TestCheckpointRestoreOffPolicy"]
)

py_test(
    name = "tests/test_checkpoint_restore_evolution_algos",
    main = "tests/test_checkpoint_restore.py",
    tags = ["team:ml", "tests_dir", "tests_dir_C"],
    size = "large",
    srcs = ["tests/test_checkpoint_restore.py"],
    args = ["TestCheckpointRestoreEvolutionAlgos"]
)

py_test(
    name = "tests/test_dependency_tf",
    tags = ["team:ml", "tests_dir", "tests_dir_D"],
    size = "small",
    srcs = ["tests/test_dependency_tf.py"]
)

py_test(
    name = "tests/test_dependency_torch",
    tags = ["team:ml", "tests_dir", "tests_dir_D"],
    size = "small",
    srcs = ["tests/test_dependency_torch.py"]
)

py_test(
    name = "tests/test_eager_support_pg",
    main = "tests/test_eager_support.py",
    tags = ["team:ml", "tests_dir", "tests_dir_E"],
    size = "large",
    srcs = ["tests/test_eager_support.py"],
    args = ["TestEagerSupportPG"]
)

py_test(
    name = "tests/test_eager_support_off_policy",
    main = "tests/test_eager_support.py",
    tags = ["team:ml", "tests_dir", "tests_dir_E"],
    size = "large",
    srcs = ["tests/test_eager_support.py"],
    args = ["TestEagerSupportOffPolicy"]
)

py_test(
    name = "test_env_with_subprocess",
    main = "tests/test_env_with_subprocess.py",
    tags = ["team:ml", "tests_dir", "tests_dir_E"],
    size = "medium",
    srcs = ["tests/test_env_with_subprocess.py"]
)

py_test(
    name = "tests/test_exec_api",
    tags = ["team:ml", "tests_dir", "tests_dir_E"],
    size = "medium",
    srcs = ["tests/test_exec_api.py"]
)

py_test(
    name = "tests/test_execution",
    tags = ["team:ml", "tests_dir", "tests_dir_E"],
    size = "medium",
    srcs = ["tests/test_execution.py"]
)

py_test(
    name = "tests/test_export",
    tags = ["team:ml", "tests_dir", "tests_dir_E"],
    size = "medium",
    srcs = ["tests/test_export.py"]
)

py_test(
    name = "tests/test_external_env",
    tags = ["team:ml", "tests_dir", "tests_dir_E"],
    size = "large",
    srcs = ["tests/test_external_env.py"]
)

py_test(
    name = "tests/test_external_multi_agent_env",
    tags = ["team:ml", "tests_dir", "tests_dir_E"],
    size = "medium",
    srcs = ["tests/test_external_multi_agent_env.py"]
)

py_test(
    name = "tests/test_filters",
    tags = ["team:ml", "tests_dir", "tests_dir_F"],
    size = "small",
    srcs = ["tests/test_filters.py"]
)

py_test(
    name = "tests/test_gpus",
    tags = ["team:ml", "tests_dir", "tests_dir_G"],
    size = "large",
    srcs = ["tests/test_gpus.py"]
)

py_test(
    name = "tests/test_ignore_worker_failure",
    tags = ["team:ml", "tests_dir", "tests_dir_I"],
    size = "large",
    srcs = ["tests/test_ignore_worker_failure.py"]
)

py_test(
    name = "tests/test_io",
    tags = ["team:ml", "tests_dir", "tests_dir_I"],
    size = "large",
    srcs = ["tests/test_io.py"]
)

py_test(
    name = "tests/test_local",
    tags = ["team:ml", "tests_dir", "tests_dir_L"],
    size = "medium",
    srcs = ["tests/test_local.py"]
)

py_test(
    name = "tests/test_lstm",
    tags = ["team:ml", "tests_dir", "tests_dir_L"],
    size = "medium",
    srcs = ["tests/test_lstm.py"]
)

py_test(
    name = "tests/test_model_imports",
    tags = ["team:ml", "tests_dir", "tests_dir_M", "model_imports"],
    size = "medium",
    data = glob(["tests/data/model_weights/**"]),
    srcs = ["tests/test_model_imports.py"]
)

py_test(
    name = "tests/test_multi_agent_env",
    tags = ["team:ml", "tests_dir", "tests_dir_M"],
    size = "medium",
    srcs = ["tests/test_multi_agent_env.py"]
)

py_test(
    name = "tests/test_multi_agent_pendulum",
    tags = ["team:ml", "tests_dir", "tests_dir_M"],
    size = "large",
    srcs = ["tests/test_multi_agent_pendulum.py"]
)

py_test(
    name = "tests/test_nested_action_spaces",
    main = "tests/test_nested_action_spaces.py",
    tags = ["team:ml", "tests_dir", "tests_dir_N"],
    size = "medium",
    srcs = ["tests/test_nested_action_spaces.py"]
)

py_test(
    name = "tests/test_nested_observation_spaces",
    main = "tests/test_nested_observation_spaces.py",
    tags = ["team:ml", "tests_dir", "tests_dir_N"],
    size = "medium",
    srcs = ["tests/test_nested_observation_spaces.py"]
)

py_test(
    name = "tests/test_nn_framework_import_errors",
    tags = ["team:ml", "tests_dir", "tests_dir_N"],
    size = "small",
    srcs = ["tests/test_nn_framework_import_errors.py"]
)

py_test(
    name = "tests/test_pettingzoo_env",
    tags = ["team:ml", "tests_dir", "tests_dir_P"],
    size = "medium",
    srcs = ["tests/test_pettingzoo_env.py"]
)

py_test(
    name = "tests/test_placement_groups",
    tags = ["team:ml", "tests_dir", "tests_dir_P"],
    size = "medium",
    srcs = ["tests/test_placement_groups.py"]
)

py_test(
    name = "tests/test_ray_client",
    tags = ["team:ml", "tests_dir", "tests_dir_R"],
    size = "large",
    srcs = ["tests/test_ray_client.py"]
)

py_test(
    name = "tests/test_reproducibility",
    tags = ["team:ml", "tests_dir", "tests_dir_R"],
    size = "medium",
    srcs = ["tests/test_reproducibility.py"]
)

# Test [train|evaluate].py scripts (w/o confirming evaluation performance).
py_test(
    name = "test_rllib_evaluate_1",
    main = "tests/test_rllib_train_and_evaluate.py",
    tags = ["team:ml", "tests_dir", "tests_dir_R"],
    size = "large",
    data = ["train.py", "evaluate.py"],
    srcs = ["tests/test_rllib_train_and_evaluate.py"],
    args = ["TestEvaluate1"]
)

py_test(
    name = "test_rllib_evaluate_2",
    main = "tests/test_rllib_train_and_evaluate.py",
    tags = ["team:ml", "tests_dir", "tests_dir_R"],
    size = "large",
    data = ["train.py", "evaluate.py"],
    srcs = ["tests/test_rllib_train_and_evaluate.py"],
    args = ["TestEvaluate2"]
)

py_test(
    name = "test_rllib_evaluate_3",
    main = "tests/test_rllib_train_and_evaluate.py",
    tags = ["team:ml", "tests_dir", "tests_dir_R"],
    size = "large",
    data = ["train.py", "evaluate.py"],
    srcs = ["tests/test_rllib_train_and_evaluate.py"],
    args = ["TestEvaluate3"]
)

py_test(
    name = "test_rllib_evaluate_4",
    main = "tests/test_rllib_train_and_evaluate.py",
    tags = ["team:ml", "tests_dir", "tests_dir_R"],
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
    tags = ["team:ml", "tests_dir", "tests_dir_R"],
    size = "large",
    data = ["train.py", "evaluate.py"],
    srcs = ["tests/test_rllib_train_and_evaluate.py"],
    args = ["TestTrainAndEvaluate"]
)

py_test(
    name = "tests/test_supported_multi_agent_pg",
    main = "tests/test_supported_multi_agent.py",
    tags = ["team:ml", "tests_dir", "tests_dir_S"],
    size = "medium",
    srcs = ["tests/test_supported_multi_agent.py"],
    args = ["TestSupportedMultiAgentPG"]
)

py_test(
    name = "tests/test_supported_multi_agent_off_policy",
    main = "tests/test_supported_multi_agent.py",
    tags = ["team:ml", "tests_dir", "tests_dir_S"],
    size = "medium",
    srcs = ["tests/test_supported_multi_agent.py"],
    args = ["TestSupportedMultiAgentOffPolicy"]
)

py_test(
     name = "tests/test_supported_spaces_pg",
     main = "tests/test_supported_spaces.py",
     tags = ["team:ml", "tests_dir", "tests_dir_S"],
     size = "large",
     srcs = ["tests/test_supported_spaces.py"],
     args = ["TestSupportedSpacesPG"]
 )

py_test(
    name = "tests/test_supported_spaces_off_policy",
    main = "tests/test_supported_spaces.py",
    tags = ["team:ml", "tests_dir", "tests_dir_S"],
    size = "medium",
    srcs = ["tests/test_supported_spaces.py"],
    args = ["TestSupportedSpacesOffPolicy"]
)

py_test(
    name = "tests/test_supported_spaces_evolution_algos",
    main = "tests/test_supported_spaces.py",
    tags = ["team:ml", "tests_dir", "tests_dir_S"],
    size = "large",
    srcs = ["tests/test_supported_spaces.py"],
    args = ["TestSupportedSpacesEvolutionAlgos"]
)

py_test(
    name = "tests/test_timesteps",
    tags = ["team:ml", "tests_dir", "tests_dir_T"],
    size = "small",
    srcs = ["tests/test_timesteps.py"]
)

# --------------------------------------------------------------------
# examples/ directory (excluding examples/documentation/...)
#
# Tag: examples, examples_[A-Z]
#
# NOTE: Add tests alphabetically into this list and make sure, to tag
# it correctly by its starting letter, e.g. tags=["examples", "examples_A"]
# for `examples/all_stuff.py`.
# --------------------------------------------------------------------

py_test(
    name = "examples/action_masking_tf",
    main = "examples/action_masking.py",
    tags = ["team:ml", "examples", "examples_A"],
    size = "medium",
    srcs = ["examples/action_masking.py"],
    args = ["--stop-iter=2"]
)

py_test(
    name = "examples/action_masking_torch",
    main = "examples/action_masking.py",
    tags = ["team:ml", "examples", "examples_A"],
    size = "medium",
    srcs = ["examples/action_masking.py"],
    args = ["--stop-iter=2", "--framework=torch"]
)

py_test(
    name = "examples/attention_net_tf",
    main = "examples/attention_net.py",
    tags = ["team:ml", "examples", "examples_A"],
    size = "medium",
    srcs = ["examples/attention_net.py"],
    args = ["--as-test", "--stop-reward=70"]
)

py_test(
    name = "examples/attention_net_torch",
    main = "examples/attention_net.py",
    tags = ["team:ml", "examples", "examples_A"],
    size = "medium",
    srcs = ["examples/attention_net.py"],
    args = ["--as-test", "--stop-reward=70", "--framework torch"]
)

py_test(
    name = "examples/autoregressive_action_dist_tf",
    main = "examples/autoregressive_action_dist.py",
    tags = ["team:ml", "examples", "examples_A"],
    size = "medium",
    srcs = ["examples/autoregressive_action_dist.py"],
    args = ["--as-test", "--stop-reward=150", "--num-cpus=4"]
)

py_test(
    name = "examples/autoregressive_action_dist_torch",
    main = "examples/autoregressive_action_dist.py",
    tags = ["team:ml", "examples", "examples_A"],
    size = "medium",
    srcs = ["examples/autoregressive_action_dist.py"],
    args = ["--as-test", "--framework=torch", "--stop-reward=150", "--num-cpus=4"]
)

py_test(
    name = "examples/bare_metal_policy_with_custom_view_reqs",
    main = "examples/bare_metal_policy_with_custom_view_reqs.py",
    tags = ["team:ml", "examples", "examples_B"],
    size = "medium",
    srcs = ["examples/bare_metal_policy_with_custom_view_reqs.py"],
)

py_test(
    name = "examples/batch_norm_model_ppo_tf",
    main = "examples/batch_norm_model.py",
    tags = ["team:ml", "examples", "examples_B"],
    size = "medium",
    srcs = ["examples/batch_norm_model.py"],
    args = ["--as-test", "--run=PPO", "--stop-reward=80"]
)

py_test(
    name = "examples/batch_norm_model_ppo_torch",
    main = "examples/batch_norm_model.py",
    tags = ["team:ml", "examples", "examples_B"],
    size = "medium",
    srcs = ["examples/batch_norm_model.py"],
    args = ["--as-test", "--framework=torch", "--run=PPO", "--stop-reward=80"]
)

py_test(
    name = "examples/batch_norm_model_dqn_tf",
    main = "examples/batch_norm_model.py",
    tags = ["team:ml", "examples", "examples_B"],
    size = "medium",
    srcs = ["examples/batch_norm_model.py"],
    args = ["--as-test", "--run=DQN", "--stop-reward=70"]
)

py_test(
    name = "examples/batch_norm_model_dqn_torch",
    main = "examples/batch_norm_model.py",
    tags = ["team:ml", "examples", "examples_B"],
    size = "large",  # DQN learns much slower with BatchNorm.
    srcs = ["examples/batch_norm_model.py"],
    args = ["--as-test", "--framework=torch", "--run=DQN", "--stop-reward=70"]
)

py_test(
    name = "examples/batch_norm_model_ddpg_tf",
    main = "examples/batch_norm_model.py",
    tags = ["team:ml", "examples", "examples_B"],
    size = "medium",
    srcs = ["examples/batch_norm_model.py"],
    args = ["--run=DDPG", "--stop-iters=1"]
)

py_test(
    name = "examples/batch_norm_model_ddpg_torch",
    main = "examples/batch_norm_model.py",
    tags = ["team:ml", "examples", "examples_B"],
    size = "medium",
    srcs = ["examples/batch_norm_model.py"],
    args = ["--framework=torch", "--run=DDPG", "--stop-iters=1"]
)

py_test(
    name = "examples/cartpole_lstm_impala_tf",
    main = "examples/cartpole_lstm.py",
    tags = ["team:ml", "examples", "examples_C", "examples_C_AtoT"],
    size = "medium",
    srcs = ["examples/cartpole_lstm.py"],
    args = ["--as-test", "--run=IMPALA", "--stop-reward=40", "--num-cpus=4"]
)

py_test(
    name = "examples/cartpole_lstm_impala_torch",
    main = "examples/cartpole_lstm.py",
    tags = ["team:ml", "examples", "examples_C", "examples_C_AtoT"],
    size = "medium",
    srcs = ["examples/cartpole_lstm.py"],
    args = ["--as-test", "--framework=torch", "--run=IMPALA", "--stop-reward=40", "--num-cpus=4"]
)

py_test(
    name = "examples/cartpole_lstm_ppo_tf",
    main = "examples/cartpole_lstm.py",
    tags = ["team:ml", "examples", "examples_C", "examples_C_AtoT"],
    size = "medium",
    srcs = ["examples/cartpole_lstm.py"],
    args = ["--as-test", "--framework=tf", "--run=PPO", "--stop-reward=40", "--num-cpus=4"]
)

py_test(
    name = "examples/cartpole_lstm_ppo_tf2",
    main = "examples/cartpole_lstm.py",
    tags = ["team:ml", "examples", "examples_C", "examples_C_AtoT"],
    size = "large",
    srcs = ["examples/cartpole_lstm.py"],
    args = ["--as-test", "--framework=tf2", "--run=PPO", "--stop-reward=40", "--num-cpus=4"]
)

py_test(
    name = "examples/cartpole_lstm_ppo_torch",
    main = "examples/cartpole_lstm.py",
    tags = ["team:ml", "examples", "examples_C", "examples_C_AtoT"],
    size = "medium",
    srcs = ["examples/cartpole_lstm.py"],
    args = ["--as-test", "--framework=torch", "--run=PPO", "--stop-reward=40", "--num-cpus=4"]
)

py_test(
    name = "examples/cartpole_lstm_ppo_tf_with_prev_a_and_r",
    main = "examples/cartpole_lstm.py",
    tags = ["team:ml", "examples", "examples_C", "examples_C_AtoT"],
    size = "medium",
    srcs = ["examples/cartpole_lstm.py"],
    args = ["--as-test", "--run=PPO", "--stop-reward=40", "--use-prev-action",  "--use-prev-reward", "--num-cpus=4"]
)

py_test(
    name = "examples/centralized_critic_tf",
    main = "examples/centralized_critic.py",
    tags = ["team:ml", "examples", "examples_C", "examples_C_AtoT"],
    size = "large",
    srcs = ["examples/centralized_critic.py"],
    args = ["--as-test", "--stop-reward=7.2"]
)

py_test(
    name = "examples/centralized_critic_torch",
    main = "examples/centralized_critic.py",
    tags = ["team:ml", "examples", "examples_C", "examples_C_AtoT"],
    size = "large",
    srcs = ["examples/centralized_critic.py"],
    args = ["--as-test", "--framework=torch", "--stop-reward=7.2"]
)

py_test(
    name = "examples/centralized_critic_2_tf",
    main = "examples/centralized_critic_2.py",
    tags = ["team:ml", "examples", "examples_C", "examples_C_AtoT"],
    size = "medium",
    srcs = ["examples/centralized_critic_2.py"],
    args = ["--as-test", "--stop-reward=6.0"]
)

py_test(
    name = "examples/centralized_critic_2_torch",
    main = "examples/centralized_critic_2.py",
    tags = ["team:ml", "examples", "examples_C", "examples_C_AtoT"],
    size = "medium",
    srcs = ["examples/centralized_critic_2.py"],
    args = ["--as-test", "--framework=torch", "--stop-reward=6.0"]
)

py_test(
    name = "examples/checkpoint_by_custom_criteria",
    main = "examples/checkpoint_by_custom_criteria.py",
    tags = ["team:ml", "examples", "examples_C", "examples_C_AtoT"],
    size = "medium",
    srcs = ["examples/checkpoint_by_custom_criteria.py"],
    args = ["--stop-iters=3 --num-cpus=3"]
)

py_test(
    name = "examples/complex_struct_space_tf",
    main = "examples/complex_struct_space.py",
    tags = ["team:ml", "examples", "examples_C", "examples_C_AtoT"],
    size = "medium",
    srcs = ["examples/complex_struct_space.py"],
    args = ["--framework=tf"],
)

py_test(
    name = "examples/complex_struct_space_tf_eager",
    main = "examples/complex_struct_space.py",
    tags = ["team:ml", "examples", "examples_C", "examples_C_AtoT"],
    size = "medium",
    srcs = ["examples/complex_struct_space.py"],
    args = ["--framework=tfe"],
)

py_test(
    name = "examples/complex_struct_space_torch",
    main = "examples/complex_struct_space.py",
    tags = ["team:ml", "examples", "examples_C", "examples_C_AtoT"],
    size = "medium",
    srcs = ["examples/complex_struct_space.py"],
    args = ["--framework=torch"],
)

py_test(
    name = "examples/curriculum_learning",
    main = "examples/curriculum_learning.py",
    tags = ["team:ml", "examples", "examples_C", "examples_C_UtoZ"],
    size = "medium",
    srcs = ["examples/curriculum_learning.py"],
    args = ["--as-test", "--stop-reward=800.0"]
)

py_test(
    name = "examples/custom_env_tf",
    main = "examples/custom_env.py",
    tags = ["team:ml", "examples", "examples_C", "examples_C_UtoZ"],
    size = "medium",
    srcs = ["examples/custom_env.py"],
    args = ["--as-test"]
)

py_test(
    name = "examples/custom_env_torch",
    main = "examples/custom_env.py",
    tags = ["team:ml", "examples", "examples_C", "examples_C_UtoZ"],
    size = "large",
    srcs = ["examples/custom_env.py"],
    args = ["--as-test", "--framework=torch"]
)

py_test(
    name = "examples/custom_eval_tf",
    main = "examples/custom_eval.py",
    tags = ["team:ml", "examples", "examples_C", "examples_C_UtoZ"],
    size = "medium",
    srcs = ["examples/custom_eval.py"],
    args = ["--num-cpus=4", "--as-test"]
)

py_test(
    name = "examples/custom_eval_torch",
    main = "examples/custom_eval.py",
    tags = ["team:ml", "examples", "examples_C", "examples_C_UtoZ"],
    size = "medium",
    srcs = ["examples/custom_eval.py"],
    args = ["--num-cpus=4", "--as-test", "--framework=torch"]
)

py_test(
    name = "examples/custom_experiment",
    main = "examples/custom_experiment.py",
    tags = ["team:ml", "examples", "examples_C", "examples_C_UtoZ"],
    size = "medium",
    srcs = ["examples/custom_experiment.py"],
    args = ["--train-iterations=10"]
)

py_test(
    name = "examples/custom_fast_model_tf",
    main = "examples/custom_fast_model.py",
    tags = ["team:ml", "examples", "examples_C", "examples_C_UtoZ"],
    size = "medium",
    srcs = ["examples/custom_fast_model.py"],
    args = ["--stop-iters=1"]
)

py_test(
    name = "examples/custom_fast_model_torch",
    main = "examples/custom_fast_model.py",
    tags = ["team:ml", "examples", "examples_C", "examples_C_UtoZ"],
    size = "medium",
    srcs = ["examples/custom_fast_model.py"],
    args = ["--stop-iters=1", "--framework=torch"]
)

py_test(
    name = "examples/custom_keras_model_a2c",
    main = "examples/custom_keras_model.py",
    tags = ["team:ml", "examples", "examples_C", "examples_C_UtoZ"],
    size = "large",
    srcs = ["examples/custom_keras_model.py"],
    args = ["--run=A2C", "--stop=50", "--num-cpus=4"]
)

py_test(
    name = "examples/custom_keras_model_dqn",
    main = "examples/custom_keras_model.py",
    tags = ["team:ml", "examples", "examples_C", "examples_C_UtoZ"],
    size = "medium",
    srcs = ["examples/custom_keras_model.py"],
    args = ["--run=DQN", "--stop=50"]
)

py_test(
    name = "examples/custom_keras_model_ppo",
    main = "examples/custom_keras_model.py",
    tags = ["team:ml", "examples", "examples_C", "examples_C_UtoZ"],
    size = "medium",
    srcs = ["examples/custom_keras_model.py"],
    args = ["--run=PPO", "--stop=50", "--num-cpus=4"]
)

py_test(
    name = "examples/custom_metrics_and_callbacks",
    main = "examples/custom_metrics_and_callbacks.py",
    tags = ["team:ml", "examples", "examples_C", "examples_C_UtoZ"],
    size = "small",
    srcs = ["examples/custom_metrics_and_callbacks.py"],
    args = ["--stop-iters=2"]
)

py_test(
    name = "examples/custom_metrics_and_callbacks_legacy",
    main = "examples/custom_metrics_and_callbacks_legacy.py",
    tags = ["team:ml", "examples", "examples_C", "examples_C_UtoZ"],
    size = "small",
    srcs = ["examples/custom_metrics_and_callbacks_legacy.py"],
    args = ["--stop-iters=2"]
)

py_test(
    name = "examples/custom_model_api_tf",
    main = "examples/custom_model_api.py",
    tags = ["team:ml", "examples", "examples_C", "examples_C_UtoZ"],
    size = "small",
    srcs = ["examples/custom_model_api.py"],
)

py_test(
    name = "examples/custom_model_api_torch",
    main = "examples/custom_model_api.py",
    tags = ["team:ml", "examples", "examples_C", "examples_C_UtoZ"],
    size = "small",
    srcs = ["examples/custom_model_api.py"],
    args = ["--framework=torch"],
)

py_test(
    name = "examples/custom_model_loss_and_metrics_ppo_tf",
    main = "examples/custom_model_loss_and_metrics.py",
    tags = ["team:ml", "examples", "examples_C", "examples_C_UtoZ"],
    size = "medium",
    # Include the json data file.
    data = ["tests/data/cartpole/small.json"],
    srcs = ["examples/custom_model_loss_and_metrics.py"],
    args = ["--run=PPO", "--stop-iters=1", "--input-files=tests/data/cartpole"]
)

py_test(
    name = "examples/custom_model_loss_and_metrics_ppo_torch",
    main = "examples/custom_model_loss_and_metrics.py",
    tags = ["team:ml", "examples", "examples_C", "examples_C_UtoZ"],
    size = "medium",
    # Include the json data file.
    data = ["tests/data/cartpole/small.json"],
    srcs = ["examples/custom_model_loss_and_metrics.py"],
    args = ["--run=PPO", "--framework=torch", "--stop-iters=1", "--input-files=tests/data/cartpole"]
)

py_test(
    name = "examples/custom_model_loss_and_metrics_pg_tf",
    main = "examples/custom_model_loss_and_metrics.py",
    tags = ["team:ml", "examples", "examples_C", "examples_C_UtoZ"],
    size = "medium",
    # Include the json data file.
    data = ["tests/data/cartpole/small.json"],
    srcs = ["examples/custom_model_loss_and_metrics.py"],
    args = ["--run=PG", "--stop-iters=1", "--input-files=tests/data/cartpole"]
)

py_test(
    name = "examples/custom_model_loss_and_metrics_pg_torch",
    main = "examples/custom_model_loss_and_metrics.py",
    tags = ["team:ml", "examples", "examples_C", "examples_C_UtoZ"],
    size = "medium",
    # Include the json data file.
    data = ["tests/data/cartpole/small.json"],
    srcs = ["examples/custom_model_loss_and_metrics.py"],
    args = ["--run=PG", "--framework=torch", "--stop-iters=1", "--input-files=tests/data/cartpole"]
)

py_test(
    name = "examples/custom_observation_filters",
    main = "examples/custom_observation_filters.py",
    tags = ["team:ml", "examples", "examples_C", "examples_C_UtoZ"],
    size = "medium",
    srcs = ["examples/custom_observation_filters.py"],
    args = ["--stop-iters=3"]
)

py_test(
    name = "examples/custom_rnn_model_repeat_after_me_tf",
    main = "examples/custom_rnn_model.py",
    tags = ["team:ml", "examples", "examples_C", "examples_C_UtoZ"],
    size = "medium",
    srcs = ["examples/custom_rnn_model.py"],
    args = ["--as-test", "--run=PPO", "--stop-reward=40", "--env=RepeatAfterMeEnv", "--num-cpus=4"]
)

py_test(
    name = "examples/custom_rnn_model_repeat_initial_obs_tf",
    main = "examples/custom_rnn_model.py",
    tags = ["team:ml", "examples", "examples_C", "examples_C_UtoZ"],
    size = "medium",
    srcs = ["examples/custom_rnn_model.py"],
    args = ["--as-test", "--run=PPO", "--stop-reward=10", "--stop-timesteps=300000", "--env=RepeatInitialObsEnv", "--num-cpus=4"]
)

py_test(
    name = "examples/custom_rnn_model_repeat_after_me_torch",
    main = "examples/custom_rnn_model.py",
    tags = ["team:ml", "examples", "examples_C", "examples_C_UtoZ"],
    size = "medium",
    srcs = ["examples/custom_rnn_model.py"],
    args = ["--as-test", "--framework=torch", "--run=PPO", "--stop-reward=40", "--env=RepeatAfterMeEnv", "--num-cpus=4"]
)

py_test(
    name = "examples/custom_rnn_model_repeat_initial_obs_torch",
    main = "examples/custom_rnn_model.py",
    tags = ["team:ml", "examples", "examples_C", "examples_C_UtoZ"],
    size = "medium",
    srcs = ["examples/custom_rnn_model.py"],
    args = ["--as-test", "--framework=torch", "--run=PPO", "--stop-reward=10", "--stop-timesteps=300000", "--env=RepeatInitialObsEnv", "--num-cpus=4"]
)

py_test(
    name = "examples/custom_tf_policy",
    tags = ["team:ml", "examples", "examples_C", "examples_C_UtoZ"],
    size = "medium",
    srcs = ["examples/custom_tf_policy.py"],
    args = ["--stop-iters=2", "--num-cpus=4"]
)

py_test(
    name = "examples/custom_torch_policy",
    tags = ["team:ml", "examples", "examples_C", "examples_C_UtoZ"],
    size = "medium",
    srcs = ["examples/custom_torch_policy.py"],
    args = ["--stop-iters=2", "--num-cpus=4"]
)

py_test(
    name = "examples/custom_train_fn",
    main = "examples/custom_train_fn.py",
    tags = ["team:ml", "examples", "examples_C", "examples_C_UtoZ"],
    size = "medium",
    srcs = ["examples/custom_train_fn.py"],
)

py_test(
    name = "examples/custom_vector_env_tf",
    main = "examples/custom_vector_env.py",
    tags = ["team:ml", "examples", "examples_C", "examples_C_UtoZ"],
    size = "medium",
    srcs = ["examples/custom_vector_env.py"],
    args = ["--as-test", "--stop-reward=40.0"]
)

py_test(
    name = "examples/custom_vector_env_torch",
    main = "examples/custom_vector_env.py",
    tags = ["team:ml", "examples", "examples_C", "examples_C_UtoZ"],
    size = "medium",
    srcs = ["examples/custom_vector_env.py"],
    args = ["--as-test", "--framework=torch", "--stop-reward=40.0"]
)

py_test(
    name = "examples/deterministic_training_tf",
    main = "examples/deterministic_training.py",
    tags = ["team:ml", "multi_gpu"],
    size = "medium",
    srcs = ["examples/deterministic_training.py"],
    args = ["--as-test", "--stop-iters=1", "--framework=tf", "--num-gpus-trainer=1", "--num-gpus-per-worker=1"]
)

py_test(
    name = "examples/deterministic_training_tf2",
    main = "examples/deterministic_training.py",
    tags = ["team:ml", "multi_gpu"],
    size = "medium",
    srcs = ["examples/deterministic_training.py"],
    args = ["--as-test", "--stop-iters=1", "--framework=tf2", "--num-gpus-trainer=1", "--num-gpus-per-worker=1"]
)

py_test(
    name = "examples/deterministic_training_torch",
    main = "examples/deterministic_training.py",
    tags = ["team:ml", "multi_gpu"],
    size = "medium",
    srcs = ["examples/deterministic_training.py"],
    args = ["--as-test", "--stop-iters=1", "--framework=torch", "--num-gpus-trainer=1", "--num-gpus-per-worker=1"]
)

py_test(
    name = "examples/eager_execution",
    tags = ["team:ml", "examples", "examples_E"],
    size = "small",
    srcs = ["examples/eager_execution.py"],
    args = ["--stop-iters=2"]
)

py_test(
    name = "examples/export/cartpole_dqn_export",
    main = "examples/export/cartpole_dqn_export.py",
    tags = ["team:ml", "examples", "examples_E"],
    size = "medium",
    srcs = ["examples/export/cartpole_dqn_export.py"],
)

py_test(
    name = "examples/export/onnx_tf",
    main = "examples/export/onnx_tf.py",
    tags = ["team:ml", "examples", "examples_E"],
    size = "medium",
    srcs = ["examples/export/onnx_tf.py"],
)

py_test(
    name = "examples/export/onnx_torch",
    main = "examples/export/onnx_torch.py",
    tags = ["team:ml", "examples", "examples_E"],
    size = "medium",
    srcs = ["examples/export/onnx_torch.py"],
)

py_test(
    name = "examples/fractional_gpus",
    main = "examples/fractional_gpus.py",
    tags = ["team:ml", "examples", "examples_F"],
    size = "medium",
    srcs = ["examples/fractional_gpus.py"],
    args = ["--as-test", "--stop-reward=40.0", "--num-gpus=0", "--num-workers=0"]
)

py_test(
    name = "examples/hierarchical_training_tf",
    main = "examples/hierarchical_training.py",
    tags = ["team:ml", "examples", "examples_H"],
    size = "medium",
    srcs = ["examples/hierarchical_training.py"],
    args = ["--stop-reward=0.0"]
)

py_test(
    name = "examples/hierarchical_training_torch",
    main = "examples/hierarchical_training.py",
    tags = ["team:ml", "examples", "examples_H"],
    size = "medium",
    srcs = ["examples/hierarchical_training.py"],
    args = ["--framework=torch", "--stop-reward=0.0"]
)

# Do not run this test (MobileNetV2 is gigantic and takes forever for 1 iter).
# py_test(
#     name = "examples/mobilenet_v2_with_lstm_tf",
#     main = "examples/mobilenet_v2_with_lstm.py",
#     tags = ["team:ml", "examples", "examples_M"],
#     size = "small",
#     srcs = ["examples/mobilenet_v2_with_lstm.py"]
# )

py_test(
    name = "examples/multi_agent_cartpole_tf",
    main = "examples/multi_agent_cartpole.py",
    tags = ["team:ml", "examples", "examples_M"],
    size = "medium",
    srcs = ["examples/multi_agent_cartpole.py"],
    args = ["--as-test", "--stop-reward=70.0", "--num-cpus=4"]
)

py_test(
    name = "examples/multi_agent_cartpole_torch",
    main = "examples/multi_agent_cartpole.py",
    tags = ["team:ml", "examples", "examples_M"],
    size = "medium",
    srcs = ["examples/multi_agent_cartpole.py"],
    args = ["--as-test", "--framework=torch", "--stop-reward=70.0", "--num-cpus=4"]
)

py_test(
    name = "examples/multi_agent_custom_policy_tf",
    main = "examples/multi_agent_custom_policy.py",
    tags = ["team:ml", "examples", "examples_M"],
    size = "small",
    srcs = ["examples/multi_agent_custom_policy.py"],
    args = ["--as-test", "--stop-reward=80"]
)

py_test(
    name = "examples/multi_agent_custom_policy_torch",
    main = "examples/multi_agent_custom_policy.py",
    tags = ["team:ml", "examples", "examples_M"],
    size = "small",
    srcs = ["examples/multi_agent_custom_policy.py"],
    args = ["--as-test", "--framework=torch", "--stop-reward=80"]
)

py_test(
    name = "examples/multi_agent_two_trainers_tf",
    main = "examples/multi_agent_two_trainers.py",
    tags = ["team:ml", "examples", "examples_M"],
    size = "medium",
    srcs = ["examples/multi_agent_two_trainers.py"],
    args = ["--as-test", "--stop-reward=70"]
)

py_test(
    name = "examples/multi_agent_two_trainers_torch",
    main = "examples/multi_agent_two_trainers.py",
    tags = ["team:ml", "examples", "examples_M"],
    size = "medium",
    srcs = ["examples/multi_agent_two_trainers.py"],
    args = ["--as-test", "--framework=torch", "--stop-reward=70"]
)

# Taking out this test for now: Mixed torch- and tf- policies within the same
# Trainer never really worked.
# py_test(
#     name = "examples/multi_agent_two_trainers_mixed_torch_tf",
#     main = "examples/multi_agent_two_trainers.py",
#     tags = ["team:ml", "examples", "examples_M"],
#     size = "medium",
#     srcs = ["examples/multi_agent_two_trainers.py"],
#     args = ["--as-test", "--mixed-torch-tf", "--stop-reward=70"]
# )

py_test(
    name = "examples/nested_action_spaces_ppo_tf",
    main = "examples/nested_action_spaces.py",
    tags = ["team:ml", "examples", "examples_N"],
    size = "medium",
    srcs = ["examples/nested_action_spaces.py"],
    args = ["--as-test", "--stop-reward=-600", "--run=PPO"]
)

py_test(
    name = "examples/nested_action_spaces_ppo_torch",
    main = "examples/nested_action_spaces.py",
    tags = ["team:ml", "examples", "examples_N"],
    size = "medium",
    srcs = ["examples/nested_action_spaces.py"],
    args = ["--as-test", "--framework=torch", "--stop-reward=-600", "--run=PPO"]
)

py_test(
    name = "examples/parallel_evaluation_and_training_13_episodes_tf",
    main = "examples/parallel_evaluation_and_training.py",
    tags = ["team:ml", "examples", "examples_P"],
    size = "medium",
    srcs = ["examples/parallel_evaluation_and_training.py"],
    args = ["--as-test", "--stop-reward=50.0", "--num-cpus=6", "--evaluation-duration=13"]
)

py_test(
    name = "examples/parallel_evaluation_and_training_auto_episodes_tf",
    main = "examples/parallel_evaluation_and_training.py",
    tags = ["team:ml", "examples", "examples_P"],
    size = "medium",
    srcs = ["examples/parallel_evaluation_and_training.py"],
    args = ["--as-test", "--stop-reward=50.0", "--num-cpus=6", "--evaluation-duration=auto"]
)

py_test(
    name = "examples/parallel_evaluation_and_training_211_ts_tf2",
    main = "examples/parallel_evaluation_and_training.py",
    tags = ["team:ml", "examples", "examples_P"],
    size = "medium",
    srcs = ["examples/parallel_evaluation_and_training.py"],
    args = ["--as-test", "--framework=tf2", "--stop-reward=30.0", "--num-cpus=6", "--evaluation-num-workers=3", "--evaluation-duration=211", "--evaluation-duration-unit=timesteps"]
)

py_test(
    name = "examples/parallel_evaluation_and_training_auto_ts_torch",
    main = "examples/parallel_evaluation_and_training.py",
    tags = ["team:ml", "examples", "examples_P"],
    size = "medium",
    srcs = ["examples/parallel_evaluation_and_training.py"],
    args = ["--as-test", "--framework=torch", "--stop-reward=30.0", "--num-cpus=6", "--evaluation-num-workers=3", "--evaluation-duration=auto", "--evaluation-duration-unit=timesteps"]
)

py_test(
    name = "examples/parametric_actions_cartpole_pg_tf",
    main = "examples/parametric_actions_cartpole.py",
    tags = ["team:ml", "examples", "examples_P"],
    size = "medium",
    srcs = ["examples/parametric_actions_cartpole.py"],
    args = ["--as-test", "--stop-reward=60.0", "--run=PG"]
)

py_test(
    name = "examples/parametric_actions_cartpole_dqn_tf",
    main = "examples/parametric_actions_cartpole.py",
    tags = ["team:ml", "examples", "examples_P"],
    size = "medium",
    srcs = ["examples/parametric_actions_cartpole.py"],
    args = ["--as-test", "--stop-reward=60.0", "--run=DQN"]
)

py_test(
    name = "examples/parametric_actions_cartpole_pg_torch",
    main = "examples/parametric_actions_cartpole.py",
    tags = ["team:ml", "examples", "examples_P"],
    size = "medium",
    srcs = ["examples/parametric_actions_cartpole.py"],
    args = ["--as-test", "--framework=torch", "--stop-reward=60.0", "--run=PG"]
)

py_test(
    name = "examples/parametric_actions_cartpole_dqn_torch",
    main = "examples/parametric_actions_cartpole.py",
    tags = ["team:ml", "examples", "examples_P"],
    size = "medium",
    srcs = ["examples/parametric_actions_cartpole.py"],
    args = ["--as-test", "--framework=torch", "--stop-reward=60.0", "--run=DQN"]
)

py_test(
    name = "examples/parametric_actions_cartpole_embeddings_learnt_by_model",
    main = "examples/parametric_actions_cartpole_embeddings_learnt_by_model.py",
    tags = ["team:ml", "examples", "examples_P"],
    size = "medium",
    srcs = ["examples/parametric_actions_cartpole_embeddings_learnt_by_model.py"],
    args = ["--as-test", "--stop-reward=80.0"]
)

py_test(
    name = "examples/inference_and_serving/policy_inference_after_training_tf",
    main = "examples/inference_and_serving/policy_inference_after_training.py",
    tags = ["team:ml", "examples", "examples_P"],
    size = "medium",
    srcs = ["examples/inference_and_serving/policy_inference_after_training.py"],
    args = ["--stop-iters=3", "--framework=tf"]
)

py_test(
    name = "examples/inference_and_serving/policy_inference_after_training_torch",
    main = "examples/inference_and_serving/policy_inference_after_training.py",
    tags = ["team:ml", "examples", "examples_P"],
    size = "medium",
    srcs = ["examples/inference_and_serving/policy_inference_after_training.py"],
    args = ["--stop-iters=3", "--framework=torch"]
)

py_test(
    name = "examples/inference_and_serving/policy_inference_after_training_with_attention_tf",
    main = "examples/inference_and_serving/policy_inference_after_training_with_attention.py",
    tags = ["team:ml", "examples", "examples_P"],
    size = "medium",
    srcs = ["examples/inference_and_serving/policy_inference_after_training_with_attention.py"],
    args = ["--stop-iters=2", "--framework=tf"]
)

py_test(
    name = "examples/inference_and_serving/policy_inference_after_training_with_attention_torch",
    main = "examples/inference_and_serving/policy_inference_after_training_with_attention.py",
    tags = ["team:ml", "examples", "examples_P"],
    size = "medium",
    srcs = ["examples/inference_and_serving/policy_inference_after_training_with_attention.py"],
    args = ["--stop-iters=2", "--framework=torch"]
)

py_test(
    name = "examples/inference_and_serving/policy_inference_after_training_with_lstm_tf",
    main = "examples/inference_and_serving/policy_inference_after_training_with_lstm.py",
    tags = ["team:ml", "examples", "examples_P"],
    size = "medium",
    srcs = ["examples/inference_and_serving/policy_inference_after_training_with_lstm.py"],
    args = ["--stop-iters=1", "--framework=tf"]
)

py_test(
    name = "examples/inference_and_serving/policy_inference_after_training_with_lstm_torch",
    main = "examples/inference_and_serving/policy_inference_after_training_with_lstm.py",
    tags = ["team:ml", "examples", "examples_P"],
    size = "medium",
    srcs = ["examples/inference_and_serving/policy_inference_after_training_with_lstm.py"],
    args = ["--stop-iters=1", "--framework=torch"]
)

py_test(
    name = "examples/preprocessing_disabled_tf",
    main = "examples/preprocessing_disabled.py",
    tags = ["team:ml", "examples", "examples_P"],
    size = "medium",
    srcs = ["examples/preprocessing_disabled.py"],
    args = ["--stop-iters=2"]
)

py_test(
    name = "examples/preprocessing_disabled_torch",
    main = "examples/preprocessing_disabled.py",
    tags = ["team:ml", "examples", "examples_P"],
    size = "medium",
    srcs = ["examples/preprocessing_disabled.py"],
    args = ["--framework=torch", "--stop-iters=2"]
)

py_test(
    name = "examples/recommender_system_with_recsim_and_slateq_tf2",
    main = "examples/recommender_system_with_recsim_and_slateq.py",
    tags = ["team:ml", "examples", "examples_R"],
    size = "large",
    srcs = ["examples/recommender_system_with_recsim_and_slateq.py"],
    args = ["--stop-iters=2", "--learning-starts=100", "--framework=tf2", "--use-tune", "--random-test-episodes=10", "--env-num-candidates=50", "--env-slate-size=2"],
)

py_test(
    name = "examples/remote_envs_with_inference_done_on_main_node_tf",
    main = "examples/remote_envs_with_inference_done_on_main_node.py",
    tags = ["team:ml", "examples", "examples_R"],
    size = "medium",
    srcs = ["examples/remote_envs_with_inference_done_on_main_node.py"],
    args = ["--as-test"],
)

py_test(
    name = "examples/remote_envs_with_inference_done_on_main_node_torch",
    main = "examples/remote_envs_with_inference_done_on_main_node.py",
    tags = ["team:ml", "examples", "examples_R"],
    size = "medium",
    srcs = ["examples/remote_envs_with_inference_done_on_main_node.py"],
    args = ["--as-test", "--framework=torch"],
)

py_test(
    name = "examples/remote_base_env_with_custom_api",
    tags = ["team:ml", "examples", "examples_R"],
    size = "medium",
    srcs = ["examples/remote_base_env_with_custom_api.py"],
    args = ["--stop-iters=3"]
)

py_test(
    name = "examples/restore_1_of_n_agents_from_checkpoint",
    tags = ["team:ml", "examples", "examples_R"],
    size = "medium",
    srcs = ["examples/restore_1_of_n_agents_from_checkpoint.py"],
    args = ["--pre-training-iters=1", "--stop-iters=1", "--num-cpus=4"]
)

py_test(
    name = "examples/rnnsac_stateless_cartpole",
    tags = ["team:ml", "gpu"],
    size = "large",
    srcs = ["examples/rnnsac_stateless_cartpole.py"]
)

py_test(
    name = "examples/rollout_worker_custom_workflow",
    tags = ["team:ml", "examples", "examples_R"],
    size = "small",
    srcs = ["examples/rollout_worker_custom_workflow.py"],
    args = ["--num-cpus=4"]
)

py_test(
    name = "examples/rock_paper_scissors_multiagent_tf",
    main = "examples/rock_paper_scissors_multiagent.py",
    tags = ["team:ml", "examples", "examples_R"],
    size = "medium",
    srcs = ["examples/rock_paper_scissors_multiagent.py"],
    args = ["--as-test"],
)

py_test(
    name = "examples/rock_paper_scissors_multiagent_torch",
    main = "examples/rock_paper_scissors_multiagent.py",
    tags = ["team:ml", "examples", "examples_R"],
    size = "medium",
    srcs = ["examples/rock_paper_scissors_multiagent.py"],
    args = ["--as-test", "--framework=torch"],
)

# Deactivated for now due to open-spiel's dependency on an outdated
# tensorflow-probability version.
# py_test(
#    name = "examples/self_play_with_open_spiel_connect_4_tf",
#    main = "examples/self_play_with_open_spiel.py",
#    tags = ["team:ml", "examples", "examples_S"],
#    size = "medium",
#    srcs = ["examples/self_play_with_open_spiel.py"],
#    args = ["--framework=tf", "--env=connect_four", "--win-rate-threshold=0.6", "--stop-iters=2", "--num-episodes-human-play=0"]
# )

# py_test(
#    name = "examples/self_play_with_open_spiel_connect_4_torch",
#    main = "examples/self_play_with_open_spiel.py",
#    tags = ["team:ml", "examples", "examples_S"],
#    size = "medium",
#    srcs = ["examples/self_play_with_open_spiel.py"],
#    args = ["--framework=torch", "--env=connect_four", "--win-rate-threshold=0.6", "--stop-iters=2", "--num-episodes-human-play=0"]
# )

# py_test(
#    name = "examples/self_play_league_based_with_open_spiel_markov_soccer_tf",
#    main = "examples/self_play_league_based_with_open_spiel.py",
#    tags = ["team:ml", "examples", "examples_S"],
#    size = "medium",
#    srcs = ["examples/self_play_league_based_with_open_spiel.py"],
#    args = ["--framework=tf", "--env=markov_soccer", "--win-rate-threshold=0.6", "--stop-iters=2", "--num-episodes-human-play=0"]
# )

# py_test(
#    name = "examples/self_play_league_based_with_open_spiel_markov_soccer_torch",
#    main = "examples/self_play_league_based_with_open_spiel.py",
#    tags = ["team:ml", "examples", "examples_S"],
#    size = "medium",
#    srcs = ["examples/self_play_league_based_with_open_spiel_markov_soccer.py"],
#    args = ["--framework=torch", "--env=markov_soccer", "--win-rate-threshold=0.6", "--stop-iters=2", "--num-episodes-human-play=0"]
# )

py_test(
    name = "examples/trajectory_view_api_tf",
    main = "examples/trajectory_view_api.py",
    tags = ["team:ml", "examples", "examples_T"],
    size = "medium",
    srcs = ["examples/trajectory_view_api.py"],
    args = ["--as-test", "--framework=tf", "--stop-reward=100.0"]
)

py_test(
    name = "examples/trajectory_view_api_torch",
    main = "examples/trajectory_view_api.py",
    tags = ["team:ml", "examples", "examples_T"],
    size = "medium",
    srcs = ["examples/trajectory_view_api.py"],
    args = ["--as-test", "--framework=torch", "--stop-reward=100.0"]
)

py_test(
    name = "examples/tune/framework",
    main = "examples/tune/framework.py",
    tags = ["team:ml", "examples", "examples_F"],
    size = "medium",
    srcs = ["examples/tune/framework.py"],
    args = ["--smoke-test"]
)

py_test(
    name = "examples/two_trainer_workflow_tf",
    main = "examples/two_trainer_workflow.py",
    tags = ["team:ml", "examples", "examples_T"],
    size = "small",
    srcs = ["examples/two_trainer_workflow.py"],
    args = ["--as-test", "--stop-reward=100.0"]
)

py_test(
    name = "examples/two_trainer_workflow_torch",
    main = "examples/two_trainer_workflow.py",
    tags = ["team:ml", "examples", "examples_T"],
    size = "small",
    srcs = ["examples/two_trainer_workflow.py"],
    args = ["--as-test", "--torch", "--stop-reward=100.0"]
)

py_test(
    name = "examples/two_trainer_workflow_mixed_torch_tf",
    main = "examples/two_trainer_workflow.py",
    tags = ["team:ml", "examples", "examples_T"],
    size = "small",
    srcs = ["examples/two_trainer_workflow.py"],
    args = ["--as-test", "--mixed-torch-tf", "--stop-reward=100.0"]
)

py_test(
    name = "examples/two_step_game_maddpg",
    main = "examples/two_step_game.py",
    tags = ["team:ml", "examples", "examples_T"],
    size = "medium",
    srcs = ["examples/two_step_game.py"],
    args = ["--as-test", "--stop-reward=7.1", "--run=contrib/MADDPG"]
)

py_test(
    name = "examples/two_step_game_pg_tf",
    main = "examples/two_step_game.py",
    tags = ["team:ml", "examples", "examples_T"],
    size = "medium",
    srcs = ["examples/two_step_game.py"],
    args = ["--as-test", "--stop-reward=7", "--run=PG"]
)

py_test(
    name = "examples/two_step_game_pg_torch",
    main = "examples/two_step_game.py",
    tags = ["team:ml", "examples", "examples_T"],
    size = "medium",
    srcs = ["examples/two_step_game.py"],
    args = ["--as-test", "--framework=torch", "--stop-reward=7", "--run=PG"]
)


py_test(
    name = "examples/bandit/lin_ts_train_wheel_env",
    main = "examples/bandit/lin_ts_train_wheel_env.py",
    tags = ["team:ml", "examples"],
    size = "small",
    srcs = ["examples/bandit/lin_ts_train_wheel_env.py"],
)

py_test(
    name = "examples/bandit/tune_lin_ts_train_wheel_env",
    main = "examples/bandit/tune_lin_ts_train_wheel_env.py",
    tags = ["team:ml", "examples"],
    size = "small",
    srcs = ["examples/bandit/tune_lin_ts_train_wheel_env.py"],
)

py_test(
    name = "examples/bandit/tune_lin_ucb_train_recommendation",
    main = "examples/bandit/tune_lin_ucb_train_recommendation.py",
    tags = ["team:ml", "examples", ],
    size = "small",
    srcs = ["examples/bandit/tune_lin_ucb_train_recommendation.py"],
)

py_test(
    name = "examples/bandit/tune_lin_ucb_train_recsim_env",
    main = "examples/bandit/tune_lin_ucb_train_recsim_env.py",
    tags = ["team:ml", "examples", ],
    size = "small",
    srcs = ["examples/bandit/tune_lin_ucb_train_recsim_env.py"],
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
    tags = ["team:ml", "documentation"],
    size = "medium",
    srcs = ["examples/documentation/custom_gym_env.py"],
)

py_test(
    name = "examples/documentation/rllib_in_60s",
    main = "examples/documentation/rllib_in_60s.py",
    tags = ["team:ml", "documentation"],
    size = "medium",
    srcs = ["examples/documentation/rllib_in_60s.py"],
)

py_test(
    name = "examples/documentation/rllib_on_ray_readme",
    main = "examples/documentation/rllib_on_ray_readme.py",
    tags = ["team:ml", "documentation"],
    size = "medium",
    srcs = ["examples/documentation/rllib_on_ray_readme.py"],
)

py_test(
    name = "examples/documentation/rllib_on_rllib_readme",
    main = "examples/documentation/rllib_on_rllib_readme.py",
    tags = ["team:ml", "documentation"],
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
    "tests/test_vector_env.py",
    "env/tests/test_multi_agent_env.py",
    "env/wrappers/tests/test_kaggle_wrapper.py",
    "examples/env/tests/test_coin_game_non_vectorized_env.py",
    "examples/env/tests/test_coin_game_vectorized_env.py",
    "examples/env/tests/test_matrix_sequential_social_dilemma.py",
    "examples/env/tests/test_wrappers.py",
    "execution/tests/test_mixin_multi_agent_replay_buffer.py",
    "utils/tests/test_check_env.py",
    "utils/tests/test_check_multi_agent.py",
    "utils/tests/test_errors.py",
    "utils/tests/test_utils.py",
  ],
  size = "large",
  extra_srcs = [],
  deps = [],
  tags = ["manual", "team:ml"],
)
