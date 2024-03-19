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
# -- "ray_data": Tests that rely on ray_data.
# -- "learning_tests_with_ray_data": Learning tests that rely on ray_data.

# - Folder-bound tests, tagged with the name of the top-level dir:
#   - `env` directory tests.
#   - `evaluation` directory tests.
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
# - "multi_gpu": Indicating that a test will definitely be run in the Large GPU
#   pipeline, defined in .buildkite/pipeline.gpu.large.yml.
# - "no_gpu": Indicating that a test should not be run in the GPU pipeline due
#   to certain incompatibilities.
# - "no_tf_eager_tracing": Exclude this test from tf-eager tracing tests.
# - "torch_only": Only run this test case with framework=torch.

# Our .buildkite/pipeline.yml and .buildkite/pipeline.gpu.yml files execute all
# these tests in n different jobs.

load("//bazel:python.bzl", "py_test_module_list")
load("//bazel:python.bzl", "doctest")

doctest(
    size = "enormous",
    files = glob(
        ["**/*.py"],
        exclude = [
            "**/examples/**",
            "**/tests/**",
            "**/test_*.py",
            # Deprecated modules
            "utils/window_stat.py",
            "utils/timer.py",
            "utils/memory.py",
            "offline/off_policy_estimator.py",
            "offline/estimators/feature_importance.py",
            "env/wrappers/recsim_wrapper.py",
            "env/remote_vector_env.py",
            # Missing imports
            "algorithms/dreamerv3/**",
            # FIXME: These modules contain broken examples that weren't previously
            # tested.
            "algorithms/algorithm_config.py",
            "algorithms/alpha_star/alpha_star.py",
            "algorithms/r2d2/r2d2.py",
            "algorithms/sac/rnnsac.py",
            "algorithms/simple_q/simple_q.py",
            "core/models/base.py",
            "core/models/specs/specs_base.py",
            "core/models/specs/specs_dict.py",
            "env/wrappers/pettingzoo_env.py",
            "evaluation/collectors/sample_collector.py",
            "evaluation/episode.py",
            "evaluation/metrics.py",
            "evaluation/observation_function.py",
            "evaluation/postprocessing.py",
            "execution/buffers/mixin_replay_buffer.py",
            "models/base_model.py",
            "models/catalog.py",
            "models/preprocessors.py",
            "models/repeated_values.py",
            "models/tf/tf_distributions.py",
            "models/torch/model.py",
            "models/torch/torch_distributions.py",
            "policy/rnn_sequencing.py",
            "utils/actor_manager.py",
            "utils/filter.py",
            "utils/from_config.py",
            "utils/metrics/window_stat.py",
            "utils/nested_dict.py",
            "utils/pre_checks/env.py",
            "utils/replay_buffers/multi_agent_mixin_replay_buffer.py",
            "utils/spaces/space_utils.py",
        ],
    ),
    tags = ["team:rllib"],
)

# --------------------------------------------------------------------
# Benchmarks
#
# Tag: benchmark
#
# This is smoke-testing the benchmark scripts.
# --------------------------------------------------------------------
py_test(
    name = "torch_compile_inference_bm",
    size = "small",
    srcs = ["benchmarks/torch_compile/run_inference_bm.py"],
    args = ["--smoke-test"],
    main = "benchmarks/torch_compile/run_inference_bm.py",
    tags = [
        "benchmark",
        "exclusive",
        "team:rllib",
        "torch_2.x_only_benchmark",
    ],
)

py_test(
    name = "torch_compile_ppo_with_inference",
    size = "medium",
    srcs = ["benchmarks/torch_compile/run_ppo_with_inference_bm.py"],
    args = ["--smoke-test"],
    main = "benchmarks/torch_compile/run_ppo_with_inference_bm.py",
    tags = [
        "benchmark",
        "exclusive",
        "team:rllib",
        "torch_2.x_only_benchmark",
    ],
)

# --------------------------------------------------------------------
# Algorithms learning regression tests.
#
# Tag: learning_tests
#
# This will test all yaml files (via `rllib train`)
# inside rllib/tuned_examples/[algo-name] for actual learning success.
# --------------------------------------------------------------------

# APPO
py_test(
    name = "learning_tests_cartpole_appo_w_rl_modules_and_learner",
    size = "medium",  # bazel may complain about it being too long sometimes - medium is on purpose as some frameworks take longer
    srcs = ["tests/run_regression_tests.py"],
    args = ["--dir=tuned_examples/appo"],
    data = ["tuned_examples/appo/cartpole-appo-w-rl-modules-and-learner.yaml"],
    main = "tests/run_regression_tests.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_cartpole",
        "learning_tests_discrete",
        "no_tf_static_graph",
        "team:rllib",
    ],
)

py_test(
    name = "learning_tests_cartpole_separate_losses_appo",
    size = "medium",
    srcs = ["tests/run_regression_tests.py"],
    args = ["--dir=tuned_examples/appo"],
    data = [
        "tuned_examples/appo/cartpole-appo-separate-losses.py",
    ],
    main = "tests/run_regression_tests.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_cartpole",
        "learning_tests_discrete",
        "team:rllib",
    ],
)

py_test(
    name = "learning_tests_multi_agent_cartpole_appo",
    size = "medium",
    srcs = ["tests/run_regression_tests.py"],
    args = ["--dir=tuned_examples/appo"],
    data = ["tuned_examples/appo/multi_agent_cartpole_appo.py"],
    main = "tests/run_regression_tests.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_cartpole",
        "learning_tests_discrete",
        "team:rllib",
    ],
)

py_test(
    name = "learning_tests_multi_agent_cartpole_w_100_policies_appo",
    size = "enormous",
    srcs = ["tests/run_regression_tests.py"],
    args = ["--dir=tuned_examples/appo"],
    data = ["tuned_examples/appo/multi-agent-cartpole-w-100-policies-appo.py"],
    main = "tests/run_regression_tests.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_cartpole",
        "learning_tests_discrete",
        "team:rllib",
    ],
)

py_test(
    name = "learning_tests_cartpole_appo_fake_gpus",
    size = "medium",
    srcs = ["tests/run_regression_tests.py"],
    args = ["--dir=tuned_examples/appo"],
    data = ["tuned_examples/appo/cartpole-appo-fake-gpus.yaml"],
    main = "tests/run_regression_tests.py",
    tags = [
        "exclusive",
        "fake_gpus",
        "learning_tests",
        "learning_tests_cartpole",
        "learning_tests_discrete",
        "team:rllib",
    ],
)

py_test(
    name = "learning_tests_stateless_cartpole_appo",
    size = "enormous",
    srcs = ["tests/run_regression_tests.py"],
    args = ["--dir=tuned_examples/appo"],
    data = ["tuned_examples/appo/stateless_cartpole_appo.py"],
    main = "tests/run_regression_tests.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_cartpole",
        "learning_tests_discrete",
        "team:rllib",
    ],
)

# Tests against crashing or hanging environments.
# Single-agent: Crash only.
py_test(
    name = "learning_tests_cartpole_crashing_appo",
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    args = [
        "--dir=tuned_examples/appo",
        "--num-cpus=6",
    ],
    data = ["tuned_examples/appo/cartpole-crashing-recreate-workers-appo.py"],
    main = "tests/run_regression_tests.py",
    tags = [
        "crashing_cartpole",
        "exclusive",
        "learning_tests",
        "learning_tests_cartpole",
        "learning_tests_discrete",
        "no_tf_static_graph",
        "team:rllib",
    ],
)

# Single-agent: Crash and stall.
py_test(
    name = "learning_tests_cartpole_crashing_and_stalling_appo",
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    args = [
        "--dir=tuned_examples/appo",
        "--num-cpus=6",
    ],
    data = ["tuned_examples/appo/cartpole-crashing-and-stalling-recreate-workers-appo.py"],
    main = "tests/run_regression_tests.py",
    tags = [
        "crashing_cartpole",
        "exclusive",
        "learning_tests",
        "learning_tests_cartpole",
        "learning_tests_discrete",
        "team:rllib",
    ],
)

# Multi-agent: Crash only.
py_test(
    name = "learning_tests_multi_agent_cartpole_crashing_appo",
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    args = [
        "--dir=tuned_examples/appo",
        "--num-cpus=6",
    ],
    data = ["tuned_examples/appo/multi-agent-cartpole-crashing-recreate-workers-appo.py"],
    main = "tests/run_regression_tests.py",
    tags = [
        "crashing_cartpole",
        "exclusive",
        "learning_tests",
        "learning_tests_cartpole",
        "learning_tests_discrete",
        "team:rllib",
    ],
)

# Multi-agent: Crash and stall.
py_test(
    name = "learning_tests_multi_agent_cartpole_crashing_and_stalling_appo",
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    args = [
        "--dir=tuned_examples/appo",
        "--num-cpus=6",
    ],
    data = ["tuned_examples/appo/multi-agent-cartpole-crashing-and-stalling-recreate-workers-appo.py"],
    main = "tests/run_regression_tests.py",
    tags = [
        "crashing_cartpole",
        "exclusive",
        "learning_tests",
        "learning_tests_cartpole",
        "learning_tests_discrete",
        "team:rllib",
    ],
)

# CQL
py_test(
    name = "learning_tests_pendulum_cql",
    size = "medium",
    srcs = ["tests/run_regression_tests.py"],
    args = ["--dir=tuned_examples/cql"],
    # Include the zipped json data file as well.
    data = [
        "tests/data/pendulum/enormous.zip",
        "tuned_examples/cql/pendulum-cql.yaml",
    ],
    main = "tests/run_regression_tests.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_continuous",
        "learning_tests_pendulum",
        "learning_tests_with_ray_data",
        "team:rllib",
    ],
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
    size = "large",  # bazel may complain about it being too long sometimes - large is on purpose as some frameworks take longer
    srcs = ["tests/run_regression_tests.py"],
    args = ["--dir=tuned_examples/dqn"],
    data = ["tuned_examples/dqn/cartpole-dqn-softq.yaml"],
    main = "tests/run_regression_tests.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_cartpole",
        "learning_tests_discrete",
        "team:rllib",
    ],
)

py_test(
    name = "learning_tests_cartpole_dqn_fake_gpus",
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    args = ["--dir=tuned_examples/dqn"],
    data = ["tuned_examples/dqn/cartpole-dqn-fake-gpus.yaml"],
    main = "tests/run_regression_tests.py",
    tags = [
        "exclusive",
        "fake_gpus",
        "learning_tests",
        "learning_tests_cartpole",
        "learning_tests_discrete",
        "team:rllib",
    ],
)

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
    name = "learning_tests_cartpole_separate_losses_impala",
    size = "medium",
    srcs = ["tests/run_regression_tests.py"],
    args = ["--dir=tuned_examples/impala"],
    data = [
        "tuned_examples/impala/cartpole-impala-separate-losses.py",
    ],
    main = "tests/run_regression_tests.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_cartpole",
        "learning_tests_discrete",
        "team:rllib",
    ],
)

py_test(
    name = "learning_tests_multi_agent_cartpole_impala",
    size = "medium",
    srcs = ["tests/run_regression_tests.py"],
    args = ["--dir=tuned_examples/impala"],
    data = ["tuned_examples/impala/multi_agent_cartpole_impala.py"],
    main = "tests/run_regression_tests.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_cartpole",
        "learning_tests_discrete",
        "team:rllib",
    ],
)

py_test(
    name = "learning_tests_cartpole_impala_fake_gpus",
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    args = ["--dir=tuned_examples/impala"],
    data = ["tuned_examples/impala/cartpole-impala-fake-gpus.yaml"],
    main = "tests/run_regression_tests.py",
    tags = [
        "exclusive",
        "fake_gpus",
        "learning_tests",
        "learning_tests_cartpole",
        "learning_tests_discrete",
        "team:rllib",
    ],
)

# PPO
# w/ new EnvRunner
py_test(
    name = "learning_tests_cartpole_ppo_w_env_runner",
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    args = ["--dir=tuned_examples/ppo"],
    data = ["tuned_examples/ppo/cartpole_ppo_envrunner.py"],
    main = "tests/run_regression_tests.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_cartpole",
        "learning_tests_discrete",
        "no_tf_static_graph",
        "team:rllib",
    ],
)

# TODO (sven): Remove the torch only flag for this test (tf2 is still very slow for EnvRunner and we need to debug this further).
py_test(
    name = "learning_tests_pendulum_ppo_w_env_runner",
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    args = ["--dir=tuned_examples/ppo"],
    data = ["tuned_examples/ppo/pendulum_ppo_envrunner.py"],
    main = "tests/run_regression_tests.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_continuous",
        "learning_tests_pendulum",
        "team:rllib",
        "torch_only",
    ],
)

# w/o EnvRunner (using RolloutWorker)
py_test(
    name = "learning_tests_cartpole_truncated_ppo",
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    args = ["--dir=tuned_examples/ppo"],
    data = ["tuned_examples/ppo/cartpole_truncated_ppo.py"],
    main = "tests/run_regression_tests.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_cartpole",
        "learning_tests_discrete",
        "no_tf_static_graph",
        "team:rllib",
    ],
)

py_test(
    name = "learning_tests_pendulum_ppo",
    size = "large",  # bazel may complain about it being too long sometimes - large is on purpose as some frameworks take longer
    srcs = ["tests/run_regression_tests.py"],
    args = ["--dir=tuned_examples/ppo"],
    data = ["tuned_examples/ppo/pendulum-ppo.yaml"],
    main = "tests/run_regression_tests.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_continuous",
        "learning_tests_pendulum",
        "no_tf_static_graph",
        "team:rllib",
    ],
)

py_test(
    name = "learning_tests_multi_agent_pendulum_ppo_envrunner",
    size = "large",  # bazel may complain about it being too long sometimes - large is on purpose as some frameworks take longer
    srcs = ["tests/run_regression_tests.py"],
    args = ["--dir=tuned_examples/ppo"],
    data = ["tuned_examples/ppo/multi_agent_pendulum_ppo_envrunner.py"],
    main = "tests/run_regression_tests.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_continuous",
        "learning_tests_pendulum",
        "no_tf_static_graph",
        "team:rllib",
    ],
)

py_test(
    name = "learning_tests_transformed_actions_pendulum_ppo",
    size = "large",  # bazel may complain about it being too long sometimes - large is on purpose as some frameworks take longer
    srcs = ["tests/run_regression_tests.py"],
    args = ["--dir=tuned_examples/ppo"],
    data = ["tuned_examples/ppo/pendulum-transformed-actions-ppo.yaml"],
    main = "tests/run_regression_tests.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_continuous",
        "learning_tests_pendulum",
        "no_tf_static_graph",
        "team:rllib",
    ],
)

py_test(
    name = "learning_tests_repeat_after_me_ppo_wo_rl_module",
    size = "medium",
    srcs = ["tests/run_regression_tests.py"],
    args = ["--dir=tuned_examples/ppo"],
    data = ["tuned_examples/ppo/repeatafterme-ppo-lstm.yaml"],
    main = "tests/run_regression_tests.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_discrete",
        "team:rllib",
    ],
)

py_test(
    name = "learning_tests_cartpole_ppo_fake_gpus",
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    args = ["--dir=tuned_examples/ppo"],
    data = ["tuned_examples/ppo/cartpole-ppo-fake-gpus.yaml"],
    main = "tests/run_regression_tests.py",
    tags = [
        "exclusive",
        "fake_gpus",
        "learning_tests",
        "learning_tests_cartpole",
        "learning_tests_discrete",
        "team:rllib",
    ],
)

# SAC
py_test(
    name = "learning_tests_pendulum_sac_envrunner",
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    args = ["--dir=tuned_examples/sac"],
    data = ["tuned_examples/sac/pendulum_sac_envrunner.py"],
    main = "tests/run_regression_tests.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_continuous",
        "learning_tests_pendulum",
        "team:rllib",
        "torch_only",
    ],
)

py_test(
    name = "learning_tests_cartpole_sac",
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    args = ["--dir=tuned_examples/sac"],
    data = ["tuned_examples/sac/cartpole-sac.yaml"],
    main = "tests/run_regression_tests.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_cartpole",
        "learning_tests_discrete",
        "team:rllib",
    ],
)

# --------------------------------------------------------------------
# Algorithms (Compilation, Losses, simple functionality tests)
# rllib/algorithms/
#
# Tag: algorithms_dir
# --------------------------------------------------------------------

# Generic (all Algorithms)

py_test(
    name = "test_algorithm",
    size = "large",
    srcs = ["algorithms/tests/test_algorithm.py"],
    data = ["tests/data/cartpole/small.json"],
    tags = [
        "algorithms_dir",
        "algorithms_dir_generic",
        "team:rllib",
    ],
)

py_test(
    name = "test_algorithm_config",
    size = "medium",
    srcs = ["algorithms/tests/test_algorithm_config.py"],
    tags = [
        "algorithms_dir",
        "algorithms_dir_generic",
        "team:rllib",
    ],
)

py_test(
    name = "test_algorithm_export_checkpoint",
    size = "medium",
    srcs = ["algorithms/tests/test_algorithm_export_checkpoint.py"],
    tags = [
        "algorithms_dir",
        "algorithms_dir_generic",
        "team:rllib",
    ],
)

py_test(
    name = "test_callbacks_on_algorithm",
    size = "large",
    srcs = ["algorithms/tests/test_callbacks_on_algorithm.py"],
    tags = [
        "algorithms_dir",
        "algorithms_dir_generic",
        "team:rllib",
    ],
)

py_test(
    name = "test_callbacks_on_env_runner",
    size = "medium",
    srcs = ["algorithms/tests/test_callbacks_on_env_runner.py"],
    tags = [
        "algorithms_dir",
        "algorithms_dir_generic",
        "team:rllib",
    ],
)

py_test(
    name = "test_callbacks_old_stack",
    size = "medium",
    srcs = ["algorithms/tests/test_callbacks_old_stack.py"],
    tags = [
        "algorithms_dir",
        "algorithms_dir_generic",
        "team:rllib",
    ],
)

py_test(
    name = "test_memory_leaks_generic",
    size = "medium",
    srcs = ["algorithms/tests/test_memory_leaks.py"],
    main = "algorithms/tests/test_memory_leaks.py",
    tags = [
        "algorithms_dir",
        "team:rllib",
    ],
)

py_test(
    name = "test_node_failure",
    size = "medium",
    srcs = ["tests/test_node_failure.py"],
    tags = [
        "exclusive",
        "team:rllib",
        "tests_dir",
    ],
)

py_test(
    name = "test_registry",
    size = "small",
    srcs = ["algorithms/tests/test_registry.py"],
    tags = [
        "algorithms_dir",
        "algorithms_dir_generic",
        "team:rllib",
    ],
)

py_test(
    name = "test_worker_failures",
    size = "large",
    srcs = ["algorithms/tests/test_worker_failures.py"],
    tags = [
        "algorithms_dir",
        "algorithms_dir_generic",
        "exclusive",
        "team:rllib",
    ],
)

# Specific Algorithms

# APPO
py_test(
    name = "test_appo",
    size = "large",
    srcs = ["algorithms/appo/tests/test_appo.py"],
    tags = [
        "algorithms_dir",
        "team:rllib",
    ],
)

py_test(
    name = "test_appo_off_policyness",
    size = "large",
    srcs = ["algorithms/appo/tests/test_appo_off_policyness.py"],
    tags = [
        "algorithms_dir",
        "exclusive",
        "multi_gpu",
        "team:rllib",
    ],
)

py_test(
    name = "test_appo_learner",
    size = "medium",
    srcs = ["algorithms/appo/tests/test_appo_learner.py"],
    tags = [
        "algorithms_dir",
        "team:rllib",
    ],
)

# BC
py_test(
    name = "test_bc",
    size = "medium",
    srcs = ["algorithms/bc/tests/test_bc.py"],
    # Include the json data file.
    data = ["tests/data/cartpole/large.json"],
    tags = [
        "algorithms_dir",
        "team:rllib",
    ],
)

# CQL
py_test(
    name = "test_cql",
    size = "large",
    srcs = ["algorithms/cql/tests/test_cql.py"],
    data = ["tests/data/pendulum/small.json"],
    tags = [
        "algorithms_dir",
        "team:rllib",
    ],
)

# DQN
py_test(
    name = "test_dqn",
    size = "large",
    srcs = ["algorithms/dqn/tests/test_dqn.py"],
    tags = [
        "algorithms_dir",
        "team:rllib",
    ],
)

py_test(
    name = "test_repro_dqn",
    size = "large",
    srcs = ["algorithms/dqn/tests/test_repro_dqn.py"],
    tags = [
        "algorithms_dir",
        "gpu",
        "team:rllib",
    ],
)

# DreamerV3
py_test(
    name = "test_dreamerv3",
    size = "large",
    srcs = ["algorithms/dreamerv3/tests/test_dreamerv3.py"],
    tags = [
        "algorithms_dir",
        "team:rllib",
    ],
)

# Impala
py_test(
    name = "test_impala",
    size = "large",
    srcs = ["algorithms/impala/tests/test_impala.py"],
    tags = [
        "algorithms_dir",
        "team:rllib",
    ],
)

py_test(
    name = "test_vtrace",
    size = "small",
    srcs = ["algorithms/impala/tests/test_vtrace.py"],
    tags = [
        "algorithms_dir",
        "team:rllib",
    ],
)

py_test(
    name = "test_vtrace_v2",
    size = "small",
    srcs = ["algorithms/impala/tests/test_vtrace_v2.py"],
    tags = [
        "algorithms_dir",
        "team:rllib",
    ],
)

py_test(
    name = "test_impala_off_policyness",
    size = "large",
    srcs = ["algorithms/impala/tests/test_impala_off_policyness.py"],
    tags = [
        "algorithms_dir",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "test_impala_learner",
    size = "medium",
    srcs = ["algorithms/impala/tests/test_impala_learner.py"],
    tags = [
        "algorithms_dir",
        "team:rllib",
    ],
)

# MARWIL
py_test(
    name = "test_marwil",
    size = "large",
    srcs = ["algorithms/marwil/tests/test_marwil.py"],
    # Include the json data file.
    data = [
        "tests/data/cartpole/large.json",
        "tests/data/cartpole/small.json",
        "tests/data/pendulum/large.json",
    ],
    tags = [
        "algorithms_dir",
        "team:rllib",
    ],
)

# PPO
py_test(
    name = "test_ppo_with_env_runner",
    size = "medium",
    srcs = ["algorithms/ppo/tests/test_ppo_with_env_runner.py"],
    tags = [
        "algorithms_dir",
        "team:rllib",
    ],
)

py_test(
    name = "test_ppo",
    size = "large",
    srcs = ["algorithms/ppo/tests/test_ppo.py"],
    tags = [
        "algorithms_dir",
        "team:rllib",
    ],
)

py_test(
    name = "test_ppo_with_rl_module",
    size = "large",
    srcs = ["algorithms/ppo/tests/test_ppo_with_rl_module.py"],
    tags = [
        "algorithms_dir",
        "team:rllib",
    ],
)

py_test(
    name = "test_ppo_rl_module",
    size = "large",
    srcs = ["algorithms/ppo/tests/test_ppo_rl_module.py"],
    tags = [
        "algorithms_dir",
        "team:rllib",
    ],
)

py_test(
    name = "test_ppo_learner",
    size = "large",
    srcs = ["algorithms/ppo/tests/test_ppo_learner.py"],
    tags = [
        "algorithms_dir",
        "team:rllib",
    ],
)

py_test(
    name = "test_repro_ppo",
    size = "large",
    srcs = ["algorithms/ppo/tests/test_repro_ppo.py"],
    tags = [
        "algorithms_dir",
        "gpu",
        "team:rllib",
    ],
)

# SAC
py_test(
    name = "test_sac",
    size = "large",
    srcs = ["algorithms/sac/tests/test_sac.py"],
    tags = [
        "algorithms_dir",
        "team:rllib",
    ],
)

# RNNSAC
py_test(
    name = "test_rnnsac",
    size = "small",
    srcs = ["algorithms/sac/tests/test_rnnsac.py"],
    tags = [
        "algorithms_dir",
        "team:rllib",
    ],
)

# --------------------------------------------------------------------
# Memory leak tests
#
# Tag: memory_leak_tests
# --------------------------------------------------------------------

py_test(
    name = "test_memory_leak_appo",
    size = "large",
    srcs = ["utils/tests/run_memory_leak_tests.py"],
    args = ["--dir=tuned_examples/appo"],
    data = ["tuned_examples/appo/memory-leak-test-appo.yaml"],
    main = "utils/tests/run_memory_leak_tests.py",
    tags = [
        "memory_leak_tests",
        "team:rllib",
    ],
)

py_test(
    name = "test_memory_leak_dqn",
    size = "large",
    srcs = ["utils/tests/run_memory_leak_tests.py"],
    args = ["--dir=tuned_examples/dqn"],
    data = ["tuned_examples/dqn/memory-leak-test-dqn.yaml"],
    main = "utils/tests/run_memory_leak_tests.py",
    tags = [
        "memory_leak_tests",
        "team:rllib",
    ],
)

py_test(
    name = "test_memory_leak_impala",
    size = "large",
    srcs = ["utils/tests/run_memory_leak_tests.py"],
    args = ["--dir=tuned_examples/impala"],
    data = ["tuned_examples/impala/memory-leak-test-impala.yaml"],
    main = "utils/tests/run_memory_leak_tests.py",
    tags = [
        "memory_leak_tests",
        "team:rllib",
    ],
)

py_test(
    name = "test_memory_leak_ppo",
    size = "large",
    srcs = ["utils/tests/run_memory_leak_tests.py"],
    args = ["--dir=tuned_examples/ppo"],
    data = ["tuned_examples/ppo/memory-leak-test-ppo.yaml"],
    main = "utils/tests/run_memory_leak_tests.py",
    tags = [
        "memory_leak_tests",
        "team:rllib",
    ],
)

py_test(
    name = "test_memory_leak_ppo_new_stack",
    size = "large",
    srcs = ["utils/tests/run_memory_leak_tests.py"],
    args = [
        "--dir=tuned_examples/ppo",
        "--to-check=rollout_worker",
    ],
    data = ["tuned_examples/ppo/memory_leak_test_ppo_new_stack.py"],
    main = "utils/tests/run_memory_leak_tests.py",
    tags = [
        "memory_leak_tests",
        "team:rllib",
    ],
)

py_test(
    name = "test_memory_leak_sac",
    size = "large",
    srcs = ["utils/tests/run_memory_leak_tests.py"],
    args = ["--dir=tuned_examples/sac"],
    data = ["tuned_examples/sac/memory-leak-test-sac.yaml"],
    main = "utils/tests/run_memory_leak_tests.py",
    tags = [
        "memory_leak_tests",
        "team:rllib",
    ],
)

# --------------------------------------------------------------------
# Connector(V1) tests
# rllib/connector/
#
# Tag: connector
# --------------------------------------------------------------------

py_test(
    name = "connectors/tests/test_connector",
    size = "small",
    srcs = ["connectors/tests/test_connector.py"],
    tags = [
        "connector",
        "team:rllib",
    ],
)

py_test(
    name = "connectors/tests/test_action",
    size = "small",
    srcs = ["connectors/tests/test_action.py"],
    tags = [
        "connector",
        "team:rllib",
    ],
)

py_test(
    name = "connectors/tests/test_agent",
    size = "medium",
    srcs = ["connectors/tests/test_agent.py"],
    tags = [
        "connector",
        "team:rllib",
    ],
)

# --------------------------------------------------------------------
# ConnectorV2 tests
# rllib/connector/
#
# Tag: connector_v2
# --------------------------------------------------------------------

# TODO (sven): Add these tests in a separate PR.
# py_test(
#    name = "connectors/tests/test_connector_v2",
#    tags = ["team:rllib", "connector_v2"],
#    size = "small",
#    srcs = ["connectors/tests/test_connector_v2.py"]
# )

# --------------------------------------------------------------------
# Env tests
# rllib/env/
#
# Tag: env
# --------------------------------------------------------------------

py_test(
    name = "env/tests/test_infinite_lookback_buffer",
    size = "small",
    srcs = ["env/tests/test_infinite_lookback_buffer.py"],
    tags = [
        "env",
        "team:rllib",
    ],
)

sh_test(
    name = "env/tests/test_local_inference_cartpole",
    size = "medium",
    srcs = ["env/tests/test_policy_client_server_setup.sh"],
    args = [
        "local",
        "cartpole",
        "8800",
    ],
    data = glob(["examples/serving/*.py"]),
    tags = [
        "env",
        "team:rllib",
    ],
)

sh_test(
    name = "env/tests/test_local_inference_cartpole_w_2_concurrent_episodes",
    size = "medium",
    srcs = ["env/tests/test_policy_client_server_setup.sh"],
    args = [
        "local",
        "cartpole-dummy-2-episodes",
        "8830",
    ],
    data = glob(["examples/serving/*.py"]),
    tags = [
        "env",
        "team:rllib",
    ],
)

# Tests with unity 3d and external envs currently don't work
# see: https://github.com/ray-project/ray/issues/34290 for more details
# sh_test(
#     name = "env/tests/test_local_inference_unity3d",
#     tags = ["team:rllib", "env"],
#     size = "large", # bazel may complain about it being too long sometimes - large is on purpose as some frameworks take longer
#     srcs = ["env/tests/test_policy_client_server_setup.sh"],
#     args = ["local", "unity3d", "8850"],
#     data = glob(["examples/serving/*.py"]),
# )

py_test(
    name = "env/tests/test_multi_agent_env",
    size = "large",
    srcs = ["env/tests/test_multi_agent_env.py"],
    tags = [
        "team:rllib",
        "tests_dir",
    ],
)

py_test(
    name = "env/tests/test_multi_agent_env_runner",
    size = "medium",
    srcs = ["env/tests/test_multi_agent_env_runner.py"],
    tags = [
        "env",
        "team:rllib",
    ],
)

py_test(
    name = "env/tests/test_multi_agent_episode",
    size = "medium",
    srcs = ["env/tests/test_multi_agent_episode.py"],
    tags = [
        "env",
        "team:rllib",
    ],
)

sh_test(
    name = "env/tests/test_remote_inference_cartpole",
    size = "large",  # bazel may complain about it being too long sometimes - large is on purpose as some frameworks take longer
    srcs = ["env/tests/test_policy_client_server_setup.sh"],
    args = [
        "remote",
        "cartpole",
        "8810",
    ],
    data = glob(["examples/serving/*.py"]),
    tags = [
        "env",
        "exclusive",
        "team:rllib",
    ],
)

sh_test(
    name = "env/tests/test_remote_inference_cartpole_lstm",
    size = "large",  # bazel may complain about it being too long sometimes - large is on purpose as some frameworks take longer
    srcs = ["env/tests/test_policy_client_server_setup.sh"],
    args = [
        "remote",
        "cartpole_lstm",
        "8820",
    ],
    data = glob(["examples/serving/*.py"]),
    tags = [
        "env",
        "exclusive",
        "team:rllib",
    ],
)

sh_test(
    name = "env/tests/test_remote_inference_cartpole_w_2_concurrent_episodes",
    size = "large",  # bazel may complain about it being too long sometimes - large is on purpose as some frameworks take longer
    srcs = ["env/tests/test_policy_client_server_setup.sh"],
    args = [
        "remote",
        "cartpole-dummy-2-episodes",
        "8840",
    ],
    data = glob(["examples/serving/*.py"]),
    tags = [
        "env",
        "exclusive",
        "team:rllib",
    ],
)

# Tests with unity 3d and external envs currently don't work
# see: https://github.com/ray-project/ray/issues/34290 for more details
# sh_test(
#     name = "env/tests/test_remote_inference_unity3d",
#     tags = ["team:rllib", "env", "exclusive"],
#     size = "small",
#     srcs = ["env/tests/test_policy_client_server_setup.sh"],
#     args = ["remote", "unity3d", "8860"],
#     data = glob(["examples/serving/*.py"]),
# )

py_test(
    name = "env/tests/test_single_agent_env_runner",
    size = "medium",
    srcs = ["env/tests/test_single_agent_env_runner.py"],
    tags = [
        "env",
        "team:rllib",
    ],
)

py_test(
    name = "env/tests/test_single_agent_episode",
    size = "small",
    srcs = ["env/tests/test_single_agent_episode.py"],
    tags = [
        "env",
        "team:rllib",
    ],
)

py_test(
    name = "env/wrappers/tests/test_exception_wrapper",
    size = "small",
    srcs = ["env/wrappers/tests/test_exception_wrapper.py"],
    tags = [
        "env",
        "team:rllib",
    ],
)

py_test(
    name = "env/wrappers/tests/test_group_agents_wrapper",
    size = "small",
    srcs = ["env/wrappers/tests/test_group_agents_wrapper.py"],
    tags = [
        "env",
        "team:rllib",
    ],
)

py_test(
    name = "env/wrappers/tests/test_recsim_wrapper",
    size = "small",
    srcs = ["env/wrappers/tests/test_recsim_wrapper.py"],
    tags = [
        "env",
        "team:rllib",
    ],
)

py_test(
    name = "env/wrappers/tests/test_unity3d_env",
    size = "small",
    srcs = ["env/wrappers/tests/test_unity3d_env.py"],
    tags = [
        "env",
        "team:rllib",
    ],
)

# --------------------------------------------------------------------
# Evaluation components
# rllib/evaluation/
#
# Tag: evaluation
# --------------------------------------------------------------------
py_test(
    name = "evaluation/tests/test_agent_collector",
    size = "small",
    srcs = ["evaluation/tests/test_agent_collector.py"],
    tags = [
        "evaluation",
        "team:rllib",
    ],
)

py_test(
    name = "evaluation/tests/test_envs_that_crash",
    size = "large",
    srcs = ["evaluation/tests/test_envs_that_crash.py"],
    tags = [
        "evaluation",
        "team:rllib",
    ],
)

py_test(
    name = "evaluation/tests/test_episode",
    size = "small",
    srcs = ["evaluation/tests/test_episode.py"],
    tags = [
        "evaluation",
        "team:rllib",
    ],
)

py_test(
    name = "evaluation/tests/test_env_runner_v2",
    size = "small",
    srcs = ["evaluation/tests/test_env_runner_v2.py"],
    tags = [
        "evaluation",
        "team:rllib",
    ],
)

py_test(
    name = "evaluation/tests/test_episode_v2",
    size = "small",
    srcs = ["evaluation/tests/test_episode_v2.py"],
    tags = [
        "evaluation",
        "team:rllib",
    ],
)

py_test(
    name = "evaluation/tests/test_postprocessing",
    size = "small",
    srcs = ["evaluation/tests/test_postprocessing.py"],
    tags = [
        "evaluation",
        "team:rllib",
    ],
)

py_test(
    name = "evaluation/tests/test_worker_set",
    size = "small",
    srcs = ["evaluation/tests/test_worker_set.py"],
    tags = [
        "evaluation",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "evaluation/tests/test_rollout_worker",
    size = "large",
    srcs = ["evaluation/tests/test_rollout_worker.py"],
    tags = [
        "evaluation",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "evaluation/tests/test_trajectory_view_api",
    size = "large",
    srcs = ["evaluation/tests/test_trajectory_view_api.py"],
    tags = [
        "evaluation",
        "team:rllib",
    ],
)

# --------------------------------------------------------------------
# RLlib core
# rllib/core/
#
# Tag: core
# --------------------------------------------------------------------

# Catalog
py_test(
    name = "test_catalog",
    size = "medium",
    srcs = ["core/models/tests/test_catalog.py"],
    tags = [
        "core",
        "team:rllib",
    ],
)

# Default Models
py_test(
    name = "test_base_models",
    size = "small",
    srcs = ["core/models/tests/test_base_models.py"],
    tags = [
        "core",
        "team:rllib",
    ],
)

py_test(
    name = "test_cnn_encoders",
    size = "large",
    srcs = ["core/models/tests/test_cnn_encoders.py"],
    tags = [
        "core",
        "models",
        "team:rllib",
    ],
)

py_test(
    name = "test_cnn_transpose_heads",
    size = "medium",
    srcs = ["core/models/tests/test_cnn_transpose_heads.py"],
    tags = [
        "core",
        "models",
        "team:rllib",
    ],
)

py_test(
    name = "test_mlp_encoders",
    size = "medium",
    srcs = ["core/models/tests/test_mlp_encoders.py"],
    tags = [
        "core",
        "models",
        "team:rllib",
    ],
)

py_test(
    name = "test_mlp_heads",
    size = "medium",
    srcs = ["core/models/tests/test_mlp_heads.py"],
    tags = [
        "core",
        "models",
        "team:rllib",
    ],
)

py_test(
    name = "test_recurrent_encoders",
    size = "medium",
    srcs = ["core/models/tests/test_recurrent_encoders.py"],
    tags = [
        "core",
        "models",
        "team:rllib",
    ],
)

# Specs
py_test(
    name = "test_check_specs",
    size = "small",
    srcs = ["core/models/specs/tests/test_check_specs.py"],
    tags = [
        "models",
        "team:rllib",
    ],
)

py_test(
    name = "test_tensor_spec",
    size = "small",
    srcs = ["core/models/specs/tests/test_tensor_spec.py"],
    tags = [
        "models",
        "team:rllib",
    ],
)

py_test(
    name = "test_spec_dict",
    size = "small",
    srcs = ["core/models/specs/tests/test_spec_dict.py"],
    tags = [
        "models",
        "team:rllib",
    ],
)

# RLModule
py_test(
    name = "test_torch_rl_module",
    size = "medium",
    srcs = ["core/rl_module/torch/tests/test_torch_rl_module.py"],
    args = ["TestRLModule"],
    tags = [
        "core",
        "team:rllib",
    ],
)

# TODO(Artur): Comment this back in as soon as we can test with GPU
# py_test(
#    name = "test_torch_rl_module_gpu",
#    main = "core/rl_module/torch/tests/test_torch_rl_module.py",
#    tags = ["team:rllib", "core", "gpu", "exclusive"],
#    size = "medium",
#    srcs = ["core/rl_module/torch/tests/test_torch_rl_module.py"],
#    args = ["TestRLModuleGPU"],
# )

py_test(
    name = "test_tf_rl_module",
    size = "medium",
    srcs = ["core/rl_module/tf/tests/test_tf_rl_module.py"],
    tags = [
        "core",
        "team:rllib",
    ],
)

py_test(
    name = "test_marl_module",
    size = "medium",
    srcs = ["core/rl_module/tests/test_marl_module.py"],
    tags = [
        "core",
        "team:rllib",
    ],
)

py_test(
    name = "test_rl_module_specs",
    size = "medium",
    srcs = ["core/rl_module/tests/test_rl_module_specs.py"],
    tags = [
        "core",
        "team:rllib",
    ],
)

# Learner
py_test(
    name = "TestLearnerGroupSyncUpdate",
    size = "large",
    srcs = ["core/learner/tests/test_learner_group.py"],
    args = ["TestLearnerGroupSyncUpdate"],
    main = "core/learner/tests/test_learner_group.py",
    tags = [
        "exclusive",
        "multi_gpu",
        "team:rllib",
    ],
)

py_test(
    name = "TestLearnerGroupCheckpointRestore",
    size = "large",
    srcs = ["core/learner/tests/test_learner_group.py"],
    args = ["TestLearnerGroupCheckpointRestore"],
    main = "core/learner/tests/test_learner_group.py",
    tags = [
        "exclusive",
        "multi_gpu",
        "team:rllib",
    ],
)

py_test(
    name = "TestLearnerGroupAsyncUpdate",
    size = "large",
    srcs = ["core/learner/tests/test_learner_group.py"],
    args = ["TestLearnerGroupAsyncUpdate"],
    main = "core/learner/tests/test_learner_group.py",
    tags = [
        "exclusive",
        "multi_gpu",
        "team:rllib",
    ],
)

py_test(
    name = "TestLearnerGroupSaveLoadState",
    size = "large",
    srcs = ["core/learner/tests/test_learner_group.py"],
    args = ["TestLearnerGroupSaveLoadState"],
    main = "core/learner/tests/test_learner_group.py",
    tags = [
        "exclusive",
        "multi_gpu",
        "team:rllib",
    ],
)

py_test(
    name = "test_learner",
    size = "medium",
    srcs = ["core/learner/tests/test_learner.py"],
    tags = [
        "core",
        "ray_data",
        "team:rllib",
    ],
)

py_test(
    name = "test_torch_learner_compile",
    size = "medium",
    srcs = ["core/learner/torch/tests/test_torch_learner_compile.py"],
    tags = [
        "core",
        "ray_data",
        "team:rllib",
    ],
)

py_test(
    name = "tests/test_algorithm_save_load_checkpoint_learner",
    size = "medium",
    srcs = ["tests/test_algorithm_save_load_checkpoint_learner.py"],
    tags = [
        "core",
        "team:rllib",
    ],
)

py_test(
    name = "test_algorithm_rl_module_restore",
    size = "large",
    srcs = ["tests/test_algorithm_rl_module_restore.py"],
    tags = [
        "core",
        "team:rllib",
    ],
)

py_test(
    name = "test_bc_algorithm",
    size = "medium",
    srcs = ["core/testing/tests/test_bc_algorithm.py"],
    tags = [
        "core",
        "team:rllib",
    ],
)

# --------------------------------------------------------------------
# Models and Distributions
# rllib/models/
#
# Tag: models
# --------------------------------------------------------------------

py_test(
    name = "test_attention_nets",
    size = "large",
    srcs = ["models/tests/test_attention_nets.py"],
    tags = [
        "models",
        "team:rllib",
    ],
)

py_test(
    name = "test_conv2d_default_stacks",
    size = "small",
    srcs = ["models/tests/test_conv2d_default_stacks.py"],
    tags = [
        "models",
        "team:rllib",
    ],
)

py_test(
    name = "test_convtranspose2d_stack",
    size = "medium",
    srcs = ["models/tests/test_convtranspose2d_stack.py"],
    data = glob(["tests/data/images/obstacle_tower.png"]),
    tags = [
        "models",
        "team:rllib",
    ],
)

py_test(
    name = "test_action_distributions",
    size = "medium",
    srcs = ["models/tests/test_action_distributions.py"],
    tags = [
        "models",
        "team:rllib",
    ],
)

py_test(
    name = "test_distributions",
    size = "small",
    srcs = ["models/tests/test_distributions.py"],
    tags = [
        "models",
        "team:rllib",
    ],
)

py_test(
    name = "test_lstms",
    size = "large",
    srcs = ["models/tests/test_lstms.py"],
    tags = [
        "models",
        "team:rllib",
    ],
)

py_test(
    name = "test_models",
    size = "medium",
    srcs = ["models/tests/test_models.py"],
    tags = [
        "models",
        "team:rllib",
    ],
)

py_test(
    name = "test_preprocessors",
    size = "medium",
    srcs = ["models/tests/test_preprocessors.py"],
    tags = [
        "models",
        "team:rllib",
    ],
)

# --------------------------------------------------------------------
# Offline
# rllib/offline/
#
# Tag: offline
# --------------------------------------------------------------------

py_test(
    name = "test_dataset_reader",
    size = "small",
    srcs = ["offline/tests/test_dataset_reader.py"],
    data = [
        "tests/data/pendulum/enormous.zip",
        "tests/data/pendulum/large.json",
    ],
    tags = [
        "offline",
        "team:rllib",
    ],
)

py_test(
    name = "test_feature_importance",
    size = "medium",
    srcs = ["offline/tests/test_feature_importance.py"],
    tags = [
        "offline",
        "team:rllib",
        "torch_only",
    ],
)

py_test(
    name = "test_json_reader",
    size = "small",
    srcs = ["offline/tests/test_json_reader.py"],
    data = ["tests/data/pendulum/large.json"],
    tags = [
        "offline",
        "team:rllib",
    ],
)

py_test(
    name = "test_ope",
    size = "medium",
    srcs = ["offline/estimators/tests/test_ope.py"],
    data = ["tests/data/cartpole/small.json"],
    tags = [
        "offline",
        "ray_data",
        "team:rllib",
    ],
)

py_test(
    name = "test_ope_math",
    size = "small",
    srcs = ["offline/estimators/tests/test_ope_math.py"],
    tags = [
        "offline",
        "team:rllib",
    ],
)

py_test(
    name = "test_dm_learning",
    size = "large",
    srcs = ["offline/estimators/tests/test_dm_learning.py"],
    tags = [
        "offline",
        "team:rllib",
    ],
)

py_test(
    name = "test_dr_learning",
    size = "large",
    srcs = ["offline/estimators/tests/test_dr_learning.py"],
    tags = [
        "offline",
        "team:rllib",
    ],
)

# --------------------------------------------------------------------
# Policies
# rllib/policy/
#
# Tag: policy
# --------------------------------------------------------------------

py_test(
    name = "policy/tests/test_compute_log_likelihoods",
    size = "medium",
    srcs = ["policy/tests/test_compute_log_likelihoods.py"],
    tags = [
        "policy",
        "team:rllib",
    ],
)

py_test(
    name = "policy/tests/test_export_checkpoint_and_model",
    size = "large",
    srcs = ["policy/tests/test_export_checkpoint_and_model.py"],
    tags = [
        "policy",
        "team:rllib",
    ],
)

py_test(
    name = "policy/tests/test_multi_agent_batch",
    size = "small",
    srcs = ["policy/tests/test_multi_agent_batch.py"],
    tags = [
        "policy",
        "team:rllib",
    ],
)

py_test(
    name = "policy/tests/test_policy",
    size = "medium",
    srcs = ["policy/tests/test_policy.py"],
    tags = [
        "policy",
        "team:rllib",
    ],
)

py_test(
    name = "policy/tests/test_policy_map",
    size = "medium",
    srcs = ["policy/tests/test_policy_map.py"],
    tags = [
        "policy",
        "team:rllib",
    ],
)

py_test(
    name = "policy/tests/test_policy_state_swapping",
    size = "medium",
    srcs = ["policy/tests/test_policy_state_swapping.py"],
    tags = [
        "gpu",
        "policy",
        "team:rllib",
    ],
)

py_test(
    name = "policy/tests/test_rnn_sequencing",
    size = "small",
    srcs = ["policy/tests/test_rnn_sequencing.py"],
    tags = [
        "policy",
        "team:rllib",
    ],
)

py_test(
    name = "policy/tests/test_sample_batch",
    size = "small",
    srcs = ["policy/tests/test_sample_batch.py"],
    tags = [
        "multi_gpu",
        "policy",
        "team:rllib",
    ],
)

py_test(
    name = "policy/tests/test_view_requirement",
    size = "small",
    srcs = ["policy/tests/test_view_requirement.py"],
    tags = [
        "policy",
        "team:rllib",
    ],
)

# --------------------------------------------------------------------
# Utils:
# rllib/utils/
#
# Tag: utils
# --------------------------------------------------------------------

py_test(
    name = "test_checkpoint_utils",
    size = "small",
    srcs = ["utils/tests/test_checkpoint_utils.py"],
    tags = [
        "team:rllib",
        "utils",
    ],
)

py_test(
    name = "test_errors",
    size = "medium",
    srcs = ["utils/tests/test_errors.py"],
    tags = [
        "team:rllib",
        "utils",
    ],
)

py_test(
    name = "test_minibatch_utils",
    size = "small",
    srcs = ["utils/tests/test_minibatch_utils.py"],
    tags = [
        "team:rllib",
        "utils",
    ],
)

py_test(
    name = "test_nested_dict",
    size = "small",
    srcs = ["utils/tests/test_nested_dict.py"],
    tags = [
        "team:rllib",
        "utils",
    ],
)

py_test(
    name = "test_serialization",
    size = "small",
    srcs = ["utils/tests/test_serialization.py"],
    tags = [
        "team:rllib",
        "utils",
    ],
)

py_test(
    name = "test_curiosity",
    size = "large",
    srcs = ["utils/exploration/tests/test_curiosity.py"],
    tags = [
        "team:rllib",
        "utils",
    ],
)

py_test(
    name = "test_explorations",
    size = "large",
    srcs = ["utils/exploration/tests/test_explorations.py"],
    tags = [
        "team:rllib",
        "utils",
    ],
)

py_test(
    name = "test_value_predictions",
    size = "small",
    srcs = ["utils/postprocessing/tests/test_value_predictions.py"],
    tags = [
        "team:rllib",
        "utils",
    ],
)

py_test(
    name = "test_random_encoder",
    size = "large",
    srcs = ["utils/exploration/tests/test_random_encoder.py"],
    tags = [
        "team:rllib",
        "utils",
    ],
)

py_test(
    name = "test_torch_utils",
    size = "medium",
    srcs = ["utils/tests/test_torch_utils.py"],
    tags = [
        "gpu",
        "team:rllib",
        "utils",
    ],
)

# Schedules
py_test(
    name = "test_schedules",
    size = "small",
    srcs = ["utils/schedules/tests/test_schedules.py"],
    tags = [
        "team:rllib",
        "utils",
    ],
)

py_test(
    name = "test_framework_agnostic_components",
    size = "small",
    srcs = ["utils/tests/test_framework_agnostic_components.py"],
    data = glob(["utils/tests/**"]),
    tags = [
        "team:rllib",
        "utils",
    ],
)

# Spaces/Space utils.
py_test(
    name = "test_space_utils",
    size = "small",
    srcs = ["utils/spaces/tests/test_space_utils.py"],
    tags = [
        "team:rllib",
        "utils",
    ],
)

# TaskPool
py_test(
    name = "test_taskpool",
    size = "small",
    srcs = ["utils/tests/test_taskpool.py"],
    tags = [
        "team:rllib",
        "utils",
    ],
)

# ReplayBuffers
py_test(
    name = "test_episode_replay_buffer",
    size = "small",
    srcs = ["utils/replay_buffers/tests/test_episode_replay_buffer.py"],
    tags = [
        "team:rllib",
        "utils",
    ],
)

py_test(
    name = "test_multi_agent_mixin_replay_buffer",
    size = "small",
    srcs = ["utils/replay_buffers/tests/test_multi_agent_mixin_replay_buffer.py"],
    tags = [
        "team:rllib",
        "utils",
    ],
)

py_test(
    name = "test_multi_agent_prioritized_replay_buffer",
    size = "small",
    srcs = ["utils/replay_buffers/tests/test_multi_agent_prioritized_replay_buffer.py"],
    tags = [
        "team:rllib",
        "utils",
    ],
)

py_test(
    name = "test_multi_agent_replay_buffer",
    size = "small",
    srcs = ["utils/replay_buffers/tests/test_multi_agent_replay_buffer.py"],
    tags = [
        "team:rllib",
        "utils",
    ],
)

py_test(
    name = "test_prioritized_episode_replay_buffer",
    size = "small",
    srcs = ["utils/replay_buffers/tests/test_prioritized_episode_replay_buffer.py"],
    tags = [
        "team::rllib",
        "utils",
    ],
)

py_test(
    name = "test_prioritized_replay_buffer_replay_buffer_api",
    size = "small",
    srcs = ["utils/replay_buffers/tests/test_prioritized_replay_buffer_replay_buffer_api.py"],
    tags = [
        "team:rllib",
        "utils",
    ],
)

py_test(
    name = "test_replay_buffer",
    size = "small",
    srcs = ["utils/replay_buffers/tests/test_replay_buffer.py"],
    tags = [
        "team:rllib",
        "utils",
    ],
)

py_test(
    name = "test_fifo_replay_buffer",
    size = "small",
    srcs = ["utils/replay_buffers/tests/test_fifo_replay_buffer.py"],
    tags = [
        "team:rllib",
        "utils",
    ],
)

py_test(
    name = "test_reservoir_buffer",
    size = "small",
    srcs = ["utils/replay_buffers/tests/test_reservoir_buffer.py"],
    tags = [
        "team:rllib",
        "utils",
    ],
)

py_test(
    name = "test_segment_tree_replay_buffer_api",
    size = "small",
    srcs = ["utils/replay_buffers/tests/test_segment_tree_replay_buffer_api.py"],
    tags = [
        "team:rllib",
        "utils",
    ],
)

py_test(
    name = "test_check_env",
    size = "small",
    srcs = ["utils/tests/test_check_env.py"],
    tags = [
        "team:rllib",
        "utils",
    ],
)

py_test(
    name = "test_check_multi_agent",
    size = "small",
    srcs = ["utils/tests/test_check_multi_agent.py"],
    tags = [
        "team:rllib",
        "utils",
    ],
)

py_test(
    name = "test_actor_manager",
    size = "medium",
    srcs = ["utils/tests/test_actor_manager.py"],
    data = ["utils/tests/random_numbers.pkl"],
    tags = [
        "exclusive",
        "team:rllib",
        "utils",
    ],
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
    size = "medium",
    srcs = ["tests/backward_compat/test_backward_compat.py"],
    data = glob(["tests/backward_compat/checkpoints/**"]),
    tags = [
        "team:rllib",
        "tests_dir",
    ],
)

py_test(
    name = "tests/backward_compat/test_gym_env_apis",
    size = "large",
    srcs = ["tests/backward_compat/test_gym_env_apis.py"],
    tags = [
        "env",
        "team:rllib",
    ],
)

py_test(
    name = "tests/test_algorithm_imports",
    size = "small",
    srcs = ["tests/test_algorithm_imports.py"],
    tags = [
        "team:rllib",
        "tests_dir",
    ],
)

py_test(
    name = "tests/test_catalog",
    size = "medium",
    srcs = ["tests/test_catalog.py"],
    tags = [
        "team:rllib",
        "tests_dir",
    ],
)

py_test(
    name = "tests/test_checkpoint_restore_ppo",
    size = "large",
    srcs = ["tests/test_algorithm_checkpoint_restore.py"],
    args = ["TestCheckpointRestorePPO"],
    main = "tests/test_algorithm_checkpoint_restore.py",
    tags = [
        "team:rllib",
        "tests_dir",
    ],
)

py_test(
    name = "tests/test_checkpoint_restore_ppo_gpu",
    size = "large",
    srcs = ["tests/test_algorithm_checkpoint_restore.py"],
    args = ["TestCheckpointRestorePPO"],
    main = "tests/test_algorithm_checkpoint_restore.py",
    tags = [
        "gpu",
        "team:rllib",
        "tests_dir",
    ],
)

py_test(
    name = "tests/test_checkpoint_restore_off_policy",
    size = "large",
    srcs = ["tests/test_algorithm_checkpoint_restore.py"],
    args = ["TestCheckpointRestoreOffPolicy"],
    main = "tests/test_algorithm_checkpoint_restore.py",
    tags = [
        "team:rllib",
        "tests_dir",
    ],
)

py_test(
    name = "tests/test_checkpoint_restore_off_policy_gpu",
    size = "large",
    srcs = ["tests/test_algorithm_checkpoint_restore.py"],
    args = ["TestCheckpointRestoreOffPolicy"],
    main = "tests/test_algorithm_checkpoint_restore.py",
    tags = [
        "gpu",
        "team:rllib",
        "tests_dir",
    ],
)

py_test(
    name = "policy/tests/test_policy_checkpoint_restore",
    size = "large",
    srcs = ["policy/tests/test_policy_checkpoint_restore.py"],
    data = glob([
        "tests/data/checkpoints/APPO_CartPole-v1-connector-enabled/**",
    ]),
    main = "policy/tests/test_policy_checkpoint_restore.py",
    tags = [
        "team:rllib",
        "tests_dir",
    ],
)

py_test(
    name = "tests/test_custom_resource",
    size = "large",  # bazel may complain about it being too long sometimes - large is on purpose as some frameworks take longer
    srcs = ["tests/test_custom_resource.py"],
    tags = [
        "team:rllib",
        "tests_dir",
    ],
)

py_test(
    name = "tests/test_dependency_tf",
    size = "small",
    srcs = ["tests/test_dependency_tf.py"],
    tags = [
        "team:rllib",
        "tests_dir",
    ],
)

py_test(
    name = "tests/test_dependency_torch",
    size = "small",
    srcs = ["tests/test_dependency_torch.py"],
    tags = [
        "team:rllib",
        "tests_dir",
    ],
)

py_test(
    name = "tests/test_eager_support_policy_gradient",
    size = "small",
    srcs = ["tests/test_eager_support.py"],
    args = ["TestEagerSupportPolicyGradient"],
    main = "tests/test_eager_support.py",
    tags = [
        "team:rllib",
        "tests_dir",
    ],
)

py_test(
    name = "tests/test_eager_support_off_policy",
    size = "small",
    srcs = ["tests/test_eager_support.py"],
    args = ["TestEagerSupportOffPolicy"],
    main = "tests/test_eager_support.py",
    tags = [
        "team:rllib",
        "tests_dir",
    ],
)

py_test(
    name = "tests/test_filters",
    size = "small",
    srcs = ["tests/test_filters.py"],
    tags = [
        "team:rllib",
        "tests_dir",
    ],
)

py_test(
    name = "tests/test_gpus",
    size = "large",
    srcs = ["tests/test_gpus.py"],
    tags = [
        "team:rllib",
        "tests_dir",
    ],
)

py_test(
    name = "tests/test_io",
    size = "large",
    srcs = ["tests/test_io.py"],
    tags = [
        "team:rllib",
        "tests_dir",
    ],
)

py_test(
    name = "tests/test_local",
    size = "small",
    srcs = ["tests/test_local.py"],
    tags = [
        "team:rllib",
        "tests_dir",
    ],
)

py_test(
    name = "tests/test_lstm",
    size = "medium",
    srcs = ["tests/test_lstm.py"],
    tags = [
        "team:rllib",
        "tests_dir",
    ],
)

py_test(
    name = "tests/test_model_imports",
    size = "medium",
    srcs = ["tests/test_model_imports.py"],
    data = glob(["tests/data/model_weights/**"]),
    tags = [
        "model_imports",
        "team:rllib",
        "tests_dir",
    ],
)

py_test(
    name = "tests/test_nested_action_spaces",
    size = "large",
    srcs = ["tests/test_nested_action_spaces.py"],
    main = "tests/test_nested_action_spaces.py",
    tags = [
        "team:rllib",
        "tests_dir",
    ],
)

py_test(
    name = "tests/test_nested_observation_spaces",
    size = "medium",
    srcs = ["tests/test_nested_observation_spaces.py"],
    main = "tests/test_nested_observation_spaces.py",
    tags = [
        "team:rllib",
        "tests_dir",
    ],
)

py_test(
    name = "tests/test_nn_framework_import_errors",
    size = "small",
    srcs = ["tests/test_nn_framework_import_errors.py"],
    tags = [
        "team:rllib",
        "tests_dir",
    ],
)

py_test(
    name = "tests/test_pettingzoo_env",
    size = "medium",
    srcs = ["tests/test_pettingzoo_env.py"],
    tags = [
        "team:rllib",
        "tests_dir",
    ],
)

py_test(
    name = "tests/test_placement_groups",
    size = "large",  # bazel may complain about it being too long sometimes - large is on purpose as some frameworks take longer
    srcs = ["tests/test_placement_groups.py"],
    tags = [
        "team:rllib",
        "tests_dir",
    ],
)

py_test(
    name = "tests/test_ray_client",
    size = "large",
    srcs = ["tests/test_ray_client.py"],
    tags = [
        "team:rllib",
        "tests_dir",
    ],
)

py_test(
    name = "tests/test_reproducibility",
    size = "medium",
    srcs = ["tests/test_reproducibility.py"],
    tags = [
        "team:rllib",
        "tests_dir",
    ],
)

# Test [train|evaluate].py scripts (w/o confirming evaluation performance).
py_test(
    name = "test_rllib_evaluate_1",
    size = "large",
    srcs = ["tests/test_rllib_train_and_evaluate.py"],
    args = ["TestEvaluate1"],
    data = [
        "evaluate.py",
        "train.py",
    ],
    main = "tests/test_rllib_train_and_evaluate.py",
    tags = [
        "team:rllib",
        "tests_dir",
    ],
)

py_test(
    name = "test_rllib_evaluate_2",
    size = "large",
    srcs = ["tests/test_rllib_train_and_evaluate.py"],
    args = ["TestEvaluate2"],
    data = [
        "evaluate.py",
        "train.py",
    ],
    main = "tests/test_rllib_train_and_evaluate.py",
    tags = [
        "team:rllib",
        "tests_dir",
    ],
)

py_test(
    name = "test_rllib_evaluate_3",
    size = "large",
    srcs = ["tests/test_rllib_train_and_evaluate.py"],
    args = ["TestEvaluate3"],
    data = [
        "evaluate.py",
        "train.py",
    ],
    main = "tests/test_rllib_train_and_evaluate.py",
    tags = [
        "team:rllib",
        "tests_dir",
    ],
)

# Test [train|evaluate].py scripts (and confirm `rllib evaluate` performance is same
# as the final one from the `rllib train` run).
py_test(
    name = "test_rllib_train_and_evaluate",
    size = "large",
    srcs = ["tests/test_rllib_train_and_evaluate.py"],
    args = ["TestTrainAndEvaluate"],
    data = [
        "evaluate.py",
        "train.py",
    ],
    main = "tests/test_rllib_train_and_evaluate.py",
    tags = [
        "team:rllib",
        "tests_dir",
    ],
)

py_test(
    name = "tests/test_supported_multi_agent_multi_gpu",
    size = "medium",
    srcs = ["tests/test_supported_multi_agent.py"],
    args = ["TestSupportedMultiAgentMultiGPU"],
    main = "tests/test_supported_multi_agent.py",
    tags = [
        "multi_gpu",
        "team:rllib",
        "tests_dir",
    ],
)

py_test(
    name = "tests/test_supported_multi_agent_policy_gradient",
    size = "large",
    srcs = ["tests/test_supported_multi_agent.py"],
    args = ["TestSupportedMultiAgentPolicyGradient"],
    main = "tests/test_supported_multi_agent.py",
    tags = [
        "team:rllib",
        "tests_dir",
    ],
)

py_test(
    name = "tests/test_supported_multi_agent_off_policy",
    size = "large",
    srcs = ["tests/test_supported_multi_agent.py"],
    args = ["TestSupportedMultiAgentOffPolicy"],
    main = "tests/test_supported_multi_agent.py",
    tags = [
        "team:rllib",
        "tests_dir",
    ],
)

py_test(
    name = "tests/test_supported_spaces_appo",
    size = "large",
    srcs = ["tests/test_supported_spaces.py"],
    args = ["TestSupportedSpacesAPPO"],
    main = "tests/test_supported_spaces.py",
    tags = [
        "exclusive",
        "team:rllib",
        "tests_dir",
    ],
)

py_test(
    name = "tests/test_supported_spaces_impala",
    size = "large",
    srcs = ["tests/test_supported_spaces.py"],
    args = ["TestSupportedSpacesIMPALA"],
    main = "tests/test_supported_spaces.py",
    tags = [
        "exclusive",
        "team:rllib",
        "tests_dir",
    ],
)

py_test(
    name = "tests/test_supported_spaces_ppo",
    size = "large",
    srcs = ["tests/test_supported_spaces.py"],
    args = ["TestSupportedSpacesPPO"],
    main = "tests/test_supported_spaces.py",
    tags = [
        "team:rllib",
        "tests_dir",
    ],
)

py_test(
    name = "tests/test_supported_spaces_dqn",
    size = "large",
    srcs = ["tests/test_supported_spaces.py"],
    args = ["TestSupportedSpacesDQN"],
    main = "tests/test_supported_spaces.py",
    tags = [
        "team:rllib",
        "tests_dir",
    ],
)

py_test(
    name = "tests/test_supported_spaces_ppo_no_preprocessor_gpu",
    size = "large",
    srcs = ["tests/test_supported_spaces.py"],
    args = ["TestSupportedSpacesPPONoPreprocessorGPU"],
    main = "tests/test_supported_spaces.py",
    tags = [
        "gpu",
        "no_cpu",
        "team:rllib",
    ],
)

py_test(
    name = "tests/test_supported_spaces_off_policy",
    size = "medium",
    srcs = ["tests/test_supported_spaces.py"],
    args = ["TestSupportedSpacesOffPolicy"],
    main = "tests/test_supported_spaces.py",
    tags = [
        "exclusive",
        "team:rllib",
        "tests_dir",
    ],
)

py_test(
    name = "tests/test_timesteps",
    size = "small",
    srcs = ["tests/test_timesteps.py"],
    tags = [
        "team:rllib",
        "tests_dir",
    ],
)

# --------------------------------------------------------------------
# examples/ directory (excluding examples/documentation/...)
#
# Tag: examples
#
# NOTE: Add tests alphabetically into this list.
# --------------------------------------------------------------------

py_test(
    name = "examples/action_masking_tf2",
    size = "small",
    srcs = ["examples/action_masking.py"],
    args = [
        "--stop-iter=2",
        "--framework=tf2",
    ],
    main = "examples/action_masking.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/action_masking_torch",
    size = "small",
    srcs = ["examples/action_masking.py"],
    args = [
        "--stop-iter=2",
        "--framework=torch",
    ],
    main = "examples/action_masking.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/autoregressive_action_dist_tf",
    size = "medium",
    srcs = ["examples/autoregressive_action_dist.py"],
    args = [
        "--as-test",
        "--framework=tf",
        "--stop-reward=150",
        "--num-cpus=4",
    ],
    main = "examples/autoregressive_action_dist.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/autoregressive_action_dist_torch",
    size = "medium",
    srcs = ["examples/autoregressive_action_dist.py"],
    args = [
        "--as-test",
        "--framework=torch",
        "--stop-reward=150",
        "--num-cpus=4",
    ],
    main = "examples/autoregressive_action_dist.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/cartpole_lstm_impala_tf2",
    size = "medium",
    srcs = ["examples/cartpole_lstm.py"],
    args = [
        "--run=IMPALA",
        "--as-test",
        "--framework=tf2",
        "--stop-reward=28",
        "--num-cpus=4",
    ],
    main = "examples/cartpole_lstm.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/cartpole_lstm_impala_torch",
    size = "medium",
    srcs = ["examples/cartpole_lstm.py"],
    args = [
        "--run=IMPALA",
        "--as-test",
        "--framework=torch",
        "--stop-reward=28",
        "--num-cpus=4",
    ],
    main = "examples/cartpole_lstm.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

# TODO (Kourosh): tf2 ~5x slower compared to torch on the new stack
py_test(
    name = "examples/cartpole_lstm_ppo_tf2",
    size = "large",
    srcs = ["examples/cartpole_lstm.py"],
    args = [
        "--run=PPO",
        "--as-test",
        "--framework=tf2",
        "--stop-reward=28",
        "--num-cpus=4",
    ],
    main = "examples/cartpole_lstm.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/cartpole_lstm_ppo_torch",
    size = "medium",
    srcs = ["examples/cartpole_lstm.py"],
    args = [
        "--run=PPO",
        "--as-test",
        "--framework=torch",
        "--stop-reward=28",
        "--num-cpus=4",
    ],
    main = "examples/cartpole_lstm.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/cartpole_lstm_ppo_torch_with_prev_a_and_r",
    size = "medium",
    srcs = ["examples/cartpole_lstm.py"],
    args = [
        "--run=PPO",
        "--as-test",
        "--framework=torch",
        "--stop-reward=28",
        "--num-cpus=4",
        "--use-prev-action",
        "--use-prev-reward",
    ],
    main = "examples/cartpole_lstm.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/catalog/custom_action_distribution",
    size = "small",
    srcs = ["examples/catalog/custom_action_distribution.py"],
    main = "examples/catalog/custom_action_distribution.py",
    tags = [
        "examples",
        "no_main",
        "team:rllib",
    ],
)

py_test(
    name = "examples/catalog/mobilenet_v2_encoder",
    size = "small",
    srcs = ["examples/catalog/mobilenet_v2_encoder.py"],
    main = "examples/catalog/mobilenet_v2_encoder.py",
    tags = [
        "examples",
        "no_main",
        "team:rllib",
    ],
)

py_test(
    name = "examples/rl_module/mobilenet_rlm",
    size = "small",
    srcs = ["examples/rl_module/mobilenet_rlm.py"],
    main = "examples/rl_module/mobilenet_rlm.py",
    tags = [
        "examples",
        "no_main",
        "team:rllib",
    ],
)

py_test(
    name = "examples/centralized_critic_tf",
    size = "medium",
    srcs = ["examples/centralized_critic.py"],
    args = [
        "--as-test",
        "--framework=tf",
        "--stop-reward=7.2",
    ],
    main = "examples/centralized_critic.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/centralized_critic_torch",
    size = "medium",
    srcs = ["examples/centralized_critic.py"],
    args = [
        "--as-test",
        "--framework=torch",
        "--stop-reward=7.2",
    ],
    main = "examples/centralized_critic.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/centralized_critic_2_tf",
    size = "medium",
    srcs = ["examples/centralized_critic_2.py"],
    args = [
        "--as-test",
        "--framework=tf",
        "--stop-reward=6.0",
    ],
    main = "examples/centralized_critic_2.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/centralized_critic_2_torch",
    size = "medium",
    srcs = ["examples/centralized_critic_2.py"],
    args = [
        "--as-test",
        "--framework=torch",
        "--stop-reward=6.0",
    ],
    main = "examples/centralized_critic_2.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/checkpoint_by_custom_criteria",
    size = "medium",
    srcs = ["examples/checkpoint_by_custom_criteria.py"],
    args = ["--stop-iters=3 --num-cpus=3"],
    main = "examples/checkpoint_by_custom_criteria.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/complex_struct_space_tf",
    size = "small",
    srcs = ["examples/complex_struct_space.py"],
    args = ["--framework=tf"],
    main = "examples/complex_struct_space.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/complex_struct_space_tf_eager",
    size = "small",
    srcs = ["examples/complex_struct_space.py"],
    args = ["--framework=tf2"],
    main = "examples/complex_struct_space.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/complex_struct_space_torch",
    size = "small",
    srcs = ["examples/complex_struct_space.py"],
    args = ["--framework=torch"],
    main = "examples/complex_struct_space.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/curriculum_learning",
    size = "medium",
    srcs = ["examples/curriculum_learning.py"],
    args = [
        "--as-test",
        "--stop-reward=800.0",
    ],
    main = "examples/curriculum_learning.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/custom_env_tf",
    size = "medium",
    srcs = ["examples/custom_env.py"],
    args = [
        "--as-test",
        "--framework=tf",
    ],
    main = "examples/custom_env.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/custom_env_torch",
    size = "medium",
    srcs = ["examples/custom_env.py"],
    args = [
        "--as-test",
        "--framework=torch",
    ],
    main = "examples/custom_env.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/custom_eval_tf",
    size = "small",
    srcs = ["examples/custom_eval.py"],
    args = [
        "--num-cpus=4",
        "--framework=tf",
        "--as-test",
    ],
    main = "examples/custom_eval.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/custom_eval_torch",
    size = "small",
    srcs = ["examples/custom_eval.py"],
    args = [
        "--num-cpus=4",
        "--as-test",
        "--framework=torch",
    ],
    main = "examples/custom_eval.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/custom_eval_parallel_to_training_torch",
    size = "small",
    srcs = ["examples/custom_eval.py"],
    args = [
        "--num-cpus=4",
        "--as-test",
        "--framework=torch",
        "--evaluation-parallel-to-training",
    ],
    main = "examples/custom_eval.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/custom_experiment",
    size = "medium",
    srcs = ["examples/custom_experiment.py"],
    args = ["--train-iterations=10"],
    main = "examples/custom_experiment.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/custom_metrics_and_callbacks",
    size = "small",
    srcs = ["examples/custom_metrics_and_callbacks.py"],
    args = ["--stop-iters=2"],
    main = "examples/custom_metrics_and_callbacks.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/custom_model_loss_and_metrics_ppo_tf",
    size = "small",
    srcs = ["examples/custom_model_loss_and_metrics.py"],
    args = [
        "--run=PPO",
        "--stop-iters=1",
        "--framework=tf",
        "--input-files=tests/data/cartpole",
    ],
    # Include the json data file.
    data = ["tests/data/cartpole/small.json"],
    main = "examples/custom_model_loss_and_metrics.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/custom_model_loss_and_metrics_ppo_torch",
    size = "small",
    srcs = ["examples/custom_model_loss_and_metrics.py"],
    args = [
        "--run=PPO",
        "--framework=torch",
        "--stop-iters=1",
        "--input-files=tests/data/cartpole",
    ],
    # Include the json data file.
    data = ["tests/data/cartpole/small.json"],
    main = "examples/custom_model_loss_and_metrics.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/custom_model_loss_and_metrics_pg_tf",
    size = "small",
    srcs = ["examples/custom_model_loss_and_metrics.py"],
    args = [
        "--stop-iters=1",
        "--framework=tf",
        "--input-files=tests/data/cartpole",
    ],
    # Include the json data file.
    data = ["tests/data/cartpole/small.json"],
    main = "examples/custom_model_loss_and_metrics.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/custom_model_loss_and_metrics_pg_torch",
    size = "small",
    srcs = ["examples/custom_model_loss_and_metrics.py"],
    args = [
        "--framework=torch",
        "--stop-iters=1",
        "--input-files=tests/data/cartpole",
    ],
    # Include the json data file.
    data = ["tests/data/cartpole/small.json"],
    main = "examples/custom_model_loss_and_metrics.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/custom_recurrent_rnn_tokenizer_repeat_after_me_tf2",
    size = "medium",
    srcs = ["examples/custom_recurrent_rnn_tokenizer.py"],
    args = [
        "--as-test",
        "--framework=tf2",
        "--stop-reward=40",
        "--env=RepeatAfterMeEnv",
        "--num-cpus=4",
    ],
    main = "examples/custom_recurrent_rnn_tokenizer.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/custom_recurrent_rnn_tokenizer_repeat_initial_obs_env_tf2",
    size = "medium",
    srcs = ["examples/custom_recurrent_rnn_tokenizer.py"],
    args = [
        "--as-test",
        "--framework=tf2",
        "--stop-reward=10",
        "--stop-timesteps=300000",
        "--env=RepeatInitialObsEnv",
        "--num-cpus=4",
    ],
    main = "examples/custom_recurrent_rnn_tokenizer.py",
    tags = [
        "examples",
        "team:rllib",
    ],
)

py_test(
    name = "examples/custom_recurrent_rnn_tokenizer_repeat_after_me_torch",
    size = "medium",
    srcs = ["examples/custom_recurrent_rnn_tokenizer.py"],
    args = [
        "--as-test",
        "--framework=torch",
        "--stop-reward=40",
        "--env=RepeatAfterMeEnv",
        "--num-cpus=4",
    ],
    main = "examples/custom_recurrent_rnn_tokenizer.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/custom_recurrent_rnn_tokenizer_repeat_initial_obs_env_torch",
    size = "medium",
    srcs = ["examples/custom_recurrent_rnn_tokenizer.py"],
    args = [
        "--as-test",
        "--framework=torch",
        "--stop-reward=10",
        "--stop-timesteps=300000",
        "--env=RepeatInitialObsEnv",
        "--num-cpus=4",
    ],
    main = "examples/custom_recurrent_rnn_tokenizer.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/custom_train_fn",
    size = "medium",
    srcs = ["examples/custom_train_fn.py"],
    main = "examples/custom_train_fn.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/deterministic_training_tf",
    size = "medium",
    srcs = ["examples/deterministic_training.py"],
    args = [
        "--as-test",
        "--stop-iters=1",
        "--framework=tf",
        "--num-gpus=1",
        "--num-gpus-per-worker=1",
    ],
    main = "examples/deterministic_training.py",
    tags = [
        "examples",
        "exclusive",
        "multi_gpu",
        "team:rllib",
    ],
)

py_test(
    name = "examples/deterministic_training_tf2",
    size = "medium",
    srcs = ["examples/deterministic_training.py"],
    args = [
        "--as-test",
        "--stop-iters=1",
        "--framework=tf2",
        "--num-gpus=1",
        "--num-gpus-per-worker=1",
    ],
    main = "examples/deterministic_training.py",
    tags = [
        "examples",
        "exclusive",
        "multi_gpu",
        "team:rllib",
    ],
)

py_test(
    name = "examples/deterministic_training_torch",
    size = "medium",
    srcs = ["examples/deterministic_training.py"],
    args = [
        "--as-test",
        "--stop-iters=1",
        "--framework=torch",
        "--num-gpus=1",
        "--num-gpus-per-worker=1",
    ],
    main = "examples/deterministic_training.py",
    tags = [
        "examples",
        "exclusive",
        "multi_gpu",
        "team:rllib",
    ],
)

py_test(
    name = "examples/env/greyscale_env",
    size = "medium",
    srcs = ["examples/env/greyscale_env.py"],
    args = ["--stop-iters=1 --as-test --framework torch"],
    tags = [
        "examples",
        "no_main",
        "team:rllib",
    ],
)

# New API Stack
py_test(
    name = "examples/evaluation/evaluation_parallel_to_training_duration_auto_torch_envrunner",
    size = "medium",
    srcs = ["examples/evaluation/evaluation_parallel_to_training.py"],
    args = [
        "--enable-new-api-stack",
        "--as-test",
        "--stop-reward=450.0",
        "--num-cpus=6",
        "--evaluation-duration=auto",
    ],
    main = "examples/evaluation/evaluation_parallel_to_training.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/evaluation/evaluation_parallel_to_training_multi_agent_duration_auto_torch_envrunner",
    size = "medium",
    srcs = ["examples/evaluation/evaluation_parallel_to_training.py"],
    args = [
        "--enable-new-api-stack",
        "--num-agents=2",
        "--as-test",
        "--stop-reward=900.0",
        "--num-cpus=6",
        "--evaluation-duration=auto",
        "--evaluation-duration-unit=episodes",
    ],
    main = "examples/evaluation/evaluation_parallel_to_training.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/evaluation/evaluation_parallel_to_training_511_ts_torch_envrunner",
    size = "medium",
    srcs = ["examples/evaluation/evaluation_parallel_to_training.py"],
    args = [
        "--enable-new-api-stack",
        "--as-test",
        "--stop-reward=450.0",
        "--num-cpus=6",
        "--evaluation-num-workers=3",
        "--evaluation-duration=511",
        "--evaluation-duration-unit=timesteps",
    ],
    main = "examples/evaluation/evaluation_parallel_to_training.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/evaluation/evaluation_parallel_to_training_multi_agent_1001_ts_torch_envrunner",
    size = "medium",
    srcs = ["examples/evaluation/evaluation_parallel_to_training.py"],
    args = [
        "--enable-new-api-stack",
        "--num-agents=2",
        "--as-test",
        "--stop-reward=900.0",
        "--num-cpus=6",
        "--evaluation-duration=1001",
        "--evaluation-duration-unit=timesteps",
    ],
    main = "examples/evaluation/evaluation_parallel_to_training.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/evaluation/evaluation_parallel_to_training_13_episodes_torch_envrunner",
    size = "medium",
    srcs = ["examples/evaluation/evaluation_parallel_to_training.py"],
    args = [
        "--enable-new-api-stack",
        "--as-test",
        "--stop-reward=450.0",
        "--num-cpus=6",
        "--evaluation-duration=13",
        "--evaluation-duration-unit=episodes",
    ],
    main = "examples/evaluation/evaluation_parallel_to_training.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/evaluation/evaluation_parallel_to_training_multi_agent_10_episodes_torch_envrunner",
    size = "medium",
    srcs = ["examples/evaluation/evaluation_parallel_to_training.py"],
    args = [
        "--enable-new-api-stack",
        "--num-agents=2",
        "--as-test",
        "--stop-reward=900.0",
        "--num-cpus=6",
        "--evaluation-duration=10",
        "--evaluation-duration-unit=episodes",
    ],
    main = "examples/evaluation/evaluation_parallel_to_training.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)
# END: New API Stack

py_test(
    name = "examples/evaluation/evaluation_parallel_to_training_13_episodes_tf",
    size = "medium",
    srcs = ["examples/evaluation/evaluation_parallel_to_training.py"],
    args = [
        "--as-test",
        "--framework=tf",
        "--stop-reward=50.0",
        "--num-cpus=6",
        "--evaluation-duration=13",
    ],
    main = "examples/evaluation/evaluation_parallel_to_training.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/evaluation/evaluation_parallel_to_training_duration_auto_tf",
    size = "medium",
    srcs = ["examples/evaluation/evaluation_parallel_to_training.py"],
    args = [
        "--as-test",
        "--stop-reward=50.0",
        "--framework=tf",
        "--num-cpus=6",
        "--evaluation-duration=auto",
    ],
    main = "examples/evaluation/evaluation_parallel_to_training.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/evaluation/evaluation_parallel_to_training_duration_auto_torch",
    size = "medium",
    srcs = ["examples/evaluation/evaluation_parallel_to_training.py"],
    args = [
        "--as-test",
        "--stop-reward=50.0",
        "--num-cpus=6",
        "--evaluation-duration=auto",
    ],
    main = "examples/evaluation/evaluation_parallel_to_training.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/evaluation/evaluation_parallel_to_training_duration_auto_tf2",
    size = "medium",
    srcs = ["examples/evaluation/evaluation_parallel_to_training.py"],
    args = [
        "--as-test",
        "--framework=tf2",
        "--stop-reward=30.0",
        "--num-cpus=6",
        "--evaluation-num-workers=3",
        "--evaluation-duration=auto",
    ],
    main = "examples/evaluation/evaluation_parallel_to_training.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/evaluation/evaluation_parallel_to_training_211_ts_tf2",
    size = "medium",
    srcs = ["examples/evaluation/evaluation_parallel_to_training.py"],
    args = [
        "--as-test",
        "--framework=tf2",
        "--stop-reward=30.0",
        "--num-cpus=6",
        "--evaluation-num-workers=3",
        "--evaluation-duration=211",
        "--evaluation-duration-unit=timesteps",
    ],
    main = "examples/evaluation/evaluation_parallel_to_training.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/export/cartpole_dqn_export",
    size = "small",
    srcs = ["examples/export/cartpole_dqn_export.py"],
    main = "examples/export/cartpole_dqn_export.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/export/onnx_tf",
    size = "small",
    srcs = ["examples/export/onnx_tf.py"],
    args = ["--framework=tf"],
    main = "examples/export/onnx_tf.py",
    tags = [
        "examples",
        "exclusive",
        "no_main",
        "team:rllib",
    ],
)

py_test(
    name = "examples/export/onnx_tf2",
    size = "small",
    srcs = ["examples/export/onnx_tf.py"],
    args = ["--framework=tf2"],
    main = "examples/export/onnx_tf.py",
    tags = [
        "examples",
        "exclusive",
        "no_main",
        "team:rllib",
    ],
)

py_test(
    name = "examples/export/onnx_torch",
    size = "small",
    srcs = ["examples/export/onnx_torch.py"],
    main = "examples/export/onnx_torch.py",
    tags = [
        "examples",
        "exclusive",
        "no_main",
        "team:rllib",
    ],
)

py_test(
    name = "examples/fractional_gpus",
    size = "medium",
    srcs = ["examples/fractional_gpus.py"],
    args = [
        "--as-test",
        "--stop-reward=40.0",
        "--num-gpus=0",
        "--num-workers=0",
    ],
    main = "examples/fractional_gpus.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/hierarchical_training_tf",
    size = "medium",
    srcs = ["examples/hierarchical_training.py"],
    args = [
        "--framework=tf",
        "--stop-reward=0.0",
    ],
    main = "examples/hierarchical_training.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/hierarchical_training_torch",
    size = "medium",
    srcs = ["examples/hierarchical_training.py"],
    args = [
        "--framework=torch",
        "--stop-reward=0.0",
    ],
    main = "examples/hierarchical_training.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/multi_agent_cartpole_tf",
    size = "small",
    srcs = ["examples/multi_agent_cartpole.py"],
    args = [
        "--as-test",
        "--framework=tf",
        "--stop-reward=70.0",
        "--num-cpus=4",
    ],
    main = "examples/multi_agent_cartpole.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/multi_agent_cartpole_torch",
    size = "small",
    srcs = ["examples/multi_agent_cartpole.py"],
    args = [
        "--as-test",
        "--framework=torch",
        "--stop-reward=70.0",
        "--num-cpus=4",
    ],
    main = "examples/multi_agent_cartpole.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/multi_agent_cartpole_w_rlm_torch",
    size = "medium",
    srcs = ["examples/multi_agent_cartpole.py"],
    args = [
        "--as-test",
        "--framework=torch",
        "--stop-reward=70.0",
        "--num-cpus=4",
    ],
    main = "examples/multi_agent_cartpole.py",
    tags = [
        "examples",
        "exclusive",
        "rlm",
        "team:rllib",
    ],
)

py_test(
    name = "examples/multi_agent_cartpole_w_rlm_tf2",
    size = "medium",
    srcs = ["examples/multi_agent_cartpole.py"],
    args = [
        "--as-test",
        "--framework=tf2",
        "--stop-reward=70.0",
        "--num-cpus=4",
    ],
    main = "examples/multi_agent_cartpole.py",
    tags = [
        "examples",
        "exclusive",
        "rlm",
        "team:rllib",
    ],
)

py_test(
    name = "examples/multi_agent_custom_policy_tf",
    size = "medium",
    srcs = ["examples/multi_agent_custom_policy.py"],
    args = [
        "--as-test",
        "--framework=tf",
        "--stop-reward=80",
    ],
    main = "examples/multi_agent_custom_policy.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/multi_agent_custom_policy_torch",
    size = "medium",
    srcs = ["examples/multi_agent_custom_policy.py"],
    args = [
        "--as-test",
        "--framework=torch",
        "--stop-reward=80",
    ],
    main = "examples/multi_agent_custom_policy.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/multi_agent_custom_policy_w_rlm_torch",
    size = "medium",
    srcs = ["examples/multi_agent_custom_policy.py"],
    args = [
        "--as-test",
        "--framework=torch",
        "--stop-reward=80",
    ],
    main = "examples/multi_agent_custom_policy.py",
    tags = [
        "examples",
        "exclusive",
        "rlm",
        "team:rllib",
    ],
)

py_test(
    name = "examples/multi_agent_custom_policy_w_rlm_tf2",
    size = "medium",
    srcs = ["examples/multi_agent_custom_policy.py"],
    args = [
        "--as-test",
        "--framework=tf2",
        "--stop-reward=80",
    ],
    main = "examples/multi_agent_custom_policy.py",
    tags = [
        "examples",
        "exclusive",
        "rlm",
        "team:rllib",
    ],
)

py_test(
    name = "examples/multi_agent_different_spaces_for_agents_tf2",
    size = "medium",
    srcs = ["examples/multi_agent_different_spaces_for_agents.py"],
    args = [
        "--stop-iters=4",
        "--framework=tf2",
    ],
    main = "examples/multi_agent_different_spaces_for_agents.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/multi_agent_different_spaces_for_agents_torch",
    size = "medium",
    srcs = ["examples/multi_agent_different_spaces_for_agents.py"],
    args = [
        "--stop-iters=4",
        "--framework=torch",
    ],
    main = "examples/multi_agent_different_spaces_for_agents.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/multi_agent_different_spaces_for_agents_w_rlm_torch",
    size = "medium",
    srcs = ["examples/multi_agent_different_spaces_for_agents.py"],
    args = [
        "--stop-iters=4",
        "--framework=torch",
    ],
    main = "examples/multi_agent_different_spaces_for_agents.py",
    tags = [
        "examples",
        "exclusive",
        "rlm",
        "team:rllib",
    ],
)

py_test(
    name = "examples/multi_agent_different_spaces_for_agents_w_rlm_tf2",
    size = "medium",
    srcs = ["examples/multi_agent_different_spaces_for_agents.py"],
    args = [
        "--stop-iters=4",
        "--framework=tf2",
    ],
    main = "examples/multi_agent_different_spaces_for_agents.py",
    tags = [
        "examples",
        "exclusive",
        "rlm",
        "team:rllib",
    ],
)

py_test(
    name = "examples/multi_agent_independent_learning",
    size = "medium",
    srcs = ["examples/multi_agent_independent_learning.py"],
    args = [
        "--num-gpus=0",
        "--as-test",
    ],
    main = "examples/multi_agent_independent_learning.py",
    tags = [
        "examples",
        "team:rllib",
    ],
)

py_test(
    name = "examples/multi_agent_two_trainers_tf",
    size = "medium",
    srcs = ["examples/multi_agent_two_trainers.py"],
    args = [
        "--as-test",
        "--framework=tf",
        "--stop-reward=70",
    ],
    main = "examples/multi_agent_two_trainers.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/multi_agent_two_trainers_torch",
    size = "small",
    srcs = ["examples/multi_agent_two_trainers.py"],
    args = [
        "--as-test",
        "--framework=torch",
        "--stop-reward=70",
    ],
    main = "examples/multi_agent_two_trainers.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/offline_rl_torch",
    size = "medium",
    srcs = ["examples/offline_rl.py"],
    args = [
        "--as-test",
        "--stop-reward=-300",
        "--stop-iters=1",
    ],
    main = "examples/offline_rl.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/nested_action_spaces_ppo_tf",
    size = "medium",
    srcs = ["examples/nested_action_spaces.py"],
    args = [
        "--as-test",
        "--framework=tf",
        "--stop-reward=-600",
        "--algo=PPO",
    ],
    main = "examples/nested_action_spaces.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/nested_action_spaces_ppo_torch_envrunner",
    size = "large",
    srcs = ["examples/nested_action_spaces.py"],
    args = [
        "--as-test",
        "--framework=torch",
        "--stop-reward=-500.0",
        "--algo=PPO",
        "--enable-new-api-stack",
    ],
    main = "examples/nested_action_spaces.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/nested_action_spaces_multi_agent_ppo_torch_envrunner",
    size = "large",
    srcs = ["examples/nested_action_spaces.py"],
    args = [
        "--as-test",
        "--num-agents=2",
        "--framework=torch",
        "--stop-reward=-1000.0",
        "--algo=PPO",
        "--enable-new-api-stack",
    ],
    main = "examples/nested_action_spaces.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/parametric_actions_cartpole_dqn_tf",
    size = "medium",
    srcs = ["examples/parametric_actions_cartpole.py"],
    args = [
        "--as-test",
        "--framework=tf",
        "--stop-reward=60.0",
        "--run=DQN",
    ],
    main = "examples/parametric_actions_cartpole.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/parametric_actions_cartpole_dqn_torch",
    size = "medium",
    srcs = ["examples/parametric_actions_cartpole.py"],
    args = [
        "--as-test",
        "--framework=torch",
        "--stop-reward=60.0",
        "--run=DQN",
    ],
    main = "examples/parametric_actions_cartpole.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/parametric_actions_cartpole_embeddings_learnt_by_model",
    size = "medium",
    srcs = ["examples/parametric_actions_cartpole_embeddings_learnt_by_model.py"],
    args = [
        "--as-test",
        "--stop-reward=80.0",
    ],
    main = "examples/parametric_actions_cartpole_embeddings_learnt_by_model.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/inference_and_serving/policy_inference_after_training_tf",
    size = "medium",
    srcs = ["examples/inference_and_serving/policy_inference_after_training.py"],
    args = [
        "--stop-iters=3",
        "--framework=tf",
    ],
    main = "examples/inference_and_serving/policy_inference_after_training.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/inference_and_serving/policy_inference_after_training_torch",
    size = "medium",
    srcs = ["examples/inference_and_serving/policy_inference_after_training.py"],
    args = [
        "--stop-iters=3",
        "--framework=torch",
    ],
    main = "examples/inference_and_serving/policy_inference_after_training.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/inference_and_serving/policy_inference_after_training_with_attention_tf",
    size = "medium",
    srcs = ["examples/inference_and_serving/policy_inference_after_training_with_attention.py"],
    args = [
        "--stop-iters=2",
        "--framework=tf",
    ],
    main = "examples/inference_and_serving/policy_inference_after_training_with_attention.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/inference_and_serving/policy_inference_after_training_with_attention_torch",
    size = "medium",
    srcs = ["examples/inference_and_serving/policy_inference_after_training_with_attention.py"],
    args = [
        "--stop-iters=2",
        "--framework=torch",
    ],
    main = "examples/inference_and_serving/policy_inference_after_training_with_attention.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/inference_and_serving/policy_inference_after_training_with_lstm_tf",
    size = "medium",
    srcs = ["examples/inference_and_serving/policy_inference_after_training_with_lstm.py"],
    args = [
        "--stop-iters=1",
        "--framework=tf",
    ],
    main = "examples/inference_and_serving/policy_inference_after_training_with_lstm.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/inference_and_serving/policy_inference_after_training_with_lstm_torch",
    size = "medium",
    srcs = ["examples/inference_and_serving/policy_inference_after_training_with_lstm.py"],
    args = [
        "--stop-iters=1",
        "--framework=torch",
    ],
    main = "examples/inference_and_serving/policy_inference_after_training_with_lstm.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/replay_buffer_api",
    size = "large",
    srcs = ["examples/replay_buffer_api.py"],
    tags = [
        "examples",
        "team:rllib",
    ],
)

py_test(
    name = "examples/restore_1_of_n_agents_from_checkpoint",
    size = "medium",
    srcs = ["examples/restore_1_of_n_agents_from_checkpoint.py"],
    args = [
        "--pre-training-iters=1",
        "--stop-iters=1",
        "--num-cpus=4",
    ],
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/rock_paper_scissors_multiagent_tf",
    size = "medium",
    srcs = ["examples/rock_paper_scissors_multiagent.py"],
    args = [
        "--as-test",
        "--framework=tf",
    ],
    main = "examples/rock_paper_scissors_multiagent.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/rock_paper_scissors_multiagent_torch",
    size = "medium",
    srcs = ["examples/rock_paper_scissors_multiagent.py"],
    args = [
        "--as-test",
        "--framework=torch",
    ],
    main = "examples/rock_paper_scissors_multiagent.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/self_play_with_open_spiel_connect_4_ppo_tf",
    size = "medium",
    srcs = ["examples/self_play_with_open_spiel.py"],
    args = [
        "--framework=tf",
        "--env=connect_four",
        "--win-rate-threshold=0.9",
        "--num-episodes-human-play=0",
        "--min-league-size=3",
    ],
    main = "examples/self_play_with_open_spiel.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/self_play_with_open_spiel_connect_4_ppo_torch",
    size = "medium",
    srcs = ["examples/self_play_with_open_spiel.py"],
    args = [
        "--framework=torch",
        "--env=connect_four",
        "--win-rate-threshold=0.9",
        "--num-episodes-human-play=0",
        "--min-league-size=3",
    ],
    main = "examples/self_play_with_open_spiel.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/self_play_with_open_spiel_connect_4_ppo_torch_envrunner",
    size = "medium",
    srcs = ["examples/self_play_with_open_spiel.py"],
    args = [
        "--enable-new-api-stack",
        "--framework=torch",
        "--env=connect_four",
        "--win-rate-threshold=0.9",
        "--num-episodes-human-play=0",
        "--min-league-size=4",
    ],
    main = "examples/self_play_with_open_spiel.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/self_play_league_based_with_open_spiel_connect_4_ppo_torch_envrunner",
    size = "large",
    srcs = ["examples/self_play_league_based_with_open_spiel.py"],
    args = [
        "--enable-new-api-stack",
        "--framework=torch",
        "--env=connect_four",
        "--win-rate-threshold=0.8",
        "--num-episodes-human-play=0",
        "--min-league-size=8",
    ],
    main = "examples/self_play_league_based_with_open_spiel.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/two_trainer_workflow_tf",
    size = "medium",
    srcs = ["examples/two_trainer_workflow.py"],
    args = [
        "--as-test",
        "--stop-reward=450.0",
    ],
    main = "examples/two_trainer_workflow.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/two_trainer_workflow_torch",
    size = "medium",
    srcs = ["examples/two_trainer_workflow.py"],
    args = [
        "--as-test",
        "--torch",
        "--stop-reward=450.0",
    ],
    main = "examples/two_trainer_workflow.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/two_step_game_ppo_tf",
    size = "medium",
    srcs = ["examples/two_step_game.py"],
    args = [
        "--as-test",
        "--framework=tf",
        "--stop-reward=7",
    ],
    main = "examples/two_step_game.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/two_step_game_ppo_torch",
    size = "medium",
    srcs = ["examples/two_step_game.py"],
    args = [
        "--as-test",
        "--framework=torch",
        "--stop-reward=7",
    ],
    main = "examples/two_step_game.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

# ----------------------
# new connector examples
# ----------------------

# Framestacking examples only run in smoke-test mode (a few iters only).
py_test(
    name = "examples/connectors/connector_v2_frame_stacking_ppo",
    size = "medium",
    srcs = ["examples/connectors/connector_v2_frame_stacking.py"],
    args = [
        "--enable-new-api-stack",
        "--stop-iter=2",
        "--framework=torch",
        "--algo=PPO",
    ],
    main = "examples/connectors/connector_v2_frame_stacking.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/connectors/connector_v2_frame_stacking_multi_agent_ppo",
    size = "medium",
    srcs = ["examples/connectors/connector_v2_frame_stacking.py"],
    args = [
        "--enable-new-api-stack",
        "--num-agents=2",
        "--stop-iter=2",
        "--framework=torch",
        "--algo=PPO",
        "--num-env-runners=4",
        "--num-cpus=6",
    ],
    main = "examples/connectors/connector_v2_frame_stacking.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

# Nested observation spaces (flattening).
py_test(
    name = "examples/connectors/connector_v2_nested_observation_spaces_ppo",
    size = "medium",
    srcs = ["examples/connectors/connector_v2_nested_observation_spaces.py"],
    args = [
        "--enable-new-api-stack",
        "--as-test",
        "--stop-reward=400.0",
        "--framework=torch",
        "--algo=PPO",
    ],
    main = "examples/connectors/connector_v2_nested_observation_spaces.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/connectors/connector_v2_nested_observation_spaces_multi_agent_ppo",
    size = "medium",
    srcs = ["examples/connectors/connector_v2_nested_observation_spaces.py"],
    args = [
        "--enable-new-api-stack",
        "--num-agents=2",
        "--as-test",
        "--stop-reward=800.0",
        "--framework=torch",
        "--algo=PPO",
    ],
    main = "examples/connectors/connector_v2_nested_observation_spaces.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

# Prev-r/prev actions + LSTM example.
py_test(
    name = "examples/connectors/connector_v2_prev_actions_prev_rewards_ppo",
    size = "large",
    srcs = ["examples/connectors/connector_v2_prev_actions_prev_rewards.py"],
    args = [
        "--enable-new-api-stack",
        "--as-test",
        "--stop-reward=200.0",
        "--framework=torch",
        "--algo=PPO",
        "--num-env-runners=4",
        "--num-cpus=6",
    ],
    main = "examples/connectors/connector_v2_prev_actions_prev_rewards.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/connectors/connector_v2_prev_actions_prev_rewards_multi_agent_ppo",
    size = "large",
    srcs = ["examples/connectors/connector_v2_prev_actions_prev_rewards.py"],
    args = [
        "--enable-new-api-stack",
        "--num-agents=2",
        "--as-test",
        "--stop-reward=400.0",
        "--framework=torch",
        "--algo=PPO",
        "--num-env-runners=4",
        "--num-cpus=6",
    ],
    main = "examples/connectors/connector_v2_prev_actions_prev_rewards.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

# MeanStd filtering example.
py_test(
    name = "examples/connectors/connector_v2_mean_std_filtering_ppo",
    size = "medium",
    srcs = ["examples/connectors/connector_v2_mean_std_filtering.py"],
    args = [
        "--enable-new-api-stack",
        "--as-test",
        "--stop-reward=-300.0",
        "--framework=torch",
        "--algo=PPO",
        "--num-env-runners=2",
    ],
    main = "examples/connectors/connector_v2_mean_std_filtering.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/connectors/connector_v2_mean_std_filtering_multi_agent_ppo",
    size = "medium",
    srcs = ["examples/connectors/connector_v2_mean_std_filtering.py"],
    args = [
        "--enable-new-api-stack",
        "--num-agents=2",
        "--as-test",
        "--stop-reward=-600.0",
        "--framework=torch",
        "--algo=PPO",
        "--num-env-runners=5",
        "--num-cpus=6",
    ],
    main = "examples/connectors/connector_v2_mean_std_filtering.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

# ----------------------
# old connector examples
# ----------------------
py_test(
    name = "examples/connectors/v1/run_connector_policy",
    size = "small",
    srcs = ["examples/connectors/v1/run_connector_policy.py"],
    main = "examples/connectors/v1/run_connector_policy.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/connectors/v1/adapt_connector_policy",
    size = "small",
    srcs = ["examples/connectors/v1/adapt_connector_policy.py"],
    main = "examples/connectors/v1/adapt_connector_policy.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/connectors/v1/self_play_with_policy_checkpoint",
    size = "small",
    srcs = ["examples/connectors/v1/self_play_with_policy_checkpoint.py"],
    args = [
        "--train_iteration=1",  # Smoke test.
    ],
    main = "examples/connectors/v1/self_play_with_policy_checkpoint.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

# --------------------------------------------------------------------
# examples/learner directory
#
#
# Description: These are RLlib tests for the new multi-gpu enabled
# training stack via Learners.
#
# NOTE: Add tests alphabetically to this list.
# --------------------------------------------------------------------

py_test(
    name = "examples/learner/multi_agent_cartpole_ppo_torch",
    size = "medium",
    srcs = ["examples/learner/multi_agent_cartpole_ppo.py"],
    args = [
        "--as-test",
        "--framework=torch",
        "--num-gpus=0",
    ],
    main = "examples/learner/multi_agent_cartpole_ppo.py",
    tags = [
        "examples",
        "team:rllib",
    ],
)

py_test(
    name = "examples/learner/multi_agent_cartpole_ppo_torch_gpu",
    size = "medium",
    srcs = ["examples/learner/multi_agent_cartpole_ppo.py"],
    args = [
        "--as-test",
        "--framework=torch",
        "--num-gpus=1",
    ],
    main = "examples/learner/multi_agent_cartpole_ppo.py",
    tags = [
        "examples",
        "exclusive",
        "gpu",
        "team:rllib",
    ],
)

py_test(
    name = "examples/learner/multi_agent_cartpole_ppo_torch_multi_gpu",
    size = "medium",
    srcs = ["examples/learner/multi_agent_cartpole_ppo.py"],
    args = [
        "--as-test",
        "--framework=torch",
        "--num-gpus=2",
    ],
    main = "examples/learner/multi_agent_cartpole_ppo.py",
    tags = [
        "examples",
        "exclusive",
        "multi_gpu",
        "team:rllib",
    ],
)

py_test(
    name = "examples/learner/ppo_tuner_local_cpu_torch",
    size = "medium",
    srcs = ["examples/learner/ppo_tuner.py"],
    args = [
        "--framework=torch",
        "--config=local-cpu",
    ],
    main = "examples/learner/ppo_tuner.py",
    tags = [
        "examples",
        "team:rllib",
    ],
)

py_test(
    name = "examples/learner/ppo_tuner_local_cpu_tf2",
    size = "medium",
    srcs = ["examples/learner/ppo_tuner.py"],
    args = [
        "--framework=tf2",
        "--config=local-cpu",
    ],
    main = "examples/learner/ppo_tuner.py",
    tags = [
        "examples",
        "team:rllib",
    ],
)

py_test(
    name = "examples/learner/ppo_tuner_local_gpu_torch",
    size = "medium",
    srcs = ["examples/learner/ppo_tuner.py"],
    args = [
        "--framework=torch",
        "--config=local-gpu",
    ],
    main = "examples/learner/ppo_tuner.py",
    tags = [
        "examples",
        "gpu",
        "team:rllib",
    ],
)

py_test(
    name = "examples/learner/ppo_tuner_local_gpu_tf2",
    size = "medium",
    srcs = ["examples/learner/ppo_tuner.py"],
    args = [
        "--framework=tf2",
        "--config=local-gpu",
    ],
    main = "examples/learner/ppo_tuner.py",
    tags = [
        "examples",
        "exclusive",
        "gpu",
        "team:rllib",
    ],
)

py_test(
    name = "examples/learner/ppo_tuner_remote_cpu_torch",
    size = "medium",
    srcs = ["examples/learner/ppo_tuner.py"],
    args = [
        "--framework=torch",
        "--config=remote-cpu",
    ],
    main = "examples/learner/ppo_tuner.py",
    tags = [
        "examples",
        "team:rllib",
    ],
)

py_test(
    name = "examples/learner/ppo_tuner_remote_cpu_tf2",
    size = "medium",
    srcs = ["examples/learner/ppo_tuner.py"],
    args = [
        "--framework=tf2",
        "--config=remote-cpu",
    ],
    main = "examples/learner/ppo_tuner.py",
    tags = [
        "examples",
        "team:rllib",
    ],
)

py_test(
    name = "examples/learner/ppo_tuner_remote_gpu_torch",
    size = "medium",
    srcs = ["examples/learner/ppo_tuner.py"],
    args = [
        "--framework=torch",
        "--config=remote-gpu",
    ],
    main = "examples/learner/ppo_tuner.py",
    tags = [
        "examples",
        "exclusive",
        "gpu",
        "team:rllib",
    ],
)

py_test(
    name = "examples/learner/ppo_tuner_remote_gpu_tf2",
    size = "medium",
    srcs = ["examples/learner/ppo_tuner.py"],
    args = [
        "--framework=tf2",
        "--config=remote-gpu",
    ],
    main = "examples/learner/ppo_tuner.py",
    tags = [
        "examples",
        "exclusive",
        "gpu",
        "team:rllib",
    ],
)

py_test(
    name = "examples/learner/ppo_tuner_multi_gpu_torch",
    size = "medium",
    srcs = ["examples/learner/ppo_tuner.py"],
    args = [
        "--framework=torch",
        "--config=multi-gpu-ddp",
    ],
    main = "examples/learner/ppo_tuner.py",
    tags = [
        "examples",
        "exclusive",
        "multi_gpu",
        "team:rllib",
    ],
)

py_test(
    name = "examples/learner/ppo_tuner_multi_gpu_tf2",
    size = "medium",
    srcs = ["examples/learner/ppo_tuner.py"],
    args = [
        "--framework=tf2",
        "--config=multi-gpu-ddp",
    ],
    main = "examples/learner/ppo_tuner.py",
    tags = [
        "examples",
        "exclusive",
        "multi_gpu",
        "team:rllib",
    ],
)

py_test(
    name = "examples/learner/train_w_bc_finetune_w_ppo",
    size = "medium",
    srcs = ["examples/learner/train_w_bc_finetune_w_ppo.py"],
    main = "examples/learner/train_w_bc_finetune_w_ppo.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
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
    size = "medium",
    srcs = ["examples/documentation/custom_gym_env.py"],
    main = "examples/documentation/custom_gym_env.py",
    tags = [
        "documentation",
        "no_main",
        "team:rllib",
    ],
)

py_test(
    name = "examples/documentation/saving_and_loading_algos_and_policies",
    size = "large",
    srcs = ["examples/documentation/saving_and_loading_algos_and_policies.py"],
    main = "examples/documentation/saving_and_loading_algos_and_policies.py",
    tags = [
        "documentation",
        "no_main",
        "team:rllib",
    ],
)

py_test(
    name = "examples/documentation/replay_buffer_demo",
    size = "medium",
    srcs = ["examples/documentation/replay_buffer_demo.py"],
    main = "examples/documentation/replay_buffer_demo.py",
    tags = [
        "documentation",
        "no_main",
        "team:rllib",
    ],
)

py_test(
    name = "examples/documentation/rllib_on_ray_readme",
    size = "medium",
    srcs = ["examples/documentation/rllib_on_ray_readme.py"],
    main = "examples/documentation/rllib_on_ray_readme.py",
    tags = [
        "documentation",
        "no_main",
        "team:rllib",
    ],
)

py_test(
    name = "examples/documentation/rllib_on_rllib_readme",
    size = "medium",
    srcs = ["examples/documentation/rllib_on_rllib_readme.py"],
    main = "examples/documentation/rllib_on_rllib_readme.py",
    tags = [
        "documentation",
        "no_main",
        "team:rllib",
    ],
)

# --------------------------------------------------------------------
# Manual/disabled tests
# --------------------------------------------------------------------
py_test_module_list(
    size = "large",
    extra_srcs = [],
    files = [
        "env/wrappers/tests/test_kaggle_wrapper.py",
        "examples/env/tests/test_cliff_walking_wall_env.py",
        "examples/env/tests/test_coin_game_non_vectorized_env.py",
        "examples/env/tests/test_coin_game_vectorized_env.py",
        "examples/env/tests/test_matrix_sequential_social_dilemma.py",
        "examples/env/tests/test_wrappers.py",
        "tests/test_dnc.py",
        "tests/test_perf.py",
        "utils/tests/test_utils.py",
    ],
    tags = [
        "manual",
        "no_main",
        "team:rllib",
    ],
    deps = [],
)
