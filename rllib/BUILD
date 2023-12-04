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
    files = glob(
        ["**/*.py"],
        exclude=[
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
            "utils/spaces/space_utils.py"
        ]
    ),
    tags = ["team:rllib"],
    size = "enormous"
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
    main = "benchmarks/torch_compile/run_inference_bm.py",
    tags = ["team:rllib", "exclusive", "benchmark", "torch_2.x_only_benchmark"],
    size = "small",
    srcs = ["benchmarks/torch_compile/run_inference_bm.py"],
    args = ["--smoke-test"]
)

py_test(
    name = "torch_compile_ppo_with_inference",
    main = "benchmarks/torch_compile/run_ppo_with_inference_bm.py",
    tags = ["team:rllib", "exclusive", "benchmark", "torch_2.x_only_benchmark"],
    size = "medium",
    srcs = ["benchmarks/torch_compile/run_ppo_with_inference_bm.py"],
    args = ["--smoke-test"]
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
    name = "learning_tests_cartpole_appo_no_vtrace",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete"],
    size = "medium", # bazel may complain about it being too long sometimes - medium is on purpose as some frameworks take longer
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/appo/cartpole-appo.yaml"],
    args = ["--dir=tuned_examples/appo"]
)

py_test(
    name = "learning_tests_cartpole_appo_w_rl_modules_and_learner",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete", "no_tf_static_graph"],
    size = "medium", # bazel may complain about it being too long sometimes - medium is on purpose as some frameworks take longer
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/appo/cartpole-appo-w-rl-modules-and-learner.yaml"],
    args = ["--dir=tuned_examples/appo"]
)

py_test(
    name = "learning_tests_cartpole_separate_losses_appo",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete"],
    size = "medium",
    srcs = ["tests/run_regression_tests.py"],
    data = [
        "tuned_examples/appo/cartpole-appo-vtrace-separate-losses.py"
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
    size = "enormous",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/appo/multi-agent-cartpole-w-100-policies-appo.py"],
    args = ["--dir=tuned_examples/appo"]
)

py_test(
    name = "learning_tests_cartpole_appo_fake_gpus",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete", "fake_gpus"],
    size = "medium",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/appo/cartpole-appo-vtrace-fake-gpus.yaml"],
    args = ["--dir=tuned_examples/appo"]
)

py_test(
    name = "learning_tests_stateless_cartpole_appo_vtrace",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete"],
    size = "enormous",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/appo/stateless-cartpole-appo-vtrace.py"],
    args = ["--dir=tuned_examples/appo"]
)

# CQL
py_test(
    name = "learning_tests_pendulum_cql",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_pendulum", "learning_tests_continuous", "learning_tests_with_ray_data"],
    size = "medium",
    srcs = ["tests/run_regression_tests.py"],
    # Include the zipped json data file as well.
    data = [
        "tuned_examples/cql/pendulum-cql.yaml",
        "tests/data/pendulum/enormous.zip",
    ],
    args = ["--dir=tuned_examples/cql"]
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

py_test(
    name = "learning_tests_cartpole_dqn_fake_gpus",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete", "fake_gpus"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/dqn/cartpole-dqn-fake-gpus.yaml"],
    args = ["--dir=tuned_examples/dqn"]
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
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete"],
    size = "medium",
    srcs = ["tests/run_regression_tests.py"],
    data = [
        "tuned_examples/impala/cartpole-impala-separate-losses.py"
    ],
    args = ["--dir=tuned_examples/impala"]
)

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

# PPO
# w/ new EnvRunner
py_test(
    name = "learning_tests_cartpole_ppo_w_env_runner",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete", "no_tf_static_graph"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/ppo/cartpole-ppo-envrunner.py"],
    args = ["--dir=tuned_examples/ppo"]
)

# TODO (sven): Remove the torch only flag for this test (tf2 is still very slow for EnvRunner and we need to debug this further).
py_test(
    name = "learning_tests_pendulum_ppo_w_env_runner",
    main = "tests/run_regression_tests.py",
    tags = ["torch_only", "team:rllib", "exclusive", "learning_tests", "learning_tests_pendulum", "learning_tests_continuous"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/ppo/pendulum-ppo-envrunner.py"],
    args = ["--dir=tuned_examples/ppo"]
)

# w/o EnvRunner (using RolloutWorker)
py_test(
    name = "learning_tests_cartpole_truncated_ppo",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete", "no_tf_static_graph"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/ppo/cartpole-truncated-ppo.py"],
    args = ["--dir=tuned_examples/ppo"]
)

py_test(
    name = "learning_tests_pendulum_ppo",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_pendulum", "learning_tests_continuous", "no_tf_static_graph"],
    size = "large", # bazel may complain about it being too long sometimes - large is on purpose as some frameworks take longer
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/ppo/pendulum-ppo.yaml"],
    args = ["--dir=tuned_examples/ppo"]
)

py_test(
    name = "learning_tests_multi_agent_pendulum_ppo",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_pendulum", "learning_tests_continuous", "no_tf_static_graph"],
    size = "large", # bazel may complain about it being too long sometimes - large is on purpose as some frameworks take longer
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/ppo/multi_agent_pendulum_ppo.py"],
    args = ["--dir=tuned_examples/ppo"]
)

py_test(
    name = "learning_tests_transformed_actions_pendulum_ppo",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_pendulum", "learning_tests_continuous", "no_tf_static_graph"],
    size = "large", # bazel may complain about it being too long sometimes - large is on purpose as some frameworks take longer
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/ppo/pendulum-transformed-actions-ppo.yaml"],
    args = ["--dir=tuned_examples/ppo"]
)

py_test(
    name = "learning_tests_repeat_after_me_ppo_wo_rl_module",
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
    size = "medium",
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

# APPO
py_test(
    name = "test_appo",
    tags = ["team:rllib", "algorithms_dir"],
    size = "large",
    srcs = ["algorithms/appo/tests/test_appo.py"]
)
py_test(
    name = "test_appo_off_policyness",
    tags = ["team:rllib", "algorithms_dir", "multi_gpu", "exclusive"],
    size = "large",
    srcs = ["algorithms/appo/tests/test_appo_off_policyness.py"]
)
py_test(
    name = "test_appo_learner",
    tags = ["team:rllib", "algorithms_dir"],
    size = "medium",
    srcs = ["algorithms/appo/tests/test_appo_learner.py"]
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

# DQN
py_test(
    name = "test_dqn",
    tags = ["team:rllib", "algorithms_dir"],
    size = "large",
    srcs = ["algorithms/dqn/tests/test_dqn.py"]
)
py_test(
    name = "test_repro_dqn",
    tags = ["team:rllib", "algorithms_dir", "gpu"],
    size = "large",
    srcs = ["algorithms/dqn/tests/test_repro_dqn.py"]
)

# DreamerV3
py_test(
    name = "test_dreamerv3",
    tags = ["team:rllib", "algorithms_dir"],
    size = "large",
    srcs = ["algorithms/dreamerv3/tests/test_dreamerv3.py"]
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
py_test(
    name = "test_vtrace_v2",
    tags = ["team:rllib", "algorithms_dir"],
    size = "small",
    srcs = ["algorithms/impala/tests/test_vtrace_v2.py"]
)
py_test(
    name = "test_impala_off_policyness",
    tags = ["team:rllib", "algorithms_dir", "exclusive"],
    size = "large",
    srcs = ["algorithms/impala/tests/test_impala_off_policyness.py"]
)
py_test(
    name = "test_impala_learner",
    tags = ["team:rllib", "algorithms_dir"],
    size = "medium",
    srcs = ["algorithms/impala/tests/test_impala_learner.py"]
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

# PPO
py_test(
    name = "test_ppo_with_env_runner",
    tags = ["team:rllib", "algorithms_dir"],
    size = "medium",
    srcs = ["algorithms/ppo/tests/test_ppo_with_env_runner.py"]
)

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
py_test(
    name = "test_ppo_rl_module",
    tags = ["team:rllib", "algorithms_dir"],
    size = "large",
    srcs = ["algorithms/ppo/tests/test_ppo_rl_module.py"]
)
py_test(
    name = "test_ppo_learner",
    tags = ["team:rllib", "algorithms_dir"],
    size = "large",
    srcs = ["algorithms/ppo/tests/test_ppo_learner.py"]
)
py_test(
    name = "test_repro_ppo",
    tags = ["team:rllib", "algorithms_dir", "gpu"],
    size = "large",
    srcs = ["algorithms/ppo/tests/test_repro_ppo.py"]
)

# SAC
py_test(
    name = "test_sac",
    tags = ["team:rllib", "algorithms_dir"],
    size = "large",
    srcs = ["algorithms/sac/tests/test_sac.py"]
)

# RNNSAC
py_test(
    name = "test_rnnsac",
    tags = ["team:rllib", "algorithms_dir"],
    size = "small",
    srcs = ["algorithms/sac/tests/test_rnnsac.py"]
)

# --------------------------------------------------------------------
# Memory leak tests
#
# Tag: memory_leak_tests
# --------------------------------------------------------------------

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
    name = "test_memory_leak_ppo_new_stack",
    tags = ["team:rllib", "memory_leak_tests"],
    main = "utils/tests/run_memory_leak_tests.py",
    size = "large",
    srcs = ["utils/tests/run_memory_leak_tests.py"],
    data = ["tuned_examples/ppo/memory-leak-test-ppo-new-stack.py"],
    args = ["--dir=tuned_examples/ppo", "--to-check=rollout_worker"]
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
    size = "medium",
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
    tags = ["team:rllib", "tests_dir"],
    size = "large",
    srcs = ["env/tests/test_multi_agent_env.py"]
)

# py_test(
#    name = "env/tests/test_multi_agent_episode",
#    tags = ["team:rllib", "env"],
#    size = "medium",
#    srcs = ["env/tests/test_multi_agent_episode.py"]
# )

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
    tags = ["team:rllib", "env"],
    size = "medium",
    srcs = ["env/tests/test_single_agent_env_runner.py"]
)

py_test(
    name = "env/tests/test_single_agent_episode",
    tags = ["team:rllib", "env"],
    size = "small",
    srcs = ["env/tests/test_single_agent_episode.py"]
)

py_test(
    name = "env/tests/test_lookback_buffer",
    tags = ["team:rllib", "env"],
    size = "small",
    srcs = ["env/tests/test_lookback_buffer.py"]
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
    size = "large",
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
# RLlib core
# rllib/core/
#
# Tag: core
# --------------------------------------------------------------------

# Catalog
py_test(
    name = "test_catalog",
    tags = ["team:rllib", "core"],
    size = "medium",
    srcs = ["core/models/tests/test_catalog.py"]
)

# Default Models
py_test(
    name = "test_base_models",
    tags = ["team:rllib", "core"],
    size = "small",
    srcs = ["core/models/tests/test_base_models.py"]
)

py_test(
    name = "test_cnn_encoders",
    tags = ["team:rllib", "core", "models"],
    size = "large",
    srcs = ["core/models/tests/test_cnn_encoders.py"]
)

py_test(
    name = "test_cnn_transpose_heads",
    tags = ["team:rllib", "core", "models"],
    size = "medium",
    srcs = ["core/models/tests/test_cnn_transpose_heads.py"]
)

py_test(
    name = "test_mlp_encoders",
    tags = ["team:rllib", "core", "models"],
    size = "medium",
    srcs = ["core/models/tests/test_mlp_encoders.py"]
)

py_test(
    name = "test_mlp_heads",
    tags = ["team:rllib", "core", "models"],
    size = "medium",
    srcs = ["core/models/tests/test_mlp_heads.py"]
)

py_test(
    name = "test_recurrent_encoders",
    tags = ["team:rllib", "core", "models"],
    size = "medium",
    srcs = ["core/models/tests/test_recurrent_encoders.py"]
)

# Specs
py_test(
    name = "test_check_specs",
    tags = ["team:rllib", "models"],
    size = "small",
    srcs = ["core/models/specs/tests/test_check_specs.py"]
)

py_test(
    name = "test_tensor_spec",
    tags = ["team:rllib", "models"],
    size = "small",
    srcs = ["core/models/specs/tests/test_tensor_spec.py"]
)

py_test(
    name = "test_spec_dict",
    tags = ["team:rllib", "models"],
    size = "small",
    srcs = ["core/models/specs/tests/test_spec_dict.py"]
)

# RLModule
py_test(
    name = "test_torch_rl_module",
    tags = ["team:rllib", "core"],
    size = "medium",
    srcs = ["core/rl_module/torch/tests/test_torch_rl_module.py"],
    args = ["TestRLModule"],
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
    tags = ["team:rllib", "core"],
    size = "medium",
    srcs = ["core/rl_module/tf/tests/test_tf_rl_module.py"]
)

py_test(
    name = "test_marl_module",
    tags = ["team:rllib", "core"],
    size = "medium",
    srcs = ["core/rl_module/tests/test_marl_module.py"]
)

py_test(
    name = "test_rl_module_specs",
    tags = ["team:rllib", "core"],
    size = "medium",
    srcs = ["core/rl_module/tests/test_rl_module_specs.py"]
)

# Learner
py_test(
    name = "TestLearnerGroupSyncUpdate",
    main = "core/learner/tests/test_learner_group.py",
    tags = ["team:rllib", "multi_gpu", "exclusive"],
    size = "large",
    srcs = ["core/learner/tests/test_learner_group.py"],
    args = ["TestLearnerGroupSyncUpdate"]
)

py_test(
    name = "TestLearnerGroupCheckpointRestore",
    main = "core/learner/tests/test_learner_group.py",
    tags = ["team:rllib", "multi_gpu", "exclusive"],
    size = "large",
    srcs = ["core/learner/tests/test_learner_group.py"],
    args = ["TestLearnerGroupCheckpointRestore"]
)

py_test(
    name = "TestLearnerGroupAsyncUpdate",
    main = "core/learner/tests/test_learner_group.py",
    tags = ["team:rllib", "multi_gpu", "exclusive"],
    size = "large",
    srcs = ["core/learner/tests/test_learner_group.py"],
    args = ["TestLearnerGroupAsyncUpdate"]
)

py_test(
    name = "TestLearnerGroupSaveLoadState",
    main = "core/learner/tests/test_learner_group.py",
    tags = ["team:rllib", "multi_gpu", "exclusive"],
    size = "large",
    srcs = ["core/learner/tests/test_learner_group.py"],
    args = ["TestLearnerGroupSaveLoadState"]
)

py_test(
    name = "test_learner_group_config",
    tags = ["team:rllib", "core"],
    size = "medium",
    srcs = ["core/learner/tests/test_learner_group_config.py"]
)

py_test(
    name = "test_learner",
    tags = ["team:rllib", "core", "ray_data"],
    size = "medium",
    srcs = ["core/learner/tests/test_learner.py"]
)

py_test(
    name = "test_torch_learner_compile",
    tags = ["team:rllib", "core", "ray_data"],
    size = "medium",
    srcs = ["core/learner/torch/tests/test_torch_learner_compile.py"]
)

py_test(
    name ="tests/test_algorithm_save_load_checkpoint_learner",
    tags = ["team:rllib", "core"],
    size = "medium",
    srcs = ["tests/test_algorithm_save_load_checkpoint_learner.py"]
)

py_test(
    name="test_algorithm_rl_module_restore",
    tags=["team:rllib", "core"],
    size="large",
    srcs=["tests/test_algorithm_rl_module_restore.py"]
)

py_test(
    name = "test_bc_algorithm",
    tags = ["team:rllib", "core"],
    size = "medium",
    srcs = ["core/testing/tests/test_bc_algorithm.py"]
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
    name = "test_conv2d_default_stacks",
    tags = ["team:rllib", "models"],
    size = "small",
    srcs = ["models/tests/test_conv2d_default_stacks.py"]
)

py_test(
    name = "test_convtranspose2d_stack",
    tags = ["team:rllib", "models"],
    size = "medium",
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
    tags = ["team:rllib", "offline", "ray_data"],
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
    tags = ["team:rllib", "policy", "multi_gpu"],
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
    name = "test_minibatch_utils",
    tags = ["team:rllib", "utils"],
    size = "small",
    srcs = ["utils/tests/test_minibatch_utils.py"]
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
    name = "test_random_encoder",
    tags = ["team:rllib", "utils"],
    size = "large",
    srcs = ["utils/exploration/tests/test_random_encoder.py"]
)

py_test(
    name = "utils/tests/test_torch_utils",
    tags = ["team:rllib", "utils", "gpu"],
    size = "medium",
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
    name = "test_episode_replay_buffer",
    tags = ["team:rllib", "utils"],
    size = "small",
    srcs = ["utils/replay_buffers/tests/test_episode_replay_buffer.py"]
)

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
    name = "tests/backward_compat/test_gym_env_apis",
    tags = ["team:rllib", "env"],
    size = "large",
    srcs = ["tests/backward_compat/test_gym_env_apis.py"]
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
    name = "tests/test_checkpoint_restore_ppo",
    main = "tests/test_algorithm_checkpoint_restore.py",
    tags = ["team:rllib", "tests_dir"],
    size = "large",
    srcs = ["tests/test_algorithm_checkpoint_restore.py"],
    args = ["TestCheckpointRestorePPO"]
)


py_test(
    name = "tests/test_checkpoint_restore_ppo_gpu",
    main = "tests/test_algorithm_checkpoint_restore.py",
    tags = ["team:rllib", "tests_dir", "gpu"],
    size = "large",
    srcs = ["tests/test_algorithm_checkpoint_restore.py"],
    args = ["TestCheckpointRestorePPO"]
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
    name = "tests/test_checkpoint_restore_off_policy_gpu",
    main = "tests/test_algorithm_checkpoint_restore.py",
    tags = ["team:rllib", "tests_dir", "gpu"],
    size = "large",
    srcs = ["tests/test_algorithm_checkpoint_restore.py"],
    args = ["TestCheckpointRestoreOffPolicy"]
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
    name = "tests/test_eager_support_policy_gradient",
    main = "tests/test_eager_support.py",
    tags = ["team:rllib", "tests_dir"],
    size = "small",
    srcs = ["tests/test_eager_support.py"],
    args = ["TestEagerSupportPolicyGradient"]
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
    size = "large",
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
    name = "tests/test_supported_multi_agent_multi_gpu",
    main = "tests/test_supported_multi_agent.py",
    tags = ["team:rllib", "tests_dir", "multi_gpu"],
    size = "medium",
    srcs = ["tests/test_supported_multi_agent.py"],
    args = ["TestSupportedMultiAgentMultiGPU"]
)


py_test(
    name = "tests/test_supported_multi_agent_policy_gradient",
    main = "tests/test_supported_multi_agent.py",
    tags = ["team:rllib", "tests_dir"],
    size = "large",
    srcs = ["tests/test_supported_multi_agent.py"],
    args = ["TestSupportedMultiAgentPolicyGradient"]
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
     name = "tests/test_supported_spaces_appo",
     main = "tests/test_supported_spaces.py",
     tags = ["team:rllib", "tests_dir", "exclusive"],
     size = "large",
     srcs = ["tests/test_supported_spaces.py"],
     args = ["TestSupportedSpacesAPPO"]
 )

py_test(
     name = "tests/test_supported_spaces_impala",
     main = "tests/test_supported_spaces.py",
     tags = ["team:rllib", "tests_dir", "exclusive"],
     size = "large",
     srcs = ["tests/test_supported_spaces.py"],
     args = ["TestSupportedSpacesIMPALA"]
)

py_test(
     name = "tests/test_supported_spaces_ppo",
     main = "tests/test_supported_spaces.py",
     tags = ["team:rllib", "tests_dir"],
     size = "large",
     srcs = ["tests/test_supported_spaces.py"],
     args = ["TestSupportedSpacesPPO"]
 )

py_test(
    name="tests/test_supported_spaces_dqn",
    main="tests/test_supported_spaces.py",
    tags=["team:rllib", "tests_dir"],
    size="large",
    srcs=["tests/test_supported_spaces.py"],
    args=["TestSupportedSpacesDQN"]
)

py_test(
     name = "tests/test_supported_spaces_ppo_no_preprocessor_gpu",
     main = "tests/test_supported_spaces.py",
     tags = ["team:rllib", "gpu", "no_cpu"],
     size = "large",
     srcs = ["tests/test_supported_spaces.py"],
     args = ["TestSupportedSpacesPPONoPreprocessorGPU"]
 )

py_test(
    name = "tests/test_supported_spaces_off_policy",
    main = "tests/test_supported_spaces.py",
    tags = ["team:rllib", "tests_dir", "exclusive"],
    size = "medium",
    srcs = ["tests/test_supported_spaces.py"],
    args = ["TestSupportedSpacesOffPolicy"]
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
    name = "examples/action_masking_tf2",
    main = "examples/action_masking.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "small",
    srcs = ["examples/action_masking.py"],
    args = ["--stop-iter=2", "--framework=tf2"]
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
    args = ["--as-test", "--stop-reward=70", "--framework=tf"]
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
    args = ["--as-test", "--framework=tf", "--stop-reward=150", "--num-cpus=4"]
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
    name = "examples/cartpole_lstm_impala_tf2",
    main = "examples/cartpole_lstm.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/cartpole_lstm.py"],
    args = ["--run=IMPALA", "--as-test", "--framework=tf2", "--stop-reward=28", "--num-cpus=4"]
)

py_test(
    name = "examples/cartpole_lstm_impala_torch",
    main = "examples/cartpole_lstm.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/cartpole_lstm.py"],
    args = ["--run=IMPALA", "--as-test", "--framework=torch", "--stop-reward=28", "--num-cpus=4"]
)


# TODO (Kourosh): tf2 ~5x slower compared to torch on the new stack
py_test(
    name = "examples/cartpole_lstm_ppo_tf2",
    main = "examples/cartpole_lstm.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "large",
    srcs = ["examples/cartpole_lstm.py"],
    args = ["--run=PPO", "--as-test", "--framework=tf2", "--stop-reward=28", "--num-cpus=4"]
)

py_test(
    name = "examples/cartpole_lstm_ppo_torch",
    main = "examples/cartpole_lstm.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/cartpole_lstm.py"],
    args = ["--run=PPO", "--as-test", "--framework=torch", "--stop-reward=28", "--num-cpus=4"]
)

py_test(
    name = "examples/cartpole_lstm_ppo_torch_with_prev_a_and_r",
    main = "examples/cartpole_lstm.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/cartpole_lstm.py"],
    args = ["--run=PPO", "--as-test", "--framework=torch", "--stop-reward=28", "--num-cpus=4", "--use-prev-action",  "--use-prev-reward"]
)

py_test(
    name = "examples/catalog/custom_action_distribution",
    main = "examples/catalog/custom_action_distribution.py",
    tags = ["team:rllib", "examples", "no_main"],
    size = "small",
    srcs = ["examples/catalog/custom_action_distribution.py"],
)


py_test(
    name = "examples/catalog/mobilenet_v2_encoder",
    main = "examples/catalog/mobilenet_v2_encoder.py",
    tags = ["team:rllib", "examples", "no_main"],
    size = "small",
    srcs = ["examples/catalog/mobilenet_v2_encoder.py"],
)


py_test(
    name = "examples/rl_module/mobilenet_rlm",
    main = "examples/rl_module/mobilenet_rlm.py",
    tags = ["team:rllib", "examples", "no_main"],
    size = "small",
    srcs = ["examples/rl_module/mobilenet_rlm.py"],
)

py_test(
    name = "examples/centralized_critic_tf",
    main = "examples/centralized_critic.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/centralized_critic.py"],
    args = ["--as-test", "--framework=tf", "--stop-reward=7.2"]
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
    args = ["--as-test", "--framework=tf", "--stop-reward=6.0"]
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
    args = ["--as-test", "--framework=tf"]
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
    args = ["--num-cpus=4", "--framework=tf", "--as-test"]
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
    name = "examples/custom_metrics_and_callbacks",
    main = "examples/custom_metrics_and_callbacks.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "small",
    srcs = ["examples/custom_metrics_and_callbacks.py"],
    args = ["--stop-iters=2"]
)

py_test(
    name = "examples/custom_model_loss_and_metrics_ppo_tf",
    main = "examples/custom_model_loss_and_metrics.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "small",
    # Include the json data file.
    data = ["tests/data/cartpole/small.json"],
    srcs = ["examples/custom_model_loss_and_metrics.py"],
    args = ["--run=PPO", "--stop-iters=1", "--framework=tf","--input-files=tests/data/cartpole"]
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
    args = ["--stop-iters=1", "--framework=tf", "--input-files=tests/data/cartpole"]
)

py_test(
    name = "examples/custom_model_loss_and_metrics_pg_torch",
    main = "examples/custom_model_loss_and_metrics.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "small",
    # Include the json data file.
    data = ["tests/data/cartpole/small.json"],
    srcs = ["examples/custom_model_loss_and_metrics.py"],
    args = ["--framework=torch", "--stop-iters=1", "--input-files=tests/data/cartpole"]
)

py_test(
    name = "examples/custom_recurrent_rnn_tokenizer_repeat_after_me_tf2",
    main = "examples/custom_recurrent_rnn_tokenizer.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/custom_recurrent_rnn_tokenizer.py"],
    args = ["--as-test", "--framework=tf2", "--stop-reward=40", "--env=RepeatAfterMeEnv", "--num-cpus=4"]
)

py_test(
    name = "examples/custom_recurrent_rnn_tokenizer_repeat_initial_obs_env_tf2",
    main = "examples/custom_recurrent_rnn_tokenizer.py",
    tags = ["team:rllib", "examples"],
    size = "medium",
    srcs = ["examples/custom_recurrent_rnn_tokenizer.py"],
    args = ["--as-test", "--framework=tf2", "--stop-reward=10", "--stop-timesteps=300000", "--env=RepeatInitialObsEnv", "--num-cpus=4"]
)

py_test(
    name = "examples/custom_recurrent_rnn_tokenizer_repeat_after_me_torch",
    main = "examples/custom_recurrent_rnn_tokenizer.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/custom_recurrent_rnn_tokenizer.py"],
    args = ["--as-test", "--framework=torch", "--stop-reward=40", "--env=RepeatAfterMeEnv", "--num-cpus=4"]
)

py_test(
    name = "examples/custom_recurrent_rnn_tokenizer_repeat_initial_obs_env_torch",
    main = "examples/custom_recurrent_rnn_tokenizer.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/custom_recurrent_rnn_tokenizer.py"],
    args = ["--as-test", "--framework=torch", "--stop-reward=10", "--stop-timesteps=300000", "--env=RepeatInitialObsEnv", "--num-cpus=4"]
)

py_test(
    name = "examples/custom_train_fn",
    main = "examples/custom_train_fn.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/custom_train_fn.py"],
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
    name = "examples/env/greyscale_env",
    tags = ["team:rllib", "examples", "no_main"],
    size = "medium",
    srcs = ["examples/env/greyscale_env.py"],
    args = ["--stop-iters=1 --as-test --framework torch"]
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
    args = [ "--framework=tf", "--stop-reward=0.0"]
)

py_test(
    name = "examples/hierarchical_training_torch",
    main = "examples/hierarchical_training.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/hierarchical_training.py"],
    args = ["--framework=torch", "--stop-reward=0.0"]
)

py_test(
    name = "examples/multi_agent_cartpole_tf",
    main = "examples/multi_agent_cartpole.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "small",
    srcs = ["examples/multi_agent_cartpole.py"],
    args = ["--as-test", "--framework=tf", "--stop-reward=70.0", "--num-cpus=4"]
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
    name = "examples/multi_agent_cartpole_w_rlm_torch",
    main = "examples/multi_agent_cartpole.py",
    tags = ["team:rllib", "exclusive", "examples", "rlm"],
    size = "medium",
    srcs = ["examples/multi_agent_cartpole.py"],
    args = ["--as-test", "--framework=torch", "--stop-reward=70.0", "--num-cpus=4"]
)

py_test(
    name = "examples/multi_agent_cartpole_w_rlm_tf2",
    main = "examples/multi_agent_cartpole.py",
    tags = ["team:rllib", "exclusive", "examples", "rlm"],
    size = "medium",
    srcs = ["examples/multi_agent_cartpole.py"],
    args = ["--as-test", "--framework=tf2", "--stop-reward=70.0", "--num-cpus=4"]
)

py_test(
    name = "examples/multi_agent_custom_policy_tf",
    main = "examples/multi_agent_custom_policy.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/multi_agent_custom_policy.py"],
    args = ["--as-test", "--framework=tf", "--stop-reward=80"]
)

py_test(
    name = "examples/multi_agent_custom_policy_torch",
    main = "examples/multi_agent_custom_policy.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/multi_agent_custom_policy.py"],
    args = ["--as-test", "--framework=torch", "--stop-reward=80"]
)

py_test(
    name = "examples/multi_agent_custom_policy_w_rlm_torch",
    main = "examples/multi_agent_custom_policy.py",
    tags = ["team:rllib", "exclusive", "examples", "rlm"],
    size = "medium",
    srcs = ["examples/multi_agent_custom_policy.py"],
    args = ["--as-test", "--framework=torch", "--stop-reward=80"]
)

py_test(
    name = "examples/multi_agent_custom_policy_w_rlm_tf2",
    main = "examples/multi_agent_custom_policy.py",
    tags = ["team:rllib", "exclusive", "examples", "rlm"],
    size = "medium",
    srcs = ["examples/multi_agent_custom_policy.py"],
    args = ["--as-test", "--framework=tf2", "--stop-reward=80"]
)

py_test(
    name = "examples/multi_agent_different_spaces_for_agents_tf2",
    main = "examples/multi_agent_different_spaces_for_agents.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/multi_agent_different_spaces_for_agents.py"],
    args = ["--stop-iters=4", "--framework=tf2"]
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
    name = "examples/multi_agent_different_spaces_for_agents_w_rlm_torch",
    main = "examples/multi_agent_different_spaces_for_agents.py",
    tags = ["team:rllib", "exclusive", "examples", "rlm"],
    size = "medium",
    srcs = ["examples/multi_agent_different_spaces_for_agents.py"],
    args = ["--stop-iters=4", "--framework=torch"]
)

py_test(
    name = "examples/multi_agent_different_spaces_for_agents_w_rlm_tf2",
    main = "examples/multi_agent_different_spaces_for_agents.py",
    tags = ["team:rllib", "exclusive", "examples", "rlm"],
    size = "medium",
    srcs = ["examples/multi_agent_different_spaces_for_agents.py"],
    args = ["--stop-iters=4", "--framework=tf2"]
)

py_test(
    name = "examples/multi_agent_independent_learning",
    main = "examples/multi_agent_independent_learning.py",
    tags = ["team:rllib", "examples"],
    size = "medium",
    srcs = ["examples/multi_agent_independent_learning.py"],
    args = ["--num-gpus=0", "--as-test"]
)

py_test(
    name = "examples/multi_agent_two_trainers_tf",
    main = "examples/multi_agent_two_trainers.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/multi_agent_two_trainers.py"],
    args = ["--as-test", "--framework=tf", "--stop-reward=70"]
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

py_test(
    name = "examples/nested_action_spaces_ppo_tf",
    main = "examples/nested_action_spaces.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/nested_action_spaces.py"],
    args = ["--as-test", "--framework=tf", "--stop-reward=-600", "--run=PPO"]
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
    args = ["--as-test", "--framework=tf", "--stop-reward=50.0", "--num-cpus=6", "--evaluation-duration=13"]
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
    name = "examples/parametric_actions_cartpole_dqn_tf",
    main = "examples/parametric_actions_cartpole.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/parametric_actions_cartpole.py"],
    args = ["--as-test", "--framework=tf", "--stop-reward=60.0", "--run=DQN"]
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
    name = "examples/rock_paper_scissors_multiagent_tf",
    main = "examples/rock_paper_scissors_multiagent.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/rock_paper_scissors_multiagent.py"],
    args = ["--as-test", "--framework=tf"],
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
    args = ["--framework=tf", "--env=connect_four", "--win-rate-threshold=0.9", "--num-episodes-human-play=0", "--as-test", "--min-win-rate=0.6"]
)

py_test(
    name = "examples/self_play_with_open_spiel_connect_4_torch",
    main = "examples/self_play_with_open_spiel.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/self_play_with_open_spiel.py"],
    args = ["--framework=torch", "--env=connect_four", "--win-rate-threshold=0.9", "--num-episodes-human-play=0", "--as-test", "--min-win-rate=0.6"]
)

py_test(
    name = "examples/self_play_with_open_spiel_connect_4_w_rlm_torch",
    main = "examples/self_play_with_open_spiel.py",
    tags = ["team:rllib", "exclusive", "examples", "rlm"],
    size = "medium",
    srcs = ["examples/self_play_with_open_spiel.py"],
    args = ["--framework=torch", "--env=connect_four", "--win-rate-threshold=0.9", "--num-episodes-human-play=0", "--as-test", "--min-win-rate=0.6"]
)

py_test(
    name = "examples/self_play_with_open_spiel_connect_4_w_rlm_tf2",
    main = "examples/self_play_with_open_spiel.py",
    tags = ["team:rllib", "exclusive", "examples", "rlm"],
    size = "medium",
    srcs = ["examples/self_play_with_open_spiel.py"],
    args = ["--framework=tf2", "--env=connect_four", "--win-rate-threshold=0.9", "--num-episodes-human-play=0", "--as-test", "--min-win-rate=0.6"]
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
    name = "examples/two_step_game_ppo_tf",
    main = "examples/two_step_game.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/two_step_game.py"],
    args = ["--as-test", "--framework=tf", "--stop-reward=7"]
)

py_test(
    name = "examples/two_step_game_ppo_torch",
    main = "examples/two_step_game.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/two_step_game.py"],
    args = ["--as-test", "--framework=torch", "--stop-reward=7"]
)

py_test(
    name = "examples/connectors/run_connector_policy",
    main = "examples/connectors/run_connector_policy.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "small",
    srcs = ["examples/connectors/run_connector_policy.py"],
)

py_test(
    name = "examples/connectors/adapt_connector_policy",
    main = "examples/connectors/adapt_connector_policy.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "small",
    srcs = ["examples/connectors/adapt_connector_policy.py"],
)

py_test(
    name = "examples/connectors/self_play_with_policy_checkpoint",
    main = "examples/connectors/self_play_with_policy_checkpoint.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "small",
    srcs = ["examples/connectors/self_play_with_policy_checkpoint.py"],
    args = [
        "--train_iteration=1"  # Smoke test.
    ]
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
    main = "examples/learner/multi_agent_cartpole_ppo.py",
    tags = ["team:rllib", "examples"],
    size = "medium",
    srcs = ["examples/learner/multi_agent_cartpole_ppo.py"],
    args = ["--as-test", "--framework=torch", "--num-gpus=0"]
)

py_test(
    name = "examples/learner/multi_agent_cartpole_ppo_torch_gpu",
    main = "examples/learner/multi_agent_cartpole_ppo.py",
    tags = ["team:rllib", "exclusive", "examples", "gpu"],
    size = "medium",
    srcs = ["examples/learner/multi_agent_cartpole_ppo.py"],
    args = ["--as-test", "--framework=torch", "--num-gpus=1"]
)


py_test(
    name = "examples/learner/multi_agent_cartpole_ppo_torch_multi_gpu",
    main = "examples/learner/multi_agent_cartpole_ppo.py",
    tags = ["team:rllib", "exclusive", "examples", "multi_gpu"],
    size = "medium",
    srcs = ["examples/learner/multi_agent_cartpole_ppo.py"],
    args = ["--as-test", "--framework=torch", "--num-gpus=2"]
)

py_test(
    name = "examples/learner/ppo_tuner_local_cpu_torch",
    main = "examples/learner/ppo_tuner.py",
    tags = ["team:rllib", "examples"],
    size = "medium",
    srcs = ["examples/learner/ppo_tuner.py"],
    args = ["--framework=torch", "--config=local-cpu"]
)

py_test(
    name = "examples/learner/ppo_tuner_local_cpu_tf2",
    main = "examples/learner/ppo_tuner.py",
    tags = ["team:rllib", "examples"],
    size = "medium",
    srcs = ["examples/learner/ppo_tuner.py"],
    args = ["--framework=tf2", "--config=local-cpu"]
)

py_test(
    name = "examples/learner/ppo_tuner_local_gpu_torch",
    main = "examples/learner/ppo_tuner.py",
    tags = ["team:rllib", "examples", "gpu"],
    size = "medium",
    srcs = ["examples/learner/ppo_tuner.py"],
    args = ["--framework=torch", "--config=local-gpu"]
)

py_test(
    name = "examples/learner/ppo_tuner_local_gpu_tf2",
    main = "examples/learner/ppo_tuner.py",
    tags = ["team:rllib", "examples", "gpu", "exclusive"],
    size = "medium",
    srcs = ["examples/learner/ppo_tuner.py"],
    args = ["--framework=tf2", "--config=local-gpu"]
)

py_test(
    name = "examples/learner/ppo_tuner_remote_cpu_torch",
    main = "examples/learner/ppo_tuner.py",
    tags = ["team:rllib", "examples"],
    size = "medium",
    srcs = ["examples/learner/ppo_tuner.py"],
    args = ["--framework=torch", "--config=remote-cpu"]
)

py_test(
    name = "examples/learner/ppo_tuner_remote_cpu_tf2",
    main = "examples/learner/ppo_tuner.py",
    tags = ["team:rllib", "examples"],
    size = "medium",
    srcs = ["examples/learner/ppo_tuner.py"],
    args = ["--framework=tf2", "--config=remote-cpu"]
)

py_test(
    name = "examples/learner/ppo_tuner_remote_gpu_torch",
    main = "examples/learner/ppo_tuner.py",
    tags = ["team:rllib", "examples", "gpu", "exclusive"],
    size = "medium",
    srcs = ["examples/learner/ppo_tuner.py"],
    args = ["--framework=torch", "--config=remote-gpu"]
)

py_test(
    name = "examples/learner/ppo_tuner_remote_gpu_tf2",
    main = "examples/learner/ppo_tuner.py",
    tags = ["team:rllib", "examples", "gpu", "exclusive"],
    size = "medium",
    srcs = ["examples/learner/ppo_tuner.py"],
    args = ["--framework=tf2", "--config=remote-gpu"]
)

py_test(
    name = "examples/learner/ppo_tuner_multi_gpu_torch",
    main = "examples/learner/ppo_tuner.py",
    tags = ["team:rllib", "examples", "multi_gpu", "exclusive"],
    size = "medium",
    srcs = ["examples/learner/ppo_tuner.py"],
    args = ["--framework=torch", "--config=multi-gpu-ddp"]
)

py_test(
    name = "examples/learner/ppo_tuner_multi_gpu_tf2",
    main = "examples/learner/ppo_tuner.py",
    tags = ["team:rllib", "examples", "multi_gpu", "exclusive"],
    size = "medium",
    srcs = ["examples/learner/ppo_tuner.py"],
    args = ["--framework=tf2", "--config=multi-gpu-ddp"]
)

py_test(
    name = "examples/learner/train_w_bc_finetune_w_ppo",
    main = "examples/learner/train_w_bc_finetune_w_ppo.py",
    tags = ["team:rllib", "examples", "exclusive"],
    size = "medium",
    srcs = ["examples/learner/train_w_bc_finetune_w_ppo.py"],
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
    size = "large",
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
    "env/wrappers/tests/test_kaggle_wrapper.py",
    "env/tests/test_multi_agent_episode.py",
    "examples/env/tests/test_cliff_walking_wall_env.py",
    "examples/env/tests/test_coin_game_non_vectorized_env.py",
    "examples/env/tests/test_coin_game_vectorized_env.py",
    "examples/env/tests/test_matrix_sequential_social_dilemma.py",
    "examples/env/tests/test_wrappers.py",
    "tests/test_dnc.py",
    "tests/test_perf.py",
    "utils/tests/test_utils.py",
  ],
  size = "large",
  extra_srcs = [],
  deps = [],
  tags = ["manual", "team:rllib", "no_main"],
)
