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
            # Exclude `tuned_examples` *.py files.
            "**/tuned_examples/**",
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
# This will test python/yaml config files
# inside rllib/tuned_examples/[algo-name] for actual learning success.
# --------------------------------------------------------------------

# APPO
# CartPole
py_test(
    name = "learning_tests_cartpole_appo",
    main = "tuned_examples/appo/cartpole_appo.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_discrete", "torch_only"],
    size = "large",
    srcs = ["tuned_examples/appo/cartpole_appo.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-gpus=1"]
)
py_test(
    name = "learning_tests_cartpole_appo_gpu",
    main = "tuned_examples/appo/cartpole_appo.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_discrete", "learning_tests_pytorch_use_all_core", "gpu"],
    size = "large",
    srcs = ["tuned_examples/appo/cartpole_appo.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-gpus=1"]
)
py_test(
    name = "learning_tests_cartpole_appo_multi_cpu",
    main = "tuned_examples/appo/cartpole_appo.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_discrete", "learning_tests_pytorch_use_all_core"],
    size = "large",
    srcs = ["tuned_examples/appo/cartpole_appo.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-gpus=2"]
)
py_test(
    name = "learning_tests_cartpole_appo_multi_gpu",
    main = "tuned_examples/appo/cartpole_appo.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_discrete", "learning_tests_pytorch_use_all_core", "multi_gpu"],
    size = "large",
    srcs = ["tuned_examples/appo/cartpole_appo.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-gpus=2"]
)
# MultiAgentCartPole
py_test(
    name = "learning_tests_multi_agent_cartpole_appo",
    main = "tuned_examples/appo/multi_agent_cartpole_appo.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_discrete", "torch_only"],
    size = "large",
    srcs = ["tuned_examples/appo/multi_agent_cartpole_appo.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-agents=2", "--num-gpus=1"]
)
py_test(
    name = "learning_tests_multi_agent_cartpole_appo_gpu",
    main = "tuned_examples/appo/multi_agent_cartpole_appo.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_discrete", "learning_tests_pytorch_use_all_core", "gpu"],
    size = "large",
    srcs = ["tuned_examples/appo/multi_agent_cartpole_appo.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-agents=2", "--num-gpus=1", "--num-cpus=6"]
)
py_test(
    name = "learning_tests_multi_agent_cartpole_appo_multi_cpu",
    main = "tuned_examples/appo/multi_agent_cartpole_appo.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_discrete", "learning_tests_pytorch_use_all_core"],
    size = "large",
    srcs = ["tuned_examples/appo/multi_agent_cartpole_appo.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-agents=2", "--num-gpus=2", "--num-cpus=7"]
)
py_test(
    name = "learning_tests_multi_agent_cartpole_appo_multi_gpu",
    main = "tuned_examples/appo/multi_agent_cartpole_appo.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_discrete", "learning_tests_pytorch_use_all_core", "multi_gpu"],
    size = "large",
    srcs = ["tuned_examples/appo/multi_agent_cartpole_appo.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-agents=2", "--num-gpus=2", "--num-cpus=7"]
)
# StatelessCartPole
py_test(
    name = "learning_tests_stateless_cartpole_appo",
    main = "tuned_examples/appo/stateless_cartpole_appo.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_discrete", "learning_tests_pytorch_use_all_core"],
    size = "large",
    srcs = ["tuned_examples/appo/stateless_cartpole_appo.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-gpus=1"]
)
py_test(
    name = "learning_tests_stateless_cartpole_appo_gpu",
    main = "tuned_examples/appo/stateless_cartpole_appo.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_discrete", "learning_tests_pytorch_use_all_core", "gpu"],
    size = "large",
    srcs = ["tuned_examples/appo/stateless_cartpole_appo.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-agents=2", "--num-gpus=1"]
)
py_test(
    name = "learning_tests_stateless_cartpole_appo_multi_cpu",
    main = "tuned_examples/appo/stateless_cartpole_appo.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_discrete", "learning_tests_pytorch_use_all_core"],
    size = "large",
    srcs = ["tuned_examples/appo/stateless_cartpole_appo.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-gpus=2"]
)
py_test(
    name = "learning_tests_stateless_cartpole_appo_multi_gpu",
    main = "tuned_examples/appo/stateless_cartpole_appo.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_discrete", "learning_tests_pytorch_use_all_core", "multi_gpu"],
    size = "large",
    srcs = ["tuned_examples/appo/stateless_cartpole_appo.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-gpus=2"]
)
# MultiAgentStatelessCartPole
py_test(
    name = "learning_tests_multi_agent_stateless_cartpole_appo",
    main = "tuned_examples/appo/multi_agent_stateless_cartpole_appo.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_discrete", "learning_tests_pytorch_use_all_core"],
    size = "large",
    srcs = ["tuned_examples/appo/multi_agent_stateless_cartpole_appo.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-gpus=1"]
)
py_test(
    name = "learning_tests_multi_agent_stateless_cartpole_appo_gpu",
    main = "tuned_examples/appo/multi_agent_stateless_cartpole_appo.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_discrete", "learning_tests_pytorch_use_all_core", "gpu"],
    size = "large",
    srcs = ["tuned_examples/appo/multi_agent_stateless_cartpole_appo.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-agents=2", "--num-gpus=1"]
)
py_test(
    name = "learning_tests_multi_agent_stateless_cartpole_appo_multi_cpu",
    main = "tuned_examples/appo/multi_agent_stateless_cartpole_appo.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_discrete", "learning_tests_pytorch_use_all_core"],
    size = "enormous",
    srcs = ["tuned_examples/appo/multi_agent_stateless_cartpole_appo.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-gpus=2"]
)
py_test(
    name = "learning_tests_multi_agent_stateless_cartpole_appo_multi_gpu",
    main = "tuned_examples/appo/multi_agent_stateless_cartpole_appo.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_discrete", "learning_tests_pytorch_use_all_core", "multi_gpu"],
    size = "enormous",
    srcs = ["tuned_examples/appo/multi_agent_stateless_cartpole_appo.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-gpus=2"]
)

#@OldAPIStack
py_test(
    name = "learning_tests_cartpole_separate_losses_appo_old_api_stack",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "torch_only", "exclusive", "learning_tests", "learning_tests_discrete"],
    size = "medium",
    srcs = ["tests/run_regression_tests.py"],
    data = [
        "tuned_examples/appo/cartpole-appo-separate-losses.py"
    ],
    args = ["--dir=tuned_examples/appo"]
)

#@OldAPIStack
py_test(
    name = "learning_tests_multi_agent_cartpole_appo_old_api_stack",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_discrete"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/appo/multi_agent_cartpole_appo_old_api_stack.py"],
    args = ["--dir=tuned_examples/appo"]
)

#@OldAPIStack
py_test(
    name = "learning_tests_multi_agent_cartpole_w_100_policies_appo_old_api_stack",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_discrete", "learning_tests_pytorch_use_all_core"],
    size = "enormous",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/appo/multi-agent-cartpole-w-100-policies-appo.py"],
    args = ["--dir=tuned_examples/appo"]
)

# BC
# CartPole
py_test(
    name = "learning_tests_cartpole_bc",
    main = "tuned_examples/bc/cartpole_bc.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_discrete", "learning_tests_pytorch_use_all_core"],
    size = "medium",
    srcs = ["tuned_examples/bc/cartpole_bc.py"],
    # Include the zipped json data file as well.
    data = [
        "tests/data/cartpole/cartpole-v1_large",
    ],
    args = ["--as-test", "--enable-new-api-stack"]
)

# CQL
#@OldAPIStack
py_test(
    name = "learning_tests_pendulum_cql_old_api_stack",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_continuous", "learning_tests_with_ray_data", "torch_only"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    # Include the zipped json data file as well.
    data = [
        "tuned_examples/cql/pendulum-cql.yaml",
        "tests/data/pendulum/enormous.zip",
    ],
    args = ["--dir=tuned_examples/cql"]
)

py_test(
    name = "learning_tests_pendulum_cql",
    main = "tuned_examples/cql/pendulum_cql.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_cartpole", "learning_tests_discrete", "learning_tests_pytorch_use_all_core"],
    size = "large",
    srcs = ["tuned_examples/cql/pendulum_cql.py"],
    # Include the zipped json data file as well.
    data = [
        "tests/data/pendulum/pendulum-v1_enormous",
    ],
    args = ["--as-test", "--enable-new-api-stack"]
)

# DQN
# CartPole
py_test(
    name = "learning_tests_cartpole_dqn",
    main = "tuned_examples/dqn/cartpole_dqn.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_discrete", "learning_tests_pytorch_use_all_core"],
    size = "large",
    srcs = ["tuned_examples/dqn/cartpole_dqn.py"],
    args = ["--as-test", "--enable-new-api-stack"]
)
py_test(
    name = "learning_tests_cartpole_dqn_gpu",
    main = "tuned_examples/dqn/cartpole_dqn.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_discrete", "learning_tests_pytorch_use_all_core", "gpu"],
    size = "large",
    srcs = ["tuned_examples/dqn/cartpole_dqn.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-gpus=1"]
)
py_test(
    name = "learning_tests_cartpole_dqn_multi_cpu",
    main = "tuned_examples/dqn/cartpole_dqn.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_discrete", "learning_tests_pytorch_use_all_core"],
    size = "large",
    srcs = ["tuned_examples/dqn/cartpole_dqn.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-gpus=2"]
)
py_test(
    name = "learning_tests_cartpole_dqn_multi_gpu",
    main = "tuned_examples/dqn/cartpole_dqn.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_discrete", "learning_tests_pytorch_use_all_core", "multi_gpu"],
    size = "large",
    srcs = ["tuned_examples/dqn/cartpole_dqn.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-gpus=2"]
)
# MultiAgentCartPole
py_test(
    name = "learning_tests_multi_agent_cartpole_dqn",
    main = "tuned_examples/dqn/multi_agent_cartpole_dqn.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_discrete", "learning_tests_pytorch_use_all_core"],
    size = "large",
    srcs = ["tuned_examples/dqn/multi_agent_cartpole_dqn.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-agents=2", "--num-cpus=4"]
)
py_test(
    name = "learning_tests_multi_agent_cartpole_dqn_gpu",
    main = "tuned_examples/dqn/multi_agent_cartpole_dqn.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_discrete", "learning_tests_pytorch_use_all_core", "gpu"],
    size = "large",
    srcs = ["tuned_examples/dqn/multi_agent_cartpole_dqn.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-agents=2", "--num-cpus=4", "--num-gpus=1"]
)
py_test(
    name = "learning_tests_multi_agent_cartpole_dqn_multi_cpu",
    main = "tuned_examples/dqn/multi_agent_cartpole_dqn.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_discrete", "learning_tests_pytorch_use_all_core"],
    size = "large",
    srcs = ["tuned_examples/dqn/multi_agent_cartpole_dqn.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-agents=2", "--num-cpus=5", "--num-gpus=2"]
)
py_test(
    name = "learning_tests_multi_agent_cartpole_dqn_multi_gpu",
    main = "tuned_examples/dqn/multi_agent_cartpole_dqn.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_discrete", "learning_tests_pytorch_use_all_core", "multi_gpu"],
    size = "large",
    srcs = ["tuned_examples/dqn/multi_agent_cartpole_dqn.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-agents=2", "--num-cpus=4", "--num-gpus=2"]
)

# IMPALA
# CartPole
py_test(
    name = "learning_tests_cartpole_impala",
    main = "tuned_examples/impala/cartpole_impala.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_discrete", "torch_only"],
    size = "large",
    srcs = ["tuned_examples/impala/cartpole_impala.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-gpus=1"]
)
py_test(
    name = "learning_tests_cartpole_impala_gpu",
    main = "tuned_examples/impala/cartpole_impala.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_discrete", "learning_tests_pytorch_use_all_core", "gpu"],
    size = "large",
    srcs = ["tuned_examples/impala/cartpole_impala.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-gpus=1"]
)
py_test(
    name = "learning_tests_cartpole_impala_multi_cpu",
    main = "tuned_examples/impala/cartpole_impala.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_discrete", "learning_tests_pytorch_use_all_core"],
    size = "large",
    srcs = ["tuned_examples/impala/cartpole_impala.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-gpus=2"]
)
py_test(
    name = "learning_tests_cartpole_impala_multi_gpu",
    main = "tuned_examples/impala/cartpole_impala.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_discrete", "learning_tests_pytorch_use_all_core", "multi_gpu"],
    size = "large",
    srcs = ["tuned_examples/impala/cartpole_impala.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-gpus=2"]
)
# MultiAgentCartPole
py_test(
    name = "learning_tests_multi_agent_cartpole_impala",
    main = "tuned_examples/impala/multi_agent_cartpole_impala.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_discrete", "learning_tests_pytorch_use_all_core", "torch_only"],
    size = "large",
    srcs = ["tuned_examples/impala/multi_agent_cartpole_impala.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-agents=2", "--num-gpus=1", "--num-cpus=6"]
)
py_test(
    name = "learning_tests_multi_agent_cartpole_impala_gpu",
    main = "tuned_examples/impala/multi_agent_cartpole_impala.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_discrete", "learning_tests_pytorch_use_all_core", "gpu"],
    size = "large",
    srcs = ["tuned_examples/impala/multi_agent_cartpole_impala.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-agents=2", "--num-gpus=1", "--num-cpus=6"]
)
py_test(
    name = "learning_tests_multi_agent_cartpole_impala_multi_cpu",
    main = "tuned_examples/impala/multi_agent_cartpole_impala.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_discrete", "learning_tests_pytorch_use_all_core"],
    size = "enormous",
    srcs = ["tuned_examples/impala/multi_agent_cartpole_impala.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-agents=2", "--num-gpus=2", "--num-cpus=7"]
)
py_test(
    name = "learning_tests_multi_agent_cartpole_impala_multi_gpu",
    main = "tuned_examples/impala/multi_agent_cartpole_impala.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_discrete", "learning_tests_pytorch_use_all_core", "multi_gpu"],
    size = "large",
    srcs = ["tuned_examples/impala/multi_agent_cartpole_impala.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-agents=2", "--num-gpus=2", "--num-cpus=7"]
)
# StatelessCartPole
py_test(
    name = "learning_tests_stateless_cartpole_impala",
    main = "tuned_examples/impala/stateless_cartpole_impala.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_discrete", "learning_tests_pytorch_use_all_core"],
    size = "large",
    srcs = ["tuned_examples/impala/stateless_cartpole_impala.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-gpus=1"]
)
py_test(
    name = "learning_tests_stateless_cartpole_impala_multi_gpu",
    main = "tuned_examples/impala/stateless_cartpole_impala.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_discrete", "learning_tests_pytorch_use_all_core", "multi_gpu"],
    size = "large",
    srcs = ["tuned_examples/impala/stateless_cartpole_impala.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-gpus=2"]
)
# MultiAgentStatelessCartPole
py_test(
    name = "learning_tests_multi_agent_stateless_cartpole_impala",
    main = "tuned_examples/impala/multi_agent_stateless_cartpole_impala.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_discrete", "learning_tests_pytorch_use_all_core"],
    size = "large",
    srcs = ["tuned_examples/impala/multi_agent_stateless_cartpole_impala.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-gpus=1"]
)
py_test(
    name = "learning_tests_multi_agent_stateless_cartpole_impala_multi_gpu",
    main = "tuned_examples/impala/multi_agent_stateless_cartpole_impala.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_discrete", "learning_tests_pytorch_use_all_core", "multi_gpu"],
    size = "large",
    srcs = ["tuned_examples/impala/multi_agent_stateless_cartpole_impala.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-gpus=2"]
)

#@OldAPIstack
py_test(
    name = "learning_tests_cartpole_separate_losses_impala_old_api_stack",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_discrete"],
    size = "medium",
    srcs = ["tests/run_regression_tests.py"],
    data = [
        "tuned_examples/impala/cartpole-impala-separate-losses.py"
    ],
    args = ["--dir=tuned_examples/impala"]
)
#@OldAPIStack
py_test(
    name = "learning_tests_multi_agent_cartpole_impala_old_api_stack",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_discrete"],
    size = "medium",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/impala/multi_agent_cartpole_impala_old_api_stack.py"],
    args = ["--dir=tuned_examples/impala"]
)

# MARWIL
# CartPole
py_test(
    name = "learning_tests_cartpole_marwil",
    main = "tuned_examples/marwil/cartpole_marwil.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_discrete", "learning_tests_pytorch_use_all_core"],
    size = "large",
    srcs = ["tuned_examples/marwil/cartpole_marwil.py"],
    # Include the zipped json data file as well.
    data = [
        "tests/data/cartpole/cartpole-v1_large",
    ],
    args = ["--as-test", "--enable-new-api-stack"]
)

# PPO
# CartPole
py_test(
    name = "learning_tests_cartpole_ppo",
    main = "tuned_examples/ppo/cartpole_ppo.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_discrete", "torch_only"],
    size = "large",
    srcs = ["tuned_examples/ppo/cartpole_ppo.py"],
    args = ["--as-test", "--enable-new-api-stack"]
)
py_test(
    name = "learning_tests_cartpole_ppo_gpu",
    main = "tuned_examples/ppo/cartpole_ppo.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_discrete", "learning_tests_pytorch_use_all_core", "gpu"],
    size = "large",
    srcs = ["tuned_examples/ppo/cartpole_ppo.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-gpus=1"]
)
py_test(
    name = "learning_tests_cartpole_ppo_multi_cpu",
    main = "tuned_examples/ppo/cartpole_ppo.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_discrete", "learning_tests_pytorch_use_all_core"],
    size = "large",
    srcs = ["tuned_examples/ppo/cartpole_ppo.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-gpus=2"]
)
py_test(
    name = "learning_tests_cartpole_ppo_multi_gpu",
    main = "tuned_examples/ppo/cartpole_ppo.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_discrete", "learning_tests_pytorch_use_all_core", "multi_gpu"],
    size = "large",
    srcs = ["tuned_examples/ppo/cartpole_ppo.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-gpus=2"]
)
# MultiAgentCartPole
py_test(
    name = "learning_tests_multi_agent_cartpole_ppo",
    main = "tuned_examples/ppo/multi_agent_cartpole_ppo.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_discrete", "torch_only"],
    size = "large",
    srcs = ["tuned_examples/ppo/multi_agent_cartpole_ppo.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-agents=2"]
)
py_test(
    name = "learning_tests_multi_agent_cartpole_ppo_gpu",
    main = "tuned_examples/ppo/multi_agent_cartpole_ppo.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_discrete", "learning_tests_pytorch_use_all_core", "gpu"],
    size = "large",
    srcs = ["tuned_examples/ppo/multi_agent_cartpole_ppo.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-agents=2", "--num-gpus=1"]
)
py_test(
    name = "learning_tests_multi_agent_cartpole_ppo_multi_cpu",
    main = "tuned_examples/ppo/multi_agent_cartpole_ppo.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_discrete", "learning_tests_pytorch_use_all_core"],
    size = "large",
    srcs = ["tuned_examples/ppo/multi_agent_cartpole_ppo.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-agents=2", "--num-gpus=2"]
)
py_test(
    name = "learning_tests_multi_agent_cartpole_ppo_multi_gpu",
    main = "tuned_examples/ppo/multi_agent_cartpole_ppo.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_discrete", "learning_tests_pytorch_use_all_core", "multi_gpu"],
    size = "large",
    srcs = ["tuned_examples/ppo/multi_agent_cartpole_ppo.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-agents=2", "--num-gpus=2"]
)
# CartPole (truncated)
py_test(
    name = "learning_tests_cartpole_truncated_ppo",
    main = "tuned_examples/ppo/cartpole_truncated_ppo.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_discrete", "torch_only"],
    size = "large",
    srcs = ["tuned_examples/ppo/cartpole_truncated_ppo.py"],
    args = ["--as-test", "--enable-new-api-stack"]
)
# StatelessCartPole
py_test(
    name = "learning_tests_stateless_cartpole_ppo",
    main = "tuned_examples/ppo/stateless_cartpole_ppo.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_discrete", "torch_only"],
    size = "large",
    srcs = ["tuned_examples/ppo/stateless_cartpole_ppo.py"],
    args = ["--as-test", "--enable-new-api-stack"]
)
py_test(
    name = "learning_tests_stateless_cartpole_ppo_gpu",
    main = "tuned_examples/ppo/stateless_cartpole_ppo.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_discrete", "learning_tests_pytorch_use_all_core", "gpu"],
    size = "large",
    srcs = ["tuned_examples/ppo/stateless_cartpole_ppo.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-gpus=1"]
)
py_test(
    name = "learning_tests_stateless_cartpole_ppo_multi_cpu",
    main = "tuned_examples/ppo/stateless_cartpole_ppo.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_discrete", "learning_tests_pytorch_use_all_core"],
    size = "large",
    srcs = ["tuned_examples/ppo/stateless_cartpole_ppo.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-gpus=2"]
)
py_test(
    name = "learning_tests_stateless_cartpole_ppo_multi_gpu",
    main = "tuned_examples/ppo/stateless_cartpole_ppo.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_discrete", "learning_tests_pytorch_use_all_core", "multi_gpu"],
    size = "large",
    srcs = ["tuned_examples/ppo/stateless_cartpole_ppo.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-gpus=2"]
)
# MultiAgentStatelessCartPole
py_test(
    name = "learning_tests_multi_agent_stateless_cartpole_ppo",
    main = "tuned_examples/ppo/multi_agent_stateless_cartpole_ppo.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_discrete", "torch_only"],
    size = "large",
    srcs = ["tuned_examples/ppo/multi_agent_stateless_cartpole_ppo.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-agents=2"]
)
py_test(
    name = "learning_tests_multi_agent_stateless_cartpole_ppo_gpu",
    main = "tuned_examples/ppo/multi_agent_stateless_cartpole_ppo.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_discrete", "learning_tests_pytorch_use_all_core", "gpu"],
    size = "large",
    srcs = ["tuned_examples/ppo/multi_agent_stateless_cartpole_ppo.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-agents=2", "--num-gpus=1"]
)
py_test(
    name = "learning_tests_multi_agent_stateless_cartpole_ppo_multi_cpu",
    main = "tuned_examples/ppo/multi_agent_stateless_cartpole_ppo.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_discrete", "learning_tests_pytorch_use_all_core"],
    size = "large",
    srcs = ["tuned_examples/ppo/multi_agent_stateless_cartpole_ppo.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-agents=2", "--num-gpus=2"]
)
py_test(
    name = "learning_tests_multi_agent_stateless_cartpole_ppo_multi_gpu",
    main = "tuned_examples/ppo/multi_agent_stateless_cartpole_ppo.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_discrete", "learning_tests_pytorch_use_all_core", "multi_gpu"],
    size = "large",
    srcs = ["tuned_examples/ppo/multi_agent_stateless_cartpole_ppo.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-agents=2", "--num-gpus=2"]
)
# Pendulum
py_test(
    name = "learning_tests_pendulum_ppo",
    main = "tuned_examples/ppo/pendulum_ppo.py",
    tags = ["torch_only", "team:rllib", "exclusive", "learning_tests", "learning_tests_continuous"],
    size = "large",
    srcs = ["tuned_examples/ppo/pendulum_ppo.py"],
    args = ["--as-test", "--enable-new-api-stack"]
)
py_test(
    name = "learning_tests_pendulum_ppo_gpu",
    main = "tuned_examples/ppo/pendulum_ppo.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_continuous", "learning_tests_pytorch_use_all_core", "gpu"],
    size = "large",
    srcs = ["tuned_examples/ppo/pendulum_ppo.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-gpus=1"]
)
py_test(
    name = "learning_tests_pendulum_ppo_multi_cpu",
    main = "tuned_examples/ppo/pendulum_ppo.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_continuous", "learning_tests_pytorch_use_all_core"],
    size = "large",
    srcs = ["tuned_examples/ppo/pendulum_ppo.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-gpus=2"]
)
py_test(
    name = "learning_tests_pendulum_ppo_multi_gpu",
    main = "tuned_examples/ppo/pendulum_ppo.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_continuous", "learning_tests_pytorch_use_all_core", "multi_gpu"],
    size = "large",
    srcs = ["tuned_examples/ppo/pendulum_ppo.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-gpus=2"]
)
# MultiAgentPendulum
py_test(
    name = "learning_tests_multi_agent_pendulum_ppo",
    main = "tuned_examples/ppo/multi_agent_pendulum_ppo.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_continuous", "torch_only"],
    size = "large",
    srcs = ["tuned_examples/ppo/multi_agent_pendulum_ppo.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-agents=2"]
)
py_test(
    name = "learning_tests_multi_agent_pendulum_ppo_gpu",
    main = "tuned_examples/ppo/multi_agent_pendulum_ppo.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_continuous", "learning_tests_pytorch_use_all_core", "gpu"],
    size = "large",
    srcs = ["tuned_examples/ppo/multi_agent_pendulum_ppo.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-agents=2", "--num-gpus=1"]
)
py_test(
    name = "learning_tests_multi_agent_pendulum_ppo_multi_cpu",
    main = "tuned_examples/ppo/multi_agent_pendulum_ppo.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_continuous", "learning_tests_pytorch_use_all_core"],
    size = "large",
    srcs = ["tuned_examples/ppo/multi_agent_pendulum_ppo.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-agents=2", "--num-gpus=2"]
)
py_test(
    name = "learning_tests_multi_agent_pendulum_ppo_multi_gpu",
    main = "tuned_examples/ppo/multi_agent_pendulum_ppo.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_continuous", "learning_tests_pytorch_use_all_core", "multi_gpu"],
    size = "large",
    srcs = ["tuned_examples/ppo/multi_agent_pendulum_ppo.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-agents=2", "--num-gpus=2"]
)

#@OldAPIStack
py_test(
    name = "learning_tests_pendulum_ppo_old_api_stack",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_continuous", "no_tf_static_graph"],
    size = "large", # bazel may complain about it being too long sometimes - large is on purpose as some frameworks take longer
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/ppo/pendulum-ppo.yaml"],
    args = ["--dir=tuned_examples/ppo"]
)
#@OldAPIStack
py_test(
    name = "learning_tests_transformed_actions_pendulum_ppo_old_api_stack",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_continuous", "no_tf_static_graph"],
    size = "large", # bazel may complain about it being too long sometimes - large is on purpose as some frameworks take longer
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/ppo/pendulum-transformed-actions-ppo.yaml"],
    args = ["--dir=tuned_examples/ppo"]
)
#@OldAPIStack
py_test(
    name = "learning_tests_repeat_after_me_ppo_old_api_stack",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_discrete"],
    size = "medium",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/ppo/repeatafterme-ppo-lstm.yaml"],
    args = ["--dir=tuned_examples/ppo"]
)

# SAC
# Pendulum
py_test(
    name = "learning_tests_pendulum_sac",
    main = "tuned_examples/sac/pendulum_sac.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_continuous"],
    size = "large",
    srcs = ["tuned_examples/sac/pendulum_sac.py"],
    args = ["--as-test", "--enable-new-api-stack"]
)
py_test(
    name = "learning_tests_pendulum_sac_gpu",
    main = "tuned_examples/sac/pendulum_sac.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_continuous", "gpu"],
    size = "large",
    srcs = ["tuned_examples/sac/pendulum_sac.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-gpus=1"]
)
py_test(
    name = "learning_tests_pendulum_sac_multi_cpu",
    main = "tuned_examples/sac/pendulum_sac.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_continuous"],
    size = "large",
    srcs = ["tuned_examples/sac/pendulum_sac.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-gpus=2"]
)
py_test(
    name = "learning_tests_pendulum_sac_multi_gpu",
    main = "tuned_examples/sac/pendulum_sac.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_continuous", "multi_gpu"],
    size = "large",
    srcs = ["tuned_examples/sac/pendulum_sac.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-gpus=2"]
)
# MultiAgentPendulum
py_test(
    name = "learning_tests_multi_agent_pendulum_sac",
    main = "tuned_examples/sac/multi_agent_pendulum_sac.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_continuous"],
    size = "large",
    srcs = ["tuned_examples/sac/multi_agent_pendulum_sac.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-agents=2", "--num-cpus=4"]
)
py_test(
    name = "learning_tests_multi_agent_pendulum_sac_gpu",
    main = "tuned_examples/sac/multi_agent_pendulum_sac.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_continuous", "gpu"],
    size = "large",
    srcs = ["tuned_examples/sac/multi_agent_pendulum_sac.py"],
    args = ["--as-test", "--enable-new-api-stack", "--num-agents=2", "--num-cpus=4", "--num-gpus=1"]
)
py_test(
    name = "learning_tests_multi_agent_pendulum_sac_multi_cpu",
    main = "tuned_examples/sac/multi_agent_pendulum_sac.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_continuous"],
    size = "large",
    srcs = ["tuned_examples/sac/multi_agent_pendulum_sac.py"],
    args = ["--enable-new-api-stack", "--num-agents=2", "--num-gpus=2"]
)
py_test(
    name = "learning_tests_multi_agent_pendulum_sac_multi_gpu",
    main = "tuned_examples/sac/multi_agent_pendulum_sac.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_continuous", "multi_gpu"],
    size = "large",
    srcs = ["tuned_examples/sac/multi_agent_pendulum_sac.py"],
    args = ["--enable-new-api-stack", "--num-agents=2", "--num-gpus=2"]
)

#@OldAPIStack
py_test(
    name = "learning_tests_cartpole_sac_old_api_stack",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_discrete"],
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
    name ="test_algorithm_save_load_checkpoint_learner",
    tags = ["team:rllib", "algorithms_dir", "algorithms_dir_generic"],
    size = "medium",
    srcs = ["algorithms/tests/test_algorithm_save_load_checkpoint_learner.py"]
)

py_test(
    name="test_algorithm_rl_module_restore",
    tags=["team:rllib", "algorithms_dir", "algorithms_dir_generic"],
    size="large",
    srcs=["algorithms/tests/test_algorithm_rl_module_restore.py"]
)

py_test(
    name = "test_algorithm_imports",
    tags = ["team:rllib", "algorithms_dir", "algorithms_dir_generic"],
    size = "small",
    srcs = ["algorithms/tests/test_algorithm_imports.py"]
)

py_test(
    name = "test_algorithm_checkpoint_restore_ppo",
    main = "algorithms/tests/test_algorithm_checkpoint_restore.py",
    tags = ["team:rllib", "algorithms_dir", "algorithms_dir_generic"],
    size = "large",
    srcs = ["algorithms/tests/test_algorithm_checkpoint_restore.py"],
    args = ["TestCheckpointRestorePPO"]
)

py_test(
    name = "test_algorithm_checkpoint_restore_ppo_gpu",
    main = "algorithms/tests/test_algorithm_checkpoint_restore.py",
    tags = ["team:rllib", "algorithms_dir", "algorithms_dir_generic", "gpu"],
    size = "large",
    srcs = ["algorithms/tests/test_algorithm_checkpoint_restore.py"],
    args = ["TestCheckpointRestorePPO"]
)

py_test(
    name = "test_algorithm_checkpoint_restore_off_policy",
    main = "algorithms/tests/test_algorithm_checkpoint_restore.py",
    tags = ["team:rllib", "algorithms_dir", "algorithms_dir_generic"],
    size = "large",
    srcs = ["algorithms/tests/test_algorithm_checkpoint_restore.py"],
    args = ["TestCheckpointRestoreOffPolicy"]
)

py_test(
    name = "test_algorithm_checkpoint_restore_off_policy_gpu",
    main = "algorithms/tests/test_algorithm_checkpoint_restore.py",
    tags = ["team:rllib", "algorithms_dir", "algorithms_dir_generic", "gpu"],
    size = "large",
    srcs = ["algorithms/tests/test_algorithm_checkpoint_restore.py"],
    args = ["TestCheckpointRestoreOffPolicy"]
)

py_test(
    name = "test_callbacks_on_algorithm",
    tags = ["team:rllib", "algorithms_dir", "algorithms_dir_generic"],
    size = "large",
    srcs = ["algorithms/tests/test_callbacks_on_algorithm.py"]
)
py_test(
    name = "test_callbacks_on_env_runner",
    tags = ["team:rllib", "algorithms_dir", "algorithms_dir_generic"],
    size = "medium",
    srcs = ["algorithms/tests/test_callbacks_on_env_runner.py"]
)
py_test(
    name = "test_callbacks_old_stack",
    tags = ["team:rllib", "algorithms_dir", "algorithms_dir_generic"],
    size = "medium",
    srcs = ["algorithms/tests/test_callbacks_old_stack.py"]
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
    name = "test_appo_learner",
    tags = ["team:rllib", "algorithms_dir"],
    size = "medium",
    srcs = ["algorithms/appo/tests/test_appo_learner.py"]
)

# BC
py_test(
    name = "test_bc_old_api_stack",
    tags = ["team:rllib", "algorithms_dir"],
    size = "medium",
    # Include the json data file.
    data = ["tests/data/cartpole/large.json"],
    srcs = ["algorithms/bc/tests/test_bc_old_api_stack.py"]
)
py_test(
    name = "test_bc",
    tags = ["team:rllib", "algorithms_dir"],
    size = "medium",
    # Include the parquet data files.
    data = ["tests/data/cartpole/cartpole-v1_large"],
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

# IMPALA
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

# MARWIL
py_test(
    name = "test_marwil_old_api_stack",
    tags = ["team:rllib", "algorithms_dir"],
    size = "large",
    # Include the json data file.
    data = [
        "tests/data/cartpole/large.json",
        "tests/data/pendulum/large.json",
        "tests/data/cartpole/small.json",
    ],
    srcs = ["algorithms/marwil/tests/test_marwil_old_api_stack.py"]
)
py_test(
    name = "test_marwil_rl_module",
    tags = ["team:rllib", "algorithms_dir"],
    size = "large",
    # Include the json data file.
    data = [
        "tests/data/cartpole/large.json",
    ],
    srcs = ["algorithms/marwil/tests/test_marwil_rl_module.py"]
)
py_test(
    name = "test_marwil",
    tags = ["team:rllib", "algorithms_dir"],
    size = "large",
    # Include the parquet data folder.
    data = [
        "tests/data/cartpole/cartpole-v1_large",
        "tests/data/pendulum/pendulum-v1_large",
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

# @OldAPIStack
py_test(
    name = "test_memory_leak_appo_old_api_stack",
    tags = ["team:rllib", "memory_leak_tests"],
    main = "utils/tests/run_memory_leak_tests.py",
    size = "large",
    srcs = ["utils/tests/run_memory_leak_tests.py"],
    data = ["tuned_examples/appo/memory-leak-test-appo.yaml"],
    args = ["--dir=tuned_examples/appo"]
)
# @OldAPIStack
py_test(
    name = "test_memory_leak_dqn_old_api_stack",
    tags = ["team:rllib", "memory_leak_tests"],
    main = "utils/tests/run_memory_leak_tests.py",
    size = "large",
    srcs = ["utils/tests/run_memory_leak_tests.py"],
    data = ["tuned_examples/dqn/memory-leak-test-dqn.yaml"],
    args = ["--dir=tuned_examples/dqn"]
)
# @OldAPIStack
py_test(
    name = "test_memory_leak_impala_old_api_stack",
    tags = ["team:rllib", "memory_leak_tests"],
    main = "utils/tests/run_memory_leak_tests.py",
    size = "large",
    srcs = ["utils/tests/run_memory_leak_tests.py"],
    data = ["tuned_examples/impala/memory-leak-test-impala.yaml"],
    args = ["--dir=tuned_examples/impala"]
)
# @OldAPIStack
py_test(
    name = "test_memory_leak_ppo_old_api_stack",
    tags = ["team:rllib", "memory_leak_tests"],
    main = "utils/tests/run_memory_leak_tests.py",
    size = "large",
    srcs = ["utils/tests/run_memory_leak_tests.py"],
    data = ["tuned_examples/ppo/memory-leak-test-ppo.yaml"],
    args = ["--dir=tuned_examples/ppo"]
)
# @OldAPIStack
py_test(
    name = "test_memory_leak_sac_old_api_stack",
    tags = ["team:rllib", "memory_leak_tests"],
    main = "utils/tests/run_memory_leak_tests.py",
    size = "large",
    srcs = ["utils/tests/run_memory_leak_tests.py"],
    data = ["tuned_examples/sac/memory-leak-test-sac.yaml"],
    args = ["--dir=tuned_examples/sac"]
)


# --------------------------------------------------------------------
# Connector(V1) tests
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
    tags = ["team:rllib", "env"],
    size = "small",
    srcs = ["env/tests/test_infinite_lookback_buffer.py"]
)

sh_test(
    name = "env/tests/test_local_inference_cartpole",
    tags = ["team:rllib", "env"],
    size = "medium",
    srcs = ["env/tests/test_policy_client_server_setup.sh"],
    args = ["local", "cartpole", "8800"],
    data = glob(["examples/envs/external_envs/*.py"]),
)

sh_test(
    name = "env/tests/test_local_inference_cartpole_w_2_concurrent_episodes",
    tags = ["team:rllib", "env"],
    size = "medium",
    srcs = ["env/tests/test_policy_client_server_setup.sh"],
    args = ["local", "cartpole-dummy-2-episodes", "8830"],
    data = glob(["examples/envs/external_envs/*.py"]),
)

# Tests with unity 3d and external envs currently don't work
# see: https://github.com/ray-project/ray/issues/34290 for more details
# sh_test(
#     name = "env/tests/test_local_inference_unity3d",
#     tags = ["team:rllib", "env"],
#     size = "large", # bazel may complain about it being too long sometimes - large is on purpose as some frameworks take longer
#     srcs = ["env/tests/test_policy_client_server_setup.sh"],
#     args = ["local", "unity3d", "8850"],
#     data = glob(["examples/envs/external_envs/*.py"]),
# )

py_test(
    name = "env/tests/test_multi_agent_env",
    tags = ["team:rllib", "tests_dir"],
    size = "large",
    srcs = ["env/tests/test_multi_agent_env.py"]
)

py_test(
    name = "env/tests/test_multi_agent_env_runner",
    tags = ["team:rllib", "env"],
    size = "medium",
    srcs = ["env/tests/test_multi_agent_env_runner.py"]
)

py_test(
    name = "env/tests/test_multi_agent_episode",
    tags = ["team:rllib", "env"],
    size = "medium",
    srcs = ["env/tests/test_multi_agent_episode.py"]
)

sh_test(
    name = "env/tests/test_remote_inference_cartpole",
    tags = ["team:rllib", "env", "exclusive"],
    size = "large", # bazel may complain about it being too long sometimes - large is on purpose as some frameworks take longer
    srcs = ["env/tests/test_policy_client_server_setup.sh"],
    args = ["remote", "cartpole", "8810"],
    data = glob(["examples/envs/external_envs/*.py"]),
)

sh_test(
    name = "env/tests/test_remote_inference_cartpole_lstm",
    tags = ["team:rllib", "env", "exclusive"],
    size = "large", # bazel may complain about it being too long sometimes - large is on purpose as some frameworks take longer
    srcs = ["env/tests/test_policy_client_server_setup.sh"],
    args = ["remote", "cartpole_lstm", "8820"],
    data = glob(["examples/envs/external_envs/*.py"]),
)

sh_test(
    name = "env/tests/test_remote_inference_cartpole_w_2_concurrent_episodes",
    tags = ["team:rllib", "env", "exclusive"],
    size = "large", # bazel may complain about it being too long sometimes - large is on purpose as some frameworks take longer
    srcs = ["env/tests/test_policy_client_server_setup.sh"],
    args = ["remote", "cartpole-dummy-2-episodes", "8840"],
    data = glob(["examples/envs/external_envs/*.py"]),
)

# Tests with unity 3d and external envs currently don't work
# see: https://github.com/ray-project/ray/issues/34290 for more details
# sh_test(
#     name = "env/tests/test_remote_inference_unity3d",
#     tags = ["team:rllib", "env", "exclusive"],
#     size = "small",
#     srcs = ["env/tests/test_policy_client_server_setup.sh"],
#     args = ["remote", "unity3d", "8860"],
#     data = glob(["examples/envs/external_envs/*.py"]),
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
    name = "env/tests/test_env_runner_group",
    tags = ["team:rllib", "evaluation", "exclusive"],
    size = "small",
    srcs = ["env/tests/test_env_runner_group.py"]
)

py_test(
    name = "evaluation/tests/test_env_runner_v2",
    tags = ["team:rllib", "evaluation"],
    size = "small",
    srcs = ["evaluation/tests/test_env_runner_v2.py"]
)

py_test(
    name = "evaluation/tests/test_episode",
    tags = ["team:rllib", "evaluation"],
    size = "small",
    srcs = ["evaluation/tests/test_episode.py"]
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
    name = "test_multi_rl_module",
    tags = ["team:rllib", "core"],
    size = "medium",
    srcs = ["core/rl_module/tests/test_multi_rl_module.py"]
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

py_test(
    name = "test_offline_env_runner",
    tags = ["team:rllib", "offline"],
    size = "small",
    srcs = ["offline/tests/test_offline_env_runner.py"],
)

py_test(
    name = "test_offline_data",
    tags = ["team:rllib", "offline"],
    size = "medium",
    srcs = ["offline/tests/test_offline_data.py"],
    data = [
        "tests/data/cartpole/cartpole-v1_large",
        "tests/data/cartpole/large.json",
    ],
)

py_test(
    name = "test_offline_prelearner",
    tags = ["team:rllib", "offline"],
    size = "small",
    srcs = ["offline/tests/test_offline_prelearner.py"],
    data = [
        "tests/data/cartpole/cartpole-v1_large",
        "tests/data/cartpole/large.json",
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
    name = "test_value_predictions",
    tags = ["team:rllib", "utils"],
    size = "small",
    srcs = ["utils/postprocessing/tests/test_value_predictions.py"]
)

py_test(
    name = "test_torch_utils",
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
    name = "test_multi_agent_episode_buffer",
    tags = ["team:rllib", "utils"],
    size = "small",
    srcs = ["utils/replay_buffers/tests/test_multi_agent_episode_buffer.py"]
)

py_test(
    name = "test_multi_agent_mixin_replay_buffer",
    tags = ["team:rllib", "utils"],
    size = "small",
    srcs = ["utils/replay_buffers/tests/test_multi_agent_mixin_replay_buffer.py"]
)

py_test(
    name = "test_multi_agent_prio_episode_buffer",
    tags = ["team:rllib", "utils"],
    size = "small",
    srcs = ["utils/replay_buffers/tests/test_multi_agent_prio_episode_buffer.py"]
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
    name = "test_prioritized_episode_buffer",
    tags = ["team::rllib", "utils"],
    size = "small",
    srcs = ["utils/replay_buffers/tests/test_prioritized_episode_buffer.py"]
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
    name = "tests/test_catalog",
    tags = ["team:rllib", "tests_dir"],
    size = "medium",
    srcs = ["tests/test_catalog.py"]
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
# examples/ directory
#
# Tag: examples
#
# NOTE: Add tests alphabetically into this list.
# --------------------------------------------------------------------

# subdirectory: _docs/

py_test(
    name = "examples/_docs/rllib_on_rllib_readme",
    main = "examples/_docs/rllib_on_rllib_readme.py",
    tags = ["team:rllib", "documentation", "no_main"],
    size = "medium",
    srcs = ["examples/_docs/rllib_on_rllib_readme.py"],
)

# ----------------------
# Old API stack examples
# ----------------------
# subdirectory: _old_api_stack/
py_test(
    name = "examples/_old_api_stack/complex_struct_space_tf",
    main = "examples/_old_api_stack/complex_struct_space.py",
    tags = ["team:rllib", "exclusive", "examples", "old_api_stack"],
    size = "small",
    srcs = ["examples/_old_api_stack/complex_struct_space.py"],
    args = ["--framework=tf"],
)

py_test(
    name = "examples/_old_api_stack/complex_struct_space_tf_eager",
    main = "examples/_old_api_stack/complex_struct_space.py",
    tags = ["team:rllib", "exclusive", "examples", "old_api_stack"],
    size = "small",
    srcs = ["examples/_old_api_stack/complex_struct_space.py"],
    args = ["--framework=tf2"],
)

py_test(
    name = "examples/_old_api_stack/complex_struct_space_torch",
    main = "examples/_old_api_stack/complex_struct_space.py",
    tags = ["team:rllib", "exclusive", "examples", "old_api_stack"],
    size = "small",
    srcs = ["examples/_old_api_stack/complex_struct_space.py"],
    args = ["--framework=torch"],
)

py_test(
    name = "examples/_old_api_stack/parametric_actions_cartpole_dqn_tf",
    main = "examples/_old_api_stack/parametric_actions_cartpole.py",
    tags = ["team:rllib", "exclusive", "examples", "old_api_stack"],
    size = "medium",
    srcs = ["examples/_old_api_stack/parametric_actions_cartpole.py"],
    args = ["--as-test", "--framework=tf", "--stop-reward=60.0", "--run=DQN"]
)

py_test(
    name = "examples/_old_api_stack/parametric_actions_cartpole_dqn_torch",
    main = "examples/_old_api_stack/parametric_actions_cartpole.py",
    tags = ["team:rllib", "exclusive", "examples", "old_api_stack"],
    size = "medium",
    srcs = ["examples/_old_api_stack/parametric_actions_cartpole.py"],
    args = ["--as-test", "--framework=torch", "--stop-reward=60.0", "--run=DQN"]
)

py_test(
    name = "examples/_old_api_stack/parametric_actions_cartpole_embeddings_learnt_by_model",
    main = "examples/_old_api_stack/parametric_actions_cartpole_embeddings_learnt_by_model.py",
    tags = ["team:rllib", "exclusive", "examples", "old_api_stack"],
    size = "medium",
    srcs = ["examples/_old_api_stack/parametric_actions_cartpole_embeddings_learnt_by_model.py"],
    args = ["--as-test", "--stop-reward=80.0"]
)

# subdirectory: _old_api_stack/connectors/
py_test(
    name = "examples/_old_api_stack/connectors/run_connector_policy",
    main = "examples/_old_api_stack/connectors/run_connector_policy.py",
    tags = ["team:rllib", "exclusive", "examples", "old_api_stack"],
    size = "small",
    srcs = ["examples/_old_api_stack/connectors/run_connector_policy.py"],
)

py_test(
    name = "examples/_old_api_stack/connectors/run_connector_policy_w_lstm",
    main = "examples/_old_api_stack/connectors/run_connector_policy.py",
    tags = ["team:rllib", "exclusive", "examples", "old_api_stack"],
    size = "small",
    srcs = ["examples/_old_api_stack/connectors/run_connector_policy.py"],
    args = ["--use-lstm"],
)

py_test(
    name = "examples/_old_api_stack/connectors/adapt_connector_policy",
    main = "examples/_old_api_stack/connectors/adapt_connector_policy.py",
    tags = ["team:rllib", "exclusive", "examples", "old_api_stack"],
    size = "small",
    srcs = ["examples/_old_api_stack/connectors/adapt_connector_policy.py"],
)

# py_test(
#     name = "examples/_old_api_stack/connectors/self_play_with_policy_checkpoint",
#     main = "examples/_old_api_stack/connectors/self_play_with_policy_checkpoint.py",
#     tags = ["team:rllib", "exclusive", "examples", "old_api_stack"],
#     size = "small",
#     srcs = ["examples/_old_api_stack/connectors/self_play_with_policy_checkpoint.py"],
#     args = [
#         "--train_iteration=1"  # Smoke test.
#     ]
# )

# ----------------------
# New API stack
# Note: This includes to-be-translated-to-new-API-stack examples
# tagged by @OldAPIStack and/or @HybridAPIStack
# ----------------------

# subdirectory: actions/

# Nested action spaces (flattening obs and learning w/ multi-action distribution).
py_test(
    name = "examples/actions/nested_action_spaces_ppo",
    main = "examples/actions/nested_action_spaces.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "large",
    srcs = ["examples/actions/nested_action_spaces.py"],
    args = ["--enable-new-api-stack", "--as-test", "--framework=torch", "--stop-reward=-500.0", "--algo=PPO"]
)

py_test(
    name = "examples/actions/nested_action_spaces_multi_agent_ppo",
    main = "examples/actions/nested_action_spaces.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "large",
    srcs = ["examples/actions/nested_action_spaces.py"],
    args = ["--enable-new-api-stack", "--as-test", "--num-agents=2", "--framework=torch", "--stop-reward=-1000.0", "--algo=PPO"]
)

# subdirectory: algorithms/

#@OldAPIStack
py_test(
    name = "examples/algorithms/custom_training_step_on_and_off_policy_combined_tf",
    main = "examples/algorithms/custom_training_step_on_and_off_policy_combined.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/algorithms/custom_training_step_on_and_off_policy_combined.py"],
    args = ["--as-test", "--stop-reward=450.0"]
)

#@OldAPIStack
py_test(
    name = "examples/algorithms/custom_training_step_on_and_off_policy_combined_torch",
    main = "examples/algorithms/custom_training_step_on_and_off_policy_combined.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/algorithms/custom_training_step_on_and_off_policy_combined.py"],
    args = ["--as-test", "--torch", "--stop-reward=450.0"]
)

# subdirectory: catalogs/
# ....................................
py_test(
    name = "examples/catalogs/custom_action_distribution",
    main = "examples/catalogs/custom_action_distribution.py",
    tags = ["team:rllib", "examples", "no_main"],
    size = "small",
    srcs = ["examples/catalogs/custom_action_distribution.py"],
)

py_test(
    name = "examples/catalogs/mobilenet_v2_encoder",
    main = "examples/catalogs/mobilenet_v2_encoder.py",
    tags = ["team:rllib", "examples", "no_main"],
    size = "small",
    srcs = ["examples/catalogs/mobilenet_v2_encoder.py"],
)

# subdirectory: checkpoints/
# ....................................
py_test(
    name = "examples/checkpoints/checkpoint_by_custom_criteria",
    main = "examples/checkpoints/checkpoint_by_custom_criteria.py",
    tags = ["team:rllib", "exclusive", "examples", "examples_use_all_core"],
    size = "large",
    srcs = ["examples/checkpoints/checkpoint_by_custom_criteria.py"],
    args = ["--enable-new-api-stack", "--stop-reward=150.0", "--num-cpus=8"]
)

py_test(
    name = "examples/checkpoints/continue_training_from_checkpoint",
    main = "examples/checkpoints/continue_training_from_checkpoint.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "large",
    srcs = ["examples/checkpoints/continue_training_from_checkpoint.py"],
    args = ["--enable-new-api-stack", "--as-test"]
)

py_test(
    name = "examples/checkpoints/continue_training_from_checkpoint_multi_agent",
    main = "examples/checkpoints/continue_training_from_checkpoint.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "large",
    srcs = ["examples/checkpoints/continue_training_from_checkpoint.py"],
    args = ["--enable-new-api-stack", "--as-test", "--num-agents=2", "--stop-reward-crash=400.0", "--stop-reward=900.0"]
)

#@OldAPIStack
py_test(
    name = "examples/checkpoints/continue_training_from_checkpoint_old_api_stack",
    main = "examples/checkpoints/continue_training_from_checkpoint.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "large",
    srcs = ["examples/checkpoints/continue_training_from_checkpoint.py"],
    args = ["--as-test"]
)

py_test(
    name = "examples/checkpoints/cartpole_dqn_export",
    main = "examples/checkpoints/cartpole_dqn_export.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "small",
    srcs = ["examples/checkpoints/cartpole_dqn_export.py"],
)

#@OldAPIStack
py_test(
    name = "examples/checkpoints/onnx_tf2",
    main = "examples/checkpoints/onnx_tf.py",
    tags = ["team:rllib", "exclusive", "examples", "no_main"],
    size = "small",
    srcs = ["examples/checkpoints/onnx_tf.py"],
    args = ["--framework=tf2"],
)

#@OldAPIStack
py_test(
    name = "examples/checkpoints/onnx_torch",
    main = "examples/checkpoints/onnx_torch.py",
    tags = ["team:rllib", "exclusive", "examples", "no_main"],
    size = "small",
    srcs = ["examples/checkpoints/onnx_torch.py"],
)

#@OldAPIStack
py_test(
    name = "examples/checkpoints/onnx_torch_lstm",
    main = "examples/checkpoints/onnx_torch_lstm.py",
    tags = ["team:rllib", "exclusive", "examples", "no_main"],
    size = "small",
    srcs = ["examples/checkpoints/onnx_torch_lstm.py"],
)

# subdirectory: connectors/
# ....................................
# Framestacking examples only run in smoke-test mode (a few iters only).
# PPO
py_test(
    name = "examples/connectors/frame_stacking_ppo",
    main = "examples/connectors/frame_stacking.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/connectors/frame_stacking.py"],
    args = ["--enable-new-api-stack", "--stop-iter=2", "--framework=torch", "--algo=PPO"]
)

py_test(
    name = "examples/connectors/frame_stacking_multi_agent_ppo",
    main = "examples/connectors/frame_stacking.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/connectors/frame_stacking.py"],
    args = ["--enable-new-api-stack", "--num-agents=2", "--stop-iter=2", "--framework=torch", "--algo=PPO", "--num-env-runners=4", "--num-cpus=6"]
)
# IMPALA
py_test(
    name = "examples/connectors/frame_stacking_impala",
    main = "examples/connectors/frame_stacking.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/connectors/frame_stacking.py"],
    args = ["--enable-new-api-stack", "--stop-iter=2", "--framework=torch", "--algo=IMPALA"]
)
py_test(
    name = "examples/connectors/frame_stacking_multi_agent_impala",
    main = "examples/connectors/frame_stacking.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/connectors/frame_stacking.py"],
    args = ["--enable-new-api-stack", "--num-agents=2", "--stop-iter=2", "--framework=torch", "--algo=IMPALA", "--num-env-runners=4", "--num-cpus=6"]
)

# Nested observation spaces (flattening).
# PPO
py_test(
    name = "examples/connectors/flatten_observations_dict_space_ppo",
    main = "examples/connectors/flatten_observations_dict_space.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/connectors/flatten_observations_dict_space.py"],
    args = ["--enable-new-api-stack", "--as-test", "--stop-reward=400.0", "--framework=torch", "--algo=PPO"]
)
py_test(
    name = "examples/connectors/flatten_observations_dict_space_multi_agent_ppo",
    main = "examples/connectors/flatten_observations_dict_space.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/connectors/flatten_observations_dict_space.py"],
    args = ["--enable-new-api-stack", "--num-agents=2", "--as-test", "--stop-reward=800.0", "--framework=torch", "--algo=PPO"]
)
# IMPALA
py_test(
    name = "examples/connectors/flatten_observations_dict_space_impala",
    main = "examples/connectors/flatten_observations_dict_space.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "large",
    srcs = ["examples/connectors/flatten_observations_dict_space.py"],
    args = ["--enable-new-api-stack", "--as-test", "--stop-reward=400.0", "--stop-timesteps=2000000", "--framework=torch", "--algo=IMPALA"]
)
py_test(
    name = "examples/connectors/flatten_observations_dict_space_multi_agent_impala",
    main = "examples/connectors/flatten_observations_dict_space.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "large",
    srcs = ["examples/connectors/flatten_observations_dict_space.py"],
    args = ["--enable-new-api-stack", "--num-agents=2", "--as-test", "--stop-reward=800.0", "--stop-timesteps=2000000", "--framework=torch", "--algo=IMPALA"]
)

# Prev-r/prev actions + LSTM example.
py_test(
    name = "examples/connectors/prev_actions_prev_rewards_ppo",
    main = "examples/connectors/prev_actions_prev_rewards.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "large",
    srcs = ["examples/connectors/prev_actions_prev_rewards.py"],
    args = ["--enable-new-api-stack", "--as-test", "--stop-reward=200.0", "--framework=torch", "--algo=PPO", "--num-env-runners=4", "--num-cpus=6"]
)

py_test(
    name = "examples/connectors/prev_actions_prev_rewards_multi_agent_ppo",
    main = "examples/connectors/prev_actions_prev_rewards.py",
    tags = ["team:rllib", "exclusive", "examples", "examples_use_all_core"],
    size = "large",
    srcs = ["examples/connectors/prev_actions_prev_rewards.py"],
    args = ["--enable-new-api-stack", "--num-agents=2", "--as-test", "--stop-reward=400.0", "--framework=torch", "--algo=PPO", "--num-env-runners=4", "--num-cpus=6"]
)

# MeanStd filtering example.
# PPO
py_test(
    name = "examples/connectors/mean_std_filtering_ppo",
    main = "examples/connectors/mean_std_filtering.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/connectors/mean_std_filtering.py"],
    args = ["--enable-new-api-stack", "--as-test", "--stop-reward=-300.0", "--framework=torch", "--algo=PPO", "--num-env-runners=2", "--num-cpus=4"]
)

py_test(
    name = "examples/connectors/mean_std_filtering_multi_agent_ppo",
    main = "examples/connectors/mean_std_filtering.py",
    tags = ["team:rllib", "exclusive", "examples", "examples_use_all_core"],
    size = "large",
    srcs = ["examples/connectors/mean_std_filtering.py"],
    args = ["--enable-new-api-stack", "--num-agents=2", "--as-test", "--stop-reward=-600.0", "--framework=torch", "--algo=PPO", "--num-env-runners=5", "--num-cpus=7"]
)
# IMPALA
# TODO (sven): Make IMPALA learn Pendulum OR make this script flexible to accept
#  (lopsided obs) CartPole as well.
# py_test(
#    name = "examples/connectors/mean_std_filtering_impala",
#    main = "examples/connectors/mean_std_filtering.py",
#    tags = ["team:rllib", "exclusive", "examples"],
#    size = "medium",
#    srcs = ["examples/connectors/mean_std_filtering.py"],
#    args = ["--enable-new-api-stack", "--as-test", "--stop-reward=-300.0", "--framework=torch", "--algo=IMPALA", "--num-env-runners=2"]
# )
# py_test(
#    name = "examples/connectors/mean_std_filtering_multi_agent_impala",
#    main = "examples/connectors/mean_std_filtering.py",
#    tags = ["team:rllib", "exclusive", "examples"],
#    size = "medium",
#    srcs = ["examples/connectors/mean_std_filtering.py"],
#    args = ["--enable-new-api-stack", "--num-agents=2", "--as-test", "--stop-reward=-600.0", "--framework=torch", "--algo=IMPALA", "--num-env-runners=5", "--num-cpus=6"]
# )

# subdirectory: curiosity/
# ....................................
py_test(
    name = "examples/curiosity/count_based_curiosity",
    main = "examples/curiosity/count_based_curiosity.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "large",
    srcs = ["examples/curiosity/count_based_curiosity.py"],
    args = ["--enable-new-api-stack", "--as-test"]
)

py_test(
    name = "examples/curiosity/euclidian_distance_based_curiosity",
    main = "examples/curiosity/euclidian_distance_based_curiosity.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "large",
    srcs = ["examples/curiosity/euclidian_distance_based_curiosity.py"],
    args = ["--enable-new-api-stack", "--as-test"]
)

py_test(
    name = "examples/curiosity/intrinsic_curiosity_model_based_curiosity_ppo",
    main = "examples/curiosity/intrinsic_curiosity_model_based_curiosity.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "large",
    srcs = ["examples/curiosity/intrinsic_curiosity_model_based_curiosity.py"],
    args = ["--enable-new-api-stack", "--as-test", "--algo=PPO"]
)

# TODO (sven): Learns, but very slowly. Needs further tuning.
# py_test(
#    name = "examples/curiosity/intrinsic_curiosity_model_based_curiosity_dqn",
#    main = "examples/curiosity/intrinsic_curiosity_model_based_curiosity.py",
#    tags = ["team:rllib", "exclusive", "examples"],
#    size = "large",
#    srcs = ["examples/curiosity/intrinsic_curiosity_model_based_curiosity.py"],
#    args = ["--enable-new-api-stack", "--as-test", "--algo=DQN"]
# )


# subdirectory: curriculum/
# ....................................
py_test(
    name = "examples/curriculum/curriculum_learning",
    main = "examples/curriculum/curriculum_learning.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/curriculum/curriculum_learning.py"],
    args = ["--enable-new-api-stack", "--as-test"]
)

# subdirectory: debugging/
# ....................................
#@OldAPIStack
py_test(
    name = "examples/debugging/deterministic_training_torch",
    main = "examples/debugging/deterministic_training.py",
    tags = ["team:rllib", "exclusive", "multi_gpu", "examples"],
    size = "medium",
    srcs = ["examples/debugging/deterministic_training.py"],
    args = ["--as-test", "--stop-iters=1", "--framework=torch", "--num-gpus=1", "--num-gpus-per-env-runner=1"]
)

# subdirectory: envs/
# ....................................
py_test(
    name = "examples/envs/custom_gym_env",
    main = "examples/envs/custom_gym_env.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/envs/custom_gym_env.py"],
    args = ["--enable-new-api-stack", "--as-test"]
)

py_test(
    name = "examples/envs/custom_env_render_method",
    main = "examples/envs/custom_env_render_method.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/envs/custom_env_render_method.py"],
    args = ["--enable-new-api-stack", "--num-agents=0"]
)

py_test(
    name = "examples/envs/custom_env_render_method_multi_agent",
    main = "examples/envs/custom_env_render_method.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/envs/custom_env_render_method.py"],
    args = ["--enable-new-api-stack", "--num-agents=2"]
)

py_test(
    name = "examples/envs/env_rendering_and_recording",
    srcs = ["examples/envs/env_rendering_and_recording.py"],
    tags = ["team:rllib", "exclusive", "examples"],
    size = "small",
    args = ["--enable-new-api-stack", "--env=CartPole-v1", "--stop-iters=3"]
)

#@OldAPIStack
py_test(
    name = "examples/envs/greyscale_env",
    tags = ["team:rllib", "examples", "no_main"],
    size = "medium",
    srcs = ["examples/envs/greyscale_env.py"],
    args = ["--stop-iters=1 --as-test --framework torch"]
)

# subdirectory: evaluation/
# ....................................
py_test(
    name = "examples/evaluation/custom_evaluation",
    main = "examples/evaluation/custom_evaluation.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/evaluation/custom_evaluation.py"],
    args = ["--enable-new-api-stack", "--framework=torch", "--as-test", "--stop-reward=0.75", "--num-cpus=5"]
)

py_test(
    name = "examples/evaluation/custom_evaluation_parallel_to_training",
    main = "examples/evaluation/custom_evaluation.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/evaluation/custom_evaluation.py"],
    args = ["--enable-new-api-stack", "--as-test", "--framework=torch", "--stop-reward=0.75", "--evaluation-parallel-to-training", "--num-cpus=5"]
)

py_test(
    name = "examples/evaluation/evaluation_parallel_to_training_duration_auto_torch",
    main = "examples/evaluation/evaluation_parallel_to_training.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/evaluation/evaluation_parallel_to_training.py"],
    args = ["--enable-new-api-stack", "--as-test", "--evaluation-parallel-to-training", "--stop-reward=450.0", "--num-cpus=6", "--evaluation-duration=auto"]
)

py_test(
    name = "examples/evaluation/evaluation_parallel_to_training_multi_agent_duration_auto_torch",
    main = "examples/evaluation/evaluation_parallel_to_training.py",
    tags = ["team:rllib", "exclusive", "examples", "examples_use_all_core"],
    size = "large",
    srcs = ["examples/evaluation/evaluation_parallel_to_training.py"],
    args = ["--enable-new-api-stack", "--num-agents=2", "--as-test", "--evaluation-parallel-to-training", "--stop-reward=900.0", "--num-cpus=6", "--evaluation-duration=auto", "--evaluation-duration-unit=episodes"]
)

py_test(
    name = "examples/evaluation/evaluation_parallel_to_training_511_ts_torch",
    main = "examples/evaluation/evaluation_parallel_to_training.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/evaluation/evaluation_parallel_to_training.py"],
    args = ["--enable-new-api-stack", "--as-test", "--evaluation-parallel-to-training", "--stop-reward=450.0", "--num-cpus=6", "--evaluation-num-env-runners=3", "--evaluation-duration=511", "--evaluation-duration-unit=timesteps"]
)

py_test(
    name = "examples/evaluation/evaluation_parallel_to_training_multi_agent_1001_ts_torch",
    main = "examples/evaluation/evaluation_parallel_to_training.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/evaluation/evaluation_parallel_to_training.py"],
    args = ["--enable-new-api-stack", "--num-agents=2", "--as-test", "--evaluation-parallel-to-training", "--stop-reward=900.0", "--num-cpus=6", "--evaluation-duration=1001", "--evaluation-duration-unit=timesteps"]
)

py_test(
    name = "examples/evaluation/evaluation_parallel_to_training_13_episodes_torch",
    main = "examples/evaluation/evaluation_parallel_to_training.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/evaluation/evaluation_parallel_to_training.py"],
    args = ["--enable-new-api-stack", "--as-test", "--evaluation-parallel-to-training", "--stop-reward=450.0", "--num-cpus=6", "--evaluation-duration=13", "--evaluation-duration-unit=episodes"]
)

py_test(
    name = "examples/evaluation/evaluation_parallel_to_training_multi_agent_10_episodes_torch",
    main = "examples/evaluation/evaluation_parallel_to_training.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/evaluation/evaluation_parallel_to_training.py"],
    args = ["--enable-new-api-stack", "--num-agents=2", "--as-test", "--evaluation-parallel-to-training", "--stop-reward=900.0", "--num-cpus=6", "--evaluation-duration=10", "--evaluation-duration-unit=episodes"]
)

# @OldAPIStack
py_test(
    name = "examples/evaluation/evaluation_parallel_to_training_duration_auto_torch_old_api_stack",
    main = "examples/evaluation/evaluation_parallel_to_training.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/evaluation/evaluation_parallel_to_training.py"],
    args = ["--as-test", "--evaluation-parallel-to-training", "--stop-reward=50.0", "--num-cpus=6", "--evaluation-duration=auto"]
)

# @OldAPIStack
py_test(
    name = "examples/evaluation/evaluation_parallel_to_training_211_ts_torch_old_api_stack",
    main = "examples/evaluation/evaluation_parallel_to_training.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/evaluation/evaluation_parallel_to_training.py"],
    args = ["--as-test", "--evaluation-parallel-to-training", "--framework=torch", "--stop-reward=30.0", "--num-cpus=6", "--evaluation-num-env-runners=3", "--evaluation-duration=211", "--evaluation-duration-unit=timesteps"]
)

# subdirectory: fault_tolerance/
# ....................................
py_test(
    name = "examples/fault_tolerance/crashing_cartpole_recreate_failed_env_runners_appo",
    main = "examples/fault_tolerance/crashing_and_stalling_env.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "large",
    srcs = ["examples/fault_tolerance/crashing_and_stalling_env.py"],
    args = ["--algo=APPO", "--enable-new-api-stack", "--as-test", "--stop-reward=450.0"]
)
py_test(
    name = "examples/fault_tolerance/crashing_cartpole_restart_failed_envs_appo",
    main = "examples/fault_tolerance/crashing_and_stalling_env.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "large",
    srcs = ["examples/fault_tolerance/crashing_and_stalling_env.py"],
    args = ["--algo=APPO", "--enable-new-api-stack", "--as-test", "--restart-failed-envs", "--stop-reward=450.0"]
)
py_test(
    name = "examples/fault_tolerance/crashing_and_stalling_cartpole_restart_failed_envs_ppo",
    main = "examples/fault_tolerance/crashing_and_stalling_env.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "large",
    srcs = ["examples/fault_tolerance/crashing_and_stalling_env.py"],
    args = ["--algo=PPO", "--enable-new-api-stack", "--as-test", "--restart-failed-envs", "--stall", "--stop-reward=450.0"]
)
py_test(
    name = "examples/fault_tolerance/crashing_and_stalling_multi_agent_cartpole_restart_failed_envs_ppo",
    main = "examples/fault_tolerance/crashing_and_stalling_env.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "large",
    srcs = ["examples/fault_tolerance/crashing_and_stalling_env.py"],
    args = ["--algo=PPO", "--num-agents=2", "--enable-new-api-stack", "--as-test", "--restart-failed-envs", "--stop-reward=800.0"]
)

# subdirectory: gpus/
# ....................................
py_test(
    name = "examples/gpus/float16_training_and_inference",
    main = "examples/gpus/float16_training_and_inference.py",
    tags = ["team:rllib", "exclusive", "examples", "gpu"],
    size = "medium",
    srcs = ["examples/gpus/float16_training_and_inference.py"],
    args = ["--enable-new-api-stack", "--as-test", "--stop-reward=150.0"]
)
py_test(
    name = "examples/gpus/mixed_precision_training_float16_inference",
    main = "examples/gpus/mixed_precision_training_float16_inference.py",
    tags = ["team:rllib", "exclusive", "examples", "gpu"],
    size = "medium",
    srcs = ["examples/gpus/mixed_precision_training_float16_inference.py"],
    args = ["--enable-new-api-stack", "--as-test"]
)
py_test(
    name = "examples/gpus/fractional_0.5_gpus_per_learner",
    main = "examples/gpus/fractional_gpus_per_learner.py",
    tags = ["team:rllib", "exclusive", "examples", "multi_gpu"],
    size = "medium",
    srcs = ["examples/gpus/fractional_gpus_per_learner.py"],
    args = ["--enable-new-api-stack", "--as-test", "--stop-reward=40.0", "--num-learners=1", "--num-gpus-per-learner=0.5"]
)
py_test(
    name = "examples/gpus/fractional_0.2_gpus_per_learner",
    main = "examples/gpus/fractional_gpus_per_learner.py",
    tags = ["team:rllib", "exclusive", "examples", "gpu"],
    size = "medium",
    srcs = ["examples/gpus/fractional_gpus_per_learner.py"],
    args = ["--enable-new-api-stack", "--as-test", "--stop-reward=40.0", "--num-learners=1", "--num-gpus-per-learner=0.2"]
)

# subdirectory: hierarchical/
# ....................................
#@OldAPIStack
py_test(
    name = "examples/hierarchical/hierarchical_training_tf",
    main = "examples/hierarchical/hierarchical_training.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/hierarchical/hierarchical_training.py"],
    args = [ "--framework=tf", "--stop-reward=0.0"]
)

#@OldAPIStack
py_test(
    name = "examples/hierarchical/hierarchical_training_torch",
    main = "examples/hierarchical/hierarchical_training.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/hierarchical/hierarchical_training.py"],
    args = ["--framework=torch", "--stop-reward=0.0"]
)

# subdirectory: inference/
# ....................................
py_test(
    name = "examples/inference/policy_inference_after_training",
    main = "examples/inference/policy_inference_after_training.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/inference/policy_inference_after_training.py"],
    args = ["--enable-new-api-stack", "--stop-reward=100.0"]
)

py_test(
    name = "examples/inference/policy_inference_after_training_w_connector",
    main = "examples/inference/policy_inference_after_training_w_connector.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/inference/policy_inference_after_training_w_connector.py"],
    args = ["--enable-new-api-stack", "--stop-reward=150.0"]
)

#@OldAPIStack
py_test(
    name = "examples/inference/policy_inference_after_training_with_attention_tf",
    main = "examples/inference/policy_inference_after_training_with_attention.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/inference/policy_inference_after_training_with_attention.py"],
    args = ["--stop-iters=2", "--framework=tf"]
)

#@OldAPIStack
py_test(
    name = "examples/inference/policy_inference_after_training_with_attention_torch",
    main = "examples/inference/policy_inference_after_training_with_attention.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/inference/policy_inference_after_training_with_attention.py"],
    args = ["--stop-iters=2", "--framework=torch"]
)

#@OldAPIStack
py_test(
    name = "examples/inference/policy_inference_after_training_with_lstm_tf",
    main = "examples/inference/policy_inference_after_training_with_lstm.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/inference/policy_inference_after_training_with_lstm.py"],
    args = ["--stop-iters=1", "--framework=tf"]
)

#@OldAPIStack
py_test(
    name = "examples/inference/policy_inference_after_training_with_lstm_torch",
    main = "examples/inference/policy_inference_after_training_with_lstm.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/inference/policy_inference_after_training_with_lstm.py"],
    args = ["--stop-iters=1", "--framework=torch"]
)

# subdirectory: learners/
# ....................................
py_test(
    name = "examples/learners/custom_loss_fn_simple",
    main = "examples/learners/custom_loss_fn_simple.py",
    tags = ["team:rllib", "examples"],
    size = "medium",
    srcs = ["examples/learners/custom_loss_fn_simple.py"],
    args = ["--enable-new-api-stack", "--as-test"]
)

py_test(
    name = "examples/learners/separate_vf_lr_and_optimizer",
    main = "examples/learners/separate_vf_lr_and_optimizer.py",
    tags = ["team:rllib", "examples"],
    size = "medium",
    srcs = ["examples/learners/separate_vf_lr_and_optimizer.py"],
    args = ["--enable-new-api-stack", "--as-test"]
)

py_test(
    name = "examples/learners/ppo_with_torch_lr_schedulers",
    main = "examples/learners/ppo_with_torch_lr_schedulers.py",
    tags = ["team:rllib", "examples"],
    size = "medium",
    srcs = ["examples/learners/ppo_with_torch_lr_schedulers.py"],
    args = ["--enable-new-api-stack", "--as-test"]
)


#@OldAPIStack @HybridAPIStack
py_test(
    name = "examples/learners/ppo_tuner_local_cpu_torch",
    main = "examples/learners/ppo_tuner.py",
    tags = ["team:rllib", "examples"],
    size = "medium",
    srcs = ["examples/learners/ppo_tuner.py"],
    args = ["--framework=torch", "--config=local-cpu"]
)

#@OldAPIStack @HybridAPIStack
py_test(
    name = "examples/learners/ppo_tuner_local_cpu_tf2",
    main = "examples/learners/ppo_tuner.py",
    tags = ["team:rllib", "examples"],
    size = "medium",
    srcs = ["examples/learners/ppo_tuner.py"],
    args = ["--framework=tf2", "--config=local-cpu"]
)

#@OldAPIStack @HybridAPIStack
py_test(
    name = "examples/learners/ppo_tuner_local_gpu_torch",
    main = "examples/learners/ppo_tuner.py",
    tags = ["team:rllib", "examples", "gpu"],
    size = "medium",
    srcs = ["examples/learners/ppo_tuner.py"],
    args = ["--framework=torch", "--config=local-gpu"]
)

#@OldAPIStack @HybridAPIStack
py_test(
    name = "examples/learners/ppo_tuner_local_gpu_tf2",
    main = "examples/learners/ppo_tuner.py",
    tags = ["team:rllib", "examples", "gpu", "exclusive"],
    size = "medium",
    srcs = ["examples/learners/ppo_tuner.py"],
    args = ["--framework=tf2", "--config=local-gpu"]
)

#@OldAPIStack @HybridAPIStack
py_test(
    name = "examples/learners/ppo_tuner_remote_cpu_torch",
    main = "examples/learners/ppo_tuner.py",
    tags = ["team:rllib", "examples"],
    size = "medium",
    srcs = ["examples/learners/ppo_tuner.py"],
    args = ["--framework=torch", "--config=remote-cpu"]
)

#@OldAPIStack @HybridAPIStack
py_test(
    name = "examples/learners/ppo_tuner_remote_cpu_tf2",
    main = "examples/learners/ppo_tuner.py",
    tags = ["team:rllib", "examples"],
    size = "medium",
    srcs = ["examples/learners/ppo_tuner.py"],
    args = ["--framework=tf2", "--config=remote-cpu"]
)

#@OldAPIStack @HybridAPIStack
py_test(
    name = "examples/learners/ppo_tuner_remote_gpu_torch",
    main = "examples/learners/ppo_tuner.py",
    tags = ["team:rllib", "examples", "gpu", "exclusive"],
    size = "medium",
    srcs = ["examples/learners/ppo_tuner.py"],
    args = ["--framework=torch", "--config=remote-gpu"]
)

#@OldAPIStack @HybridAPIStack
py_test(
    name = "examples/learners/ppo_tuner_remote_gpu_tf2",
    main = "examples/learners/ppo_tuner.py",
    tags = ["team:rllib", "examples", "gpu", "exclusive"],
    size = "medium",
    srcs = ["examples/learners/ppo_tuner.py"],
    args = ["--framework=tf2", "--config=remote-gpu"]
)

#@OldAPIStack @HybridAPIStack
py_test(
    name = "examples/learners/ppo_tuner_multi_gpu_torch",
    main = "examples/learners/ppo_tuner.py",
    tags = ["team:rllib", "examples", "multi_gpu", "exclusive"],
    size = "medium",
    srcs = ["examples/learners/ppo_tuner.py"],
    args = ["--framework=torch", "--config=multi-gpu-ddp"]
)

#@OldAPIStack @HybridAPIStack
py_test(
    name = "examples/learners/ppo_tuner_multi_gpu_tf2",
    main = "examples/learners/ppo_tuner.py",
    tags = ["team:rllib", "examples", "multi_gpu", "exclusive"],
    size = "medium",
    srcs = ["examples/learners/ppo_tuner.py"],
    args = ["--framework=tf2", "--config=multi-gpu-ddp"]
)

#@OldAPIStack @HybridAPIStack
py_test(
    name = "examples/learners/train_w_bc_finetune_w_ppo",
    main = "examples/learners/train_w_bc_finetune_w_ppo.py",
    tags = ["team:rllib", "examples", "exclusive"],
    size = "medium",
    srcs = ["examples/learners/train_w_bc_finetune_w_ppo.py"],
)

# subdirectory: multi_agent/
# ....................................
py_test(
    name = "examples/multi_agent/custom_heuristic_policy",
    main = "examples/multi_agent/custom_heuristic_policy.py",
    tags = ["team:rllib", "exclusive", "examples", "examples_use_all_core"],
    size = "large",
    srcs = ["examples/multi_agent/custom_heuristic_policy.py"],
    args = ["--enable-new-api-stack", "--num-agents=2", "--as-test", "--framework=torch", "--stop-reward=450.0"]
)

py_test(
    name = "examples/multi_agent/different_spaces_for_agents_ppo",
    main = "examples/multi_agent/different_spaces_for_agents.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "small",
    srcs = ["examples/multi_agent/different_spaces_for_agents.py"],
    args = ["--enable-new-api-stack", "--algo=PPO", "--stop-iters=4", "--framework=torch"]
)

py_test(
    name = "examples/multi_agent/multi_agent_cartpole",
    main = "examples/multi_agent/multi_agent_cartpole.py",
    tags = ["team:rllib", "exclusive", "examples", "examples_use_all_core"],
    size = "large",
    srcs = ["examples/multi_agent/multi_agent_cartpole.py"],
    args = ["--enable-new-api-stack", "--num-agents=2", "--as-test", "--framework=torch", "--stop-reward=600.0", "--num-cpus=4"]
)

py_test(
    name = "examples/multi_agent/multi_agent_pendulum_multi_gpu",
    main = "examples/multi_agent/multi_agent_pendulum.py",
    tags = ["team:rllib", "exclusive", "examples", "multi_gpu"],
    size = "large",
    srcs = ["examples/multi_agent/multi_agent_pendulum.py"],
    args = ["--enable-new-api-stack", "--num-agents=2", "--as-test", "--framework=torch", "--stop-reward=-500.0", "--num-cpus=5", "--num-gpus=2"]
)

py_test(
    name = "examples/multi_agent/pettingzoo_independent_learning",
    main = "examples/multi_agent/pettingzoo_independent_learning.py",
    tags = ["team:rllib", "examples"],
    size = "large",
    srcs = ["examples/multi_agent/pettingzoo_independent_learning.py"],
    args = ["--enable-new-api-stack", "--num-agents=2", "--as-test", "--framework=torch", "--stop-reward=-200.0", "--num-cpus=4"]
)

py_test(
    name = "examples/multi_agent/pettingzoo_parameter_sharing",
    main = "examples/multi_agent/pettingzoo_parameter_sharing.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "large",
    srcs = ["examples/multi_agent/pettingzoo_parameter_sharing.py"],
    args = ["--enable-new-api-stack", "--num-agents=2", "--as-test", "--framework=torch", "--stop-reward=-210.0", "--num-cpus=4"],
)

# TODO (sven): Activate this test once this script is ready.
# py_test(
#    name = "examples/multi_agent/pettingzoo_shared_value_function",
#    main = "examples/multi_agent/pettingzoo_shared_value_function.py",
#    tags = ["team:rllib", "exclusive", "examples"],
#    size = "large",
#    srcs = ["examples/multi_agent/pettingzoo_shared_value_function.py"],
#    args = ["--enable-new-api-stack", "--num-agents=2", "--as-test", "--framework=torch", "--stop-reward=-100.0", "--num-cpus=4"],
# )

py_test(
    name = "examples/checkpoints/restore_1_of_n_agents_from_checkpoint",
    main = "examples/checkpoints/restore_1_of_n_agents_from_checkpoint.py",
    tags = ["team:rllib", "exclusive", "examples", "examples_use_all_core", "no_main"],
    size = "large",
    srcs = ["examples/checkpoints/restore_1_of_n_agents_from_checkpoint.py"],
    args = ["--enable-new-api-stack", "--num-agents=2", "--framework=torch", "--checkpoint-freq=20", "--checkpoint-at-end", "--num-cpus=4", "--algo=PPO"]
)

py_test(
    name = "examples/multi_agent/rock_paper_scissors_heuristic_vs_learned",
    main = "examples/multi_agent/rock_paper_scissors_heuristic_vs_learned.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/multi_agent/rock_paper_scissors_heuristic_vs_learned.py"],
    args = ["--enable-new-api-stack", "--num-agents=2", "--as-test", "--framework=torch", "--stop-reward=6.5"],
)

py_test(
    name = "examples/multi_agent/rock_paper_scissors_heuristic_vs_learned_w_lstm",
    main = "examples/multi_agent/rock_paper_scissors_heuristic_vs_learned.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "large",
    srcs = ["examples/multi_agent/rock_paper_scissors_heuristic_vs_learned.py"],
    args = ["--enable-new-api-stack", "--num-agents=2", "--as-test", "--framework=torch", "--stop-reward=7.2", "--use-lstm", "--num-env-runners=4", "--num-cpus=6"],
)

py_test(
    name = "examples/multi_agent/rock_paper_scissors_learned_vs_learned",
    main = "examples/multi_agent/rock_paper_scissors_learned_vs_learned.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/multi_agent/rock_paper_scissors_learned_vs_learned.py"],
    args = ["--enable-new-api-stack", "--num-agents=2", "--framework=torch", "--stop-iter=10"],
)

# @OldAPIStack
py_test(
    name = "examples/multi_agent/self_play_with_open_spiel_connect_4_ppo_tf_old_api_stack",
    main = "examples/multi_agent/self_play_with_open_spiel.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/multi_agent/self_play_with_open_spiel.py"],
    args = ["--framework=tf", "--env=connect_four", "--win-rate-threshold=0.9", "--num-episodes-human-play=0", "--min-league-size=3"]
)

# @OldAPIStack
py_test(
    name = "examples/multi_agent/self_play_with_open_spiel_connect_4_ppo_torch_old_api_stack",
    main = "examples/multi_agent/self_play_with_open_spiel.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/multi_agent/self_play_with_open_spiel.py"],
    args = ["--framework=torch", "--env=connect_four", "--win-rate-threshold=0.9", "--num-episodes-human-play=0", "--min-league-size=3"]
)

py_test(
    name = "examples/multi_agent/self_play_with_open_spiel_connect_4_ppo_torch",
    main = "examples/multi_agent/self_play_with_open_spiel.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/multi_agent/self_play_with_open_spiel.py"],
    args = ["--enable-new-api-stack", "--framework=torch", "--env=connect_four", "--win-rate-threshold=0.9", "--num-episodes-human-play=0", "--min-league-size=4"]
)

py_test(
    name = "examples/multi_agent/self_play_league_based_with_open_spiel_connect_4_ppo_torch",
    main = "examples/multi_agent/self_play_league_based_with_open_spiel.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "large",
    srcs = ["examples/multi_agent/self_play_league_based_with_open_spiel.py"],
    args = ["--enable-new-api-stack", "--framework=torch", "--env=connect_four", "--win-rate-threshold=0.8", "--num-episodes-human-play=0", "--min-league-size=8"]
)

# @OldAPIStack
py_test(
    name = "examples/multi_agent/two_algorithms_tf_old_api_stack",
    main = "examples/multi_agent/two_algorithms.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/multi_agent/two_algorithms.py"],
    args = ["--as-test", "--framework=tf", "--stop-reward=70"]
)

# @OldAPIStack
py_test(
    name = "examples/multi_agent/two_algorithms_torch_old_api_stack",
    main = "examples/multi_agent/two_algorithms.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "small",
    srcs = ["examples/multi_agent/two_algorithms.py"],
    args = ["--as-test", "--framework=torch", "--stop-reward=70"]
)

py_test(
    name = "examples/multi_agent/two_step_game_with_grouped_agents",
    main = "examples/multi_agent/two_step_game_with_grouped_agents.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/multi_agent/two_step_game_with_grouped_agents.py"],
    args = ["--enable-new-api-stack", "--num-agents=2", "--as-test", "--framework=torch", "--stop-reward=7.0"]
)

# subdirectory: offline_rl/
# ....................................

# @HybridAPIStack
# py_test(
#     name = "examples/offline_rl/pretrain_bc_single_agent_evaluate_as_multi_agent",
#     main = "examples/offline_rl/pretrain_bc_single_agent_evaluate_as_multi_agent.py",
#     tags = ["team:rllib", "exclusive", "examples"],
#     size = "large",
#     srcs = ["examples/offline_rl/pretrain_bc_single_agent_evaluate_as_multi_agent.py"],
#     data = ["tests/data/cartpole/large.json"],
#     args = ["--as-test"]
# )

#@OldAPIStack
# TODO (sven): Doesn't seem to learn at the moment. Uncomment once fixed.
# py_test(
#    name = "examples/offline_rl/custom_input_api_cql_torch",
#    main = "examples/offline_rl/custom_input_api.py",
#    tags = ["team:rllib", "exclusive", "examples"],
#    size = "medium",
#    srcs = ["examples/offline_rl/custom_input_api.py"],
#    args = ["--as-test", "--stop-reward=-300", "--stop-iters=1"]
# )

#@OldAPIStack
py_test(
    name = "examples/offline_rl/offline_rl_torch_old_api_stack",
    main = "examples/offline_rl/offline_rl.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/offline_rl/offline_rl.py"],
    args = ["--as-test", "--stop-reward=-300", "--stop-iters=1"]
)

# subdirectory: ray_serve/
# ....................................
# TODO (sven): Uncomment once the problem with the path on BAZEL is solved.
# py_test(
#    name = "examples/ray_serve/ray_serve_with_rllib",
#    main = "examples/ray_serve/ray_serve_with_rllib.py",
#    tags = ["team:rllib", "exclusive", "examples"],
#    size = "medium",
#    srcs = ["examples/ray_serve/ray_serve_with_rllib.py"],
#    args = ["--train-iters=2", "--serve-episodes=2", "--no-render"]
# )

# subdirectory: ray_tune/
# ....................................
py_test(
    name = "examples/ray_tune/custom_experiment",
    main = "examples/ray_tune/custom_experiment.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/ray_tune/custom_experiment.py"],
)

py_test(
    name = "examples/ray_tune/custom_logger",
    main = "examples/ray_tune/custom_logger.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/ray_tune/custom_logger.py"],
)

py_test(
    name = "examples/ray_tune/custom_progress_reporter",
    main = "examples/ray_tune/custom_progress_reporter.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/ray_tune/custom_progress_reporter.py"],
)

# subdirectory: rl_modules/
# ....................................
py_test(
    name = "examples/rl_modules/action_masking_rl_module",
    main = "examples/rl_modules/action_masking_rl_module.py",
    tags = ["team:rllib", "examples"],
    size = "medium",
    srcs = ["examples/rl_modules/action_masking_rl_module.py"],
    args = ["--enable-new-api-stack", "--stop-iters=5"],
)

py_test(
    name = "examples/rl_modules/autoregressive_actions_rl_module",
    main = "examples/rl_modules/autoregressive_actions_rl_module.py",
    tags = ["team:rllib", "examples"],
    size = "medium",
    srcs = ["examples/rl_modules/autoregressive_actions_rl_module.py"],
    args = ["--enable-new-api-stack"],
)
py_test(
    name = "examples/rl_modules/custom_cnn_rl_module",
    main = "examples/rl_modules/custom_cnn_rl_module.py",
    tags = ["team:rllib", "examples"],
    size = "medium",
    srcs = ["examples/rl_modules/custom_cnn_rl_module.py"],
    args = ["--enable-new-api-stack", "--stop-iters=3"],
)
py_test(
    name = "examples/rl_modules/custom_lstm_rl_module",
    main = "examples/rl_modules/custom_lstm_rl_module.py",
    tags = ["team:rllib", "examples"],
    size = "large",
    srcs = ["examples/rl_modules/custom_lstm_rl_module.py"],
    args = ["--as-test", "--enable-new-api-stack"],
)
py_test(
    name = "examples/rl_modules/classes/mobilenet_rlm",
    main = "examples/rl_modules/classes/mobilenet_rlm.py",
    tags = ["team:rllib", "examples", "no_main"],
    size = "small",
    srcs = ["examples/rl_modules/classes/mobilenet_rlm.py"],
)

py_test(
    name = "examples/rl_modules/migrate_modelv2_to_new_api_stack_by_config",
    main = "examples/rl_modules/migrate_modelv2_to_new_api_stack_by_config.py",
    tags = ["team:rllib", "examples"],
    size = "large",
    srcs = ["examples/rl_modules/migrate_modelv2_to_new_api_stack_by_config.py"],
)
py_test(
    name = "examples/rl_modules/migrate_modelv2_to_new_api_stack_by_policy_checkpoint",
    main = "examples/rl_modules/migrate_modelv2_to_new_api_stack_by_policy_checkpoint.py",
    tags = ["team:rllib", "examples"],
    size = "large",
    srcs = ["examples/rl_modules/migrate_modelv2_to_new_api_stack_by_policy_checkpoint.py"],
)
py_test(
    name = "examples/rl_modules/pretraining_single_agent_training_multi_agent",
    main = "examples/rl_modules/pretraining_single_agent_training_multi_agent.py",
    tags = ["team:rllib", "examples"],
    size = "medium",
    srcs = ["examples/rl_modules/pretraining_single_agent_training_multi_agent.py"],
    args = ["--enable-new-api-stack", "--num-agents=2", "--stop-iters-pretraining=5", "--stop-iters=20", "--stop-reward=150.0"],
)

#@OldAPIStack
py_test(
    name = "examples/autoregressive_action_dist_tf",
    main = "examples/autoregressive_action_dist.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/autoregressive_action_dist.py"],
    args = ["--as-test", "--framework=tf", "--stop-reward=150", "--num-cpus=4"]
)

#@OldAPIStack
py_test(
    name = "examples/autoregressive_action_dist_torch",
    main = "examples/autoregressive_action_dist.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/autoregressive_action_dist.py"],
    args = ["--as-test", "--framework=torch", "--stop-reward=150", "--num-cpus=4"]
)

#@OldAPIStack
py_test(
    name = "examples/cartpole_lstm_impala_tf2",
    main = "examples/cartpole_lstm.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/cartpole_lstm.py"],
    args = ["--run=IMPALA", "--as-test", "--framework=tf2", "--stop-reward=28", "--num-cpus=4"]
)

#@OldAPIStack
py_test(
    name = "examples/cartpole_lstm_impala_torch",
    main = "examples/cartpole_lstm.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/cartpole_lstm.py"],
    args = ["--run=IMPALA", "--as-test", "--framework=torch", "--stop-reward=28", "--num-cpus=4"]
)

#@OldAPIStack
py_test(
    name = "examples/cartpole_lstm_ppo_tf2",
    main = "examples/cartpole_lstm.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "large",
    srcs = ["examples/cartpole_lstm.py"],
    args = ["--run=PPO", "--as-test", "--framework=tf2", "--stop-reward=28", "--num-cpus=4"]
)

#@OldAPIStack
py_test(
    name = "examples/cartpole_lstm_ppo_torch",
    main = "examples/cartpole_lstm.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/cartpole_lstm.py"],
    args = ["--run=PPO", "--as-test", "--framework=torch", "--stop-reward=28", "--num-cpus=4"]
)

#@OldAPIStack
py_test(
    name = "examples/cartpole_lstm_ppo_torch_with_prev_a_and_r",
    main = "examples/cartpole_lstm.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/cartpole_lstm.py"],
    args = ["--run=PPO", "--as-test", "--framework=torch", "--stop-reward=28", "--num-cpus=4", "--use-prev-action",  "--use-prev-reward"]
)

#@OldAPIStack
py_test(
    name = "examples/centralized_critic_tf",
    main = "examples/centralized_critic.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/centralized_critic.py"],
    args = ["--as-test", "--framework=tf", "--stop-reward=7.2"]
)

#@OldAPIStack
py_test(
    name = "examples/centralized_critic_torch",
    main = "examples/centralized_critic.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/centralized_critic.py"],
    args = ["--as-test", "--framework=torch", "--stop-reward=7.2"]
)

#@OldAPIStack
py_test(
    name = "examples/centralized_critic_2_tf",
    main = "examples/centralized_critic_2.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/centralized_critic_2.py"],
    args = ["--as-test", "--framework=tf", "--stop-reward=6.0"]
)

#@OldAPIStack
py_test(
    name = "examples/centralized_critic_2_torch",
    main = "examples/centralized_critic_2.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "medium",
    srcs = ["examples/centralized_critic_2.py"],
    args = ["--as-test", "--framework=torch", "--stop-reward=6.0"]
)

#@OldAPIStack
py_test(
    name = "examples/metrics/custom_metrics_and_callbacks",
    main = "examples/metrics/custom_metrics_and_callbacks.py",
    tags = ["team:rllib", "exclusive", "examples"],
    size = "small",
    srcs = ["examples/metrics/custom_metrics_and_callbacks.py"],
    args = ["--stop-iters=2"]
)

#@OldAPIStack
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

#@OldAPIStack
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
    name = "examples/replay_buffer_api",
    tags = ["team:rllib", "examples"],
    size = "large",
    srcs = ["examples/replay_buffer_api.py"],
)


# --------------------------------------------------------------------
# Manual/disabled tests
# --------------------------------------------------------------------
py_test_module_list(
  files = [
    "utils/tests/test_utils.py",
  ],
  size = "large",
  extra_srcs = [],
  deps = [],
  tags = ["manual", "team:rllib", "no_main"],
)
