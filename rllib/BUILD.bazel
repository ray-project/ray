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

load("@rules_python//python:defs.bzl", "py_test")
load("//bazel:python.bzl", "doctest", "py_test_module_list")

filegroup(
    name = "cartpole-v1_large",
    data = glob(["tests/data/cartpole/cartpole-v1_large/*.parquet"]),
    visibility = ["//visibility:public"],
)

doctest(
    size = "enormous",
    data = glob(["tests/data/cartpole/cartpole-v1_large/*.parquet"]),
    files = glob(
        ["**/*.py"],
        exclude = [
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
            "core/distribution/torch/torch_distribution.py",
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
            "models/torch/model.py",
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
    size = "medium",
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
# This will test python/yaml config files
# inside rllib/tuned_examples/[algo-name] for actual learning success.
# --------------------------------------------------------------------

# APPO
# CartPole
py_test(
    name = "learning_tests_cartpole_appo",
    size = "large",
    srcs = ["tuned_examples/appo/cartpole_appo.py"],
    args = [
        "--as-test",
        "--num-cpus=7",
        "--num-env-runners=5",
    ],
    main = "tuned_examples/appo/cartpole_appo.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_discrete",
        "team:rllib",
        "torch_only",
    ],
)

# TODO (sven): For some weird reason, this test runs extremely slow on the CI (not on cluster, not locally) -> taking this out for now ...
# py_test(
#    name = "learning_tests_cartpole_appo_gpu",
#    main = "tuned_examples/appo/cartpole_appo.py",
#    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_discrete", "learning_tests_pytorch_use_all_core", "gpu"],
#    size = "large",
#    srcs = ["tuned_examples/appo/cartpole_appo.py"],
#    args = ["--as-test", "--num-gpus-per-learner=1", "--num-cpus=7", "--num-env-runners=5"]
# )
py_test(
    name = "learning_tests_cartpole_appo_multi_cpu",
    size = "large",
    srcs = ["tuned_examples/appo/cartpole_appo.py"],
    args = [
        "--as-test",
        "--num-learners=2",
        "--num-cpus=9",
        "--num-env-runners=6",
    ],
    main = "tuned_examples/appo/cartpole_appo.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_discrete",
        "learning_tests_pytorch_use_all_core",
        "team:rllib",
        "torch_only",
    ],
)

py_test(
    name = "learning_tests_cartpole_appo_multi_gpu",
    size = "large",
    srcs = ["tuned_examples/appo/cartpole_appo.py"],
    args = [
        "--as-test",
        "--num-learners=2",
        "--num-gpus-per-learner=1",
        "--num-cpus=7",
        "--num-env-runners=6",
    ],
    main = "tuned_examples/appo/cartpole_appo.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_discrete",
        "learning_tests_pytorch_use_all_core",
        "multi_gpu",
        "team:rllib",
        "torch_only",
    ],
)

# MultiAgentCartPole
py_test(
    name = "learning_tests_multi_agent_cartpole_appo",
    size = "large",
    srcs = ["tuned_examples/appo/multi_agent_cartpole_appo.py"],
    args = [
        "--as-test",
        "--num-agents=2",
        "--num-cpus=8",
        "--num-env-runners=6",
    ],
    main = "tuned_examples/appo/multi_agent_cartpole_appo.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_discrete",
        "team:rllib",
        "torch_only",
    ],
)

py_test(
    name = "learning_tests_multi_agent_cartpole_appo_gpu",
    size = "large",
    srcs = ["tuned_examples/appo/multi_agent_cartpole_appo.py"],
    args = [
        "--as-test",
        "--num-agents=2",
        "--num-gpus-per-learner=1",
        "--num-cpus=7",
        "--num-env-runners=5",
    ],
    main = "tuned_examples/appo/multi_agent_cartpole_appo.py",
    tags = [
        "exclusive",
        "gpu",
        "learning_tests",
        "learning_tests_discrete",
        "learning_tests_pytorch_use_all_core",
        "team:rllib",
        "torch_only",
    ],
)

py_test(
    name = "learning_tests_multi_agent_cartpole_appo_multi_cpu",
    size = "large",
    srcs = ["tuned_examples/appo/multi_agent_cartpole_appo.py"],
    args = [
        "--as-test",
        "--num-agents=2",
        "--num-learners=2",
        "--num-cpus=9",
        "--num-env-runners=6",
    ],
    main = "tuned_examples/appo/multi_agent_cartpole_appo.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_discrete",
        "learning_tests_pytorch_use_all_core",
        "team:rllib",
        "torch_only",
        # Test is failing: https://github.com/ray-project/ray/issues/52270
        "manual",
    ],
)

py_test(
    name = "learning_tests_multi_agent_cartpole_appo_multi_gpu",
    size = "large",
    srcs = ["tuned_examples/appo/multi_agent_cartpole_appo.py"],
    args = [
        "--as-test",
        "--num-agents=2",
        "--num-learners=2",
        "--num-gpus-per-learner=1",
        "--num-cpus=7",
        "--num-env-runners=6",
    ],
    main = "tuned_examples/appo/multi_agent_cartpole_appo.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_discrete",
        "learning_tests_pytorch_use_all_core",
        "multi_gpu",
        "team:rllib",
        "torch_only",
    ],
)

# StatelessCartPole
py_test(
    name = "learning_tests_stateless_cartpole_appo",
    size = "large",
    srcs = ["tuned_examples/appo/stateless_cartpole_appo.py"],
    args = [
        "--as-test",
        "--num-cpus=8",
        "--num-env-runners=6",
    ],
    main = "tuned_examples/appo/stateless_cartpole_appo.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_discrete",
        "learning_tests_pytorch_use_all_core",
        "team:rllib",
        "torch_only",
    ],
)

py_test(
    name = "learning_tests_stateless_cartpole_appo_gpu",
    size = "large",
    srcs = ["tuned_examples/appo/stateless_cartpole_appo.py"],
    args = [
        "--as-test",
        "--num-agents=2",
        "--num-gpus-per-learner=1",
        "--num-cpus=7",
        "--num-env-runners=5",
    ],
    main = "tuned_examples/appo/stateless_cartpole_appo.py",
    tags = [
        "exclusive",
        "gpu",
        "learning_tests",
        "learning_tests_discrete",
        "learning_tests_pytorch_use_all_core",
        "team:rllib",
        "torch_only",
    ],
)

py_test(
    name = "learning_tests_stateless_cartpole_appo_multi_cpu",
    size = "large",
    srcs = ["tuned_examples/appo/stateless_cartpole_appo.py"],
    args = [
        "--as-test",
        "--num-learners=2",
        "--num-cpus=9",
        "--num-env-runners=6",
    ],
    main = "tuned_examples/appo/stateless_cartpole_appo.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_discrete",
        "learning_tests_pytorch_use_all_core",
        "team:rllib",
        "torch_only",
    ],
)

py_test(
    name = "learning_tests_stateless_cartpole_appo_multi_gpu",
    size = "large",
    srcs = ["tuned_examples/appo/stateless_cartpole_appo.py"],
    args = [
        "--as-test",
        "--num-learners=2",
        "--num-gpus-per-learner=1",
        "--num-cpus=7",
        "--num-env-runners=6",
    ],
    main = "tuned_examples/appo/stateless_cartpole_appo.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_discrete",
        "learning_tests_pytorch_use_all_core",
        "multi_gpu",
        "team:rllib",
        "torch_only",
    ],
)

# MultiAgentStatelessCartPole
# py_test(
#     name = "learning_tests_multi_agent_stateless_cartpole_appo",
#     main = "tuned_examples/appo/multi_agent_stateless_cartpole_appo.py",
#     tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_discrete", "learning_tests_pytorch_use_all_core"],
#     size = "large",
#     srcs = ["tuned_examples/appo/multi_agent_stateless_cartpole_appo.py"],
#     args = ["--as-test"]
# )
# py_test(
#     name = "learning_tests_multi_agent_stateless_cartpole_appo_gpu",
#     main = "tuned_examples/appo/multi_agent_stateless_cartpole_appo.py",
#     tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_discrete", "learning_tests_pytorch_use_all_core", "gpu"],
#     size = "large",
#     srcs = ["tuned_examples/appo/multi_agent_stateless_cartpole_appo.py"],
#     args = ["--as-test", "--num-agents=2", "--num-gpus-per-learner=1"]
# )
# py_test(
#     name = "learning_tests_multi_agent_stateless_cartpole_appo_multi_cpu",
#     main = "tuned_examples/appo/multi_agent_stateless_cartpole_appo.py",
#     tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_discrete", "learning_tests_pytorch_use_all_core"],
#     size = "large",
#     srcs = ["tuned_examples/appo/multi_agent_stateless_cartpole_appo.py"],
#     args = ["--as-test", "--num-learners=2"]
# )
# py_test(
#     name = "learning_tests_multi_agent_stateless_cartpole_appo_multi_gpu",
#     main = "tuned_examples/appo/multi_agent_stateless_cartpole_appo.py",
#     tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_discrete", "learning_tests_pytorch_use_all_core", "multi_gpu"],
#     size = "large",
#     srcs = ["tuned_examples/appo/multi_agent_stateless_cartpole_appo.py"],
#     args = ["--as-test", "--num-learners=2", "--num-gpus-per-learner=1"]
# )
# Pendulum
py_test(
    name = "learning_tests_pendulum_appo",
    size = "large",
    srcs = ["tuned_examples/appo/pendulum_appo.py"],
    args = [
        "--as-test",
        "--num-cpus=6",
        "--num-env-runners=4",
    ],
    main = "tuned_examples/appo/pendulum_appo.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_continuous",
        "team:rllib",
        "torch_only",
    ],
)

# MultiAgentPong (multi-GPU smoke test)
py_test(
    name = "learning_tests_multi_agent_pong_appo_multi_gpu",
    size = "large",
    srcs = ["tuned_examples/appo/multi_agent_pong_appo.py"],
    args = [
        "--stop-iters=3",
        "--num-agents=2",
        "--num-learners=2",
        "--num-gpus-per-learner=1",
        "--num-aggregator-actors-per-learner=1",
    ],
    main = "tuned_examples/appo/multi_agent_pong_appo.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_discrete",
        "multi_gpu",
        "team:rllib",
        "torch_only",
    ],
)

#@OldAPIStack
py_test(
    name = "learning_tests_multi_agent_cartpole_w_100_policies_appo_old_api_stack",
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    args = ["--dir=tuned_examples/appo"],
    data = ["tuned_examples/appo/multi-agent-cartpole-w-100-policies-appo.py"],
    main = "tests/run_regression_tests.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_discrete",
        "learning_tests_pytorch_use_all_core",
        "team:rllib",
    ],
)

# BC
# CartPole
py_test(
    name = "learning_tests_cartpole_bc",
    size = "medium",
    srcs = ["tuned_examples/bc/cartpole_bc.py"],
    args = [
        "--as-test",
    ],
    # Include the offline data files.
    data = [
        "tests/data/cartpole/cartpole-v1_large",
    ],
    main = "tuned_examples/bc/cartpole_bc.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_discrete",
        "learning_tests_pytorch_use_all_core",
        "team:rllib",
        "torch_only",
    ],
)

py_test(
    name = "learning_tests_cartpole_bc_gpu",
    size = "medium",
    srcs = ["tuned_examples/bc/cartpole_bc.py"],
    args = [
        "--as-test",
        "--num-gpus-per-learner=1",
    ],
    # Include the offline data files.
    data = [
        "tests/data/cartpole/cartpole-v1_large",
    ],
    main = "tuned_examples/bc/cartpole_bc.py",
    tags = [
        "exclusive",
        "gpu",
        "learning_tests",
        "learning_tests_discrete",
        "learning_tests_pytorch_use_all_core",
        "team:rllib",
        "torch_only",
        # Disabled: https://github.com/ray-project/ray/issues/50532
        "manual",
    ],
)

py_test(
    name = "learning_tests_cartpole_bc_with_offline_evaluation",
    size = "medium",
    srcs = ["tuned_examples/bc/cartpole_bc_with_offline_evaluation.py"],
    args = [
        "--as-test",
        "--offline-evaluation-interval=1",
        "--num-offline-eval-runners=2",
    ],
    # Include the offline data files.
    data = [
        "tests/data/cartpole/cartpole-v1_large",
    ],
    main = "tuned_examples/bc/cartpole_bc_with_offline_evaluation.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_discrete",
        "learning_tests_pytorch_use_all_core",
        "team:rllib",
        "torch_only",
    ],
)

py_test(
    name = "learning_tests_cartpole_bc_with_offline_evaluation_gpu",
    size = "medium",
    srcs = ["tuned_examples/bc/cartpole_bc_with_offline_evaluation.py"],
    args = [
        "--as-test",
        "--num-gpus-per-learner=1",
        "--offline-evaluation-interval=1",
        "--num-offline-eval-runners=2",
        "--num-gpus-per-offline-eval-runner=0.5",
    ],
    # Include the offline data files.
    data = [
        "tests/data/cartpole/cartpole-v1_large",
    ],
    main = "tuned_examples/bc/cartpole_bc_with_offline_evaluation.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_discrete",
        "learning_tests_pytorch_use_all_core",
        "multi_gpu",
        "team:rllib",
        "torch_only",
    ],
)

# CQL
# Pendulum
py_test(
    name = "learning_tests_pendulum_cql",
    size = "large",
    srcs = ["tuned_examples/cql/pendulum_cql.py"],
    args = [
        "--as-test",
    ],
    # Include the zipped json data file as well.
    data = [
        "tests/data/pendulum/pendulum-v1_enormous",
    ],
    main = "tuned_examples/cql/pendulum_cql.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_cartpole",
        "learning_tests_discrete",
        "learning_tests_pytorch_use_all_core",
        "team:rllib",
        "torch_only",
        # Disabled: https://github.com/ray-project/ray/issues/43808
        "manual",
    ],
)

# GPU training.
py_test(
    name = "learning_tests_pendulum_cql_gpu",
    size = "large",
    srcs = ["tuned_examples/cql/pendulum_cql.py"],
    args = [
        "--as-test",
        "--num-gpus-per-learner=1",
    ],
    # Include the zipped json data file as well.
    data = [
        "tests/data/pendulum/pendulum-v1_enormous",
    ],
    main = "tuned_examples/cql/pendulum_cql.py",
    tags = [
        "exclusive",
        "gpu",
        "learning_tests",
        "learning_tests_cartpole",
        "learning_tests_discrete",
        "learning_tests_pytorch_use_all_core",
        "team:rllib",
        "torch_only",
        # Disabled: https://github.com/ray-project/ray/issues/50538
        "manual",
    ],
)

# DQN
# CartPole
py_test(
    name = "learning_tests_cartpole_dqn",
    size = "large",
    srcs = ["tuned_examples/dqn/cartpole_dqn.py"],
    args = [
        "--as-test",
    ],
    main = "tuned_examples/dqn/cartpole_dqn.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_discrete",
        "learning_tests_pytorch_use_all_core",
        "team:rllib",
        "torch_only",
    ],
)

py_test(
    name = "learning_tests_cartpole_dqn_gpu",
    size = "large",
    srcs = ["tuned_examples/dqn/cartpole_dqn.py"],
    args = [
        "--as-test",
        "--num-learners=1",
        "--num-gpus-per-learner=1",
    ],
    main = "tuned_examples/dqn/cartpole_dqn.py",
    tags = [
        "exclusive",
        "gpu",
        "learning_tests",
        "learning_tests_discrete",
        "learning_tests_pytorch_use_all_core",
        "team:rllib",
        "torch_only",
    ],
)

py_test(
    name = "learning_tests_cartpole_dqn_multi_cpu",
    size = "large",
    srcs = ["tuned_examples/dqn/cartpole_dqn.py"],
    args = [
        "--as-test",
        "--num-learners=2",
    ],
    main = "tuned_examples/dqn/cartpole_dqn.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_discrete",
        "learning_tests_pytorch_use_all_core",
        "team:rllib",
        "torch_only",
    ],
)

py_test(
    name = "learning_tests_cartpole_dqn_multi_gpu",
    size = "large",
    srcs = ["tuned_examples/dqn/cartpole_dqn.py"],
    args = [
        "--as-test",
        "--num-learners=2",
        "--num-gpus-per-learner=1",
    ],
    main = "tuned_examples/dqn/cartpole_dqn.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_discrete",
        "learning_tests_pytorch_use_all_core",
        "multi_gpu",
        "team:rllib",
        "torch_only",
        # Disabled: https://github.com/ray-project/ray/issues/47216
        "manual",
    ],
)

# MultiAgentCartPole
py_test(
    name = "learning_tests_multi_agent_cartpole_dqn",
    size = "large",
    srcs = ["tuned_examples/dqn/multi_agent_cartpole_dqn.py"],
    args = [
        "--as-test",
        "--num-agents=2",
        "--num-cpus=4",
    ],
    main = "tuned_examples/dqn/multi_agent_cartpole_dqn.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_discrete",
        "learning_tests_pytorch_use_all_core",
        "team:rllib",
        "torch_only",
    ],
)

py_test(
    name = "learning_tests_multi_agent_cartpole_dqn_gpu",
    size = "large",
    srcs = ["tuned_examples/dqn/multi_agent_cartpole_dqn.py"],
    args = [
        "--as-test",
        "--num-agents=2",
        "--num-cpus=4",
        "--num-learners=1",
        "--num-gpus-per-learner=1",
    ],
    main = "tuned_examples/dqn/multi_agent_cartpole_dqn.py",
    tags = [
        "exclusive",
        "gpu",
        "learning_tests",
        "learning_tests_discrete",
        "learning_tests_pytorch_use_all_core",
        "team:rllib",
        "torch_only",
    ],
)

py_test(
    name = "learning_tests_multi_agent_cartpole_dqn_multi_cpu",
    size = "large",
    srcs = ["tuned_examples/dqn/multi_agent_cartpole_dqn.py"],
    args = [
        "--as-test",
        "--num-agents=2",
        "--num-cpus=5",
        "--num-learners=2",
    ],
    main = "tuned_examples/dqn/multi_agent_cartpole_dqn.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_discrete",
        "learning_tests_pytorch_use_all_core",
        "team:rllib",
        "torch_only",
    ],
)

py_test(
    name = "learning_tests_multi_agent_cartpole_dqn_multi_gpu",
    size = "large",
    srcs = ["tuned_examples/dqn/multi_agent_cartpole_dqn.py"],
    args = [
        "--as-test",
        "--num-agents=2",
        "--num-cpus=4",
        "--num-learners=2",
        "--num-gpus-per-learner=1",
    ],
    main = "tuned_examples/dqn/multi_agent_cartpole_dqn.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_discrete",
        "learning_tests_pytorch_use_all_core",
        "multi_gpu",
        "team:rllib",
        "torch_only",
    ],
)

# DreamerV3
# takes too long (up to 20-30min to learn -200 on 1 GPU)
# # Pendulum
# py_test(
#    name = "learning_tests_pendulum_dreamerv3_gpu",
#    size = "large",
#    srcs = ["tuned_examples/dreamerv3/pendulum_dreamerv3.py"],
#    args = [
#        "--as-test",
#        "--num-gpus-per-learner=1",
#        "--num-learners=1",
#    ],
#    main = "tuned_examples/marwil/cartpole_marwil.py",
#    tags = [
#        "exclusive",
#        "gpu",
#        "learning_tests",
#        "learning_tests_continuous",
#        "learning_tests_pytorch_use_all_core",
#        "team:rllib",
#        "torch_only",
#    ],
# )

# IMPALA
# CartPole
py_test(
    name = "learning_tests_cartpole_impala",
    size = "large",
    srcs = ["tuned_examples/impala/cartpole_impala.py"],
    args = [
        "--as-test",
    ],
    main = "tuned_examples/impala/cartpole_impala.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_discrete",
        "team:rllib",
        "torch_only",
    ],
)

py_test(
    name = "learning_tests_cartpole_impala_gpu",
    size = "large",
    srcs = ["tuned_examples/impala/cartpole_impala.py"],
    args = [
        "--as-test",
        "--num-gpus-per-learner=1",
    ],
    main = "tuned_examples/impala/cartpole_impala.py",
    tags = [
        "exclusive",
        "gpu",
        "learning_tests",
        "learning_tests_discrete",
        "learning_tests_pytorch_use_all_core",
        "team:rllib",
        "torch_only",
    ],
)

py_test(
    name = "learning_tests_cartpole_impala_multi_cpu",
    size = "large",
    srcs = ["tuned_examples/impala/cartpole_impala.py"],
    args = [
        "--as-test",
        "--num-learners=2",
    ],
    main = "tuned_examples/impala/cartpole_impala.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_discrete",
        "learning_tests_pytorch_use_all_core",
        "team:rllib",
        "torch_only",
    ],
)

py_test(
    name = "learning_tests_cartpole_impala_multi_gpu",
    size = "large",
    srcs = ["tuned_examples/impala/cartpole_impala.py"],
    args = [
        "--as-test",
        "--num-learners=2",
        "--num-gpus-per-learner=1",
    ],
    main = "tuned_examples/impala/cartpole_impala.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_discrete",
        "learning_tests_pytorch_use_all_core",
        "multi_gpu",
        "team:rllib",
        "torch_only",
    ],
)

# MultiAgentCartPole
py_test(
    name = "learning_tests_multi_agent_cartpole_impala",
    size = "large",
    srcs = ["tuned_examples/impala/multi_agent_cartpole_impala.py"],
    args = [
        "--as-test",
        "--num-agents=2",
        "--num-cpus=6",
    ],
    main = "tuned_examples/impala/multi_agent_cartpole_impala.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_discrete",
        "learning_tests_pytorch_use_all_core",
        "team:rllib",
        "torch_only",
    ],
)

py_test(
    name = "learning_tests_multi_agent_cartpole_impala_gpu",
    size = "large",
    srcs = ["tuned_examples/impala/multi_agent_cartpole_impala.py"],
    args = [
        "--as-test",
        "--num-agents=2",
        "--num-gpus-per-learner=1",
        "--num-cpus=6",
    ],
    main = "tuned_examples/impala/multi_agent_cartpole_impala.py",
    tags = [
        "exclusive",
        "gpu",
        "learning_tests",
        "learning_tests_discrete",
        "learning_tests_pytorch_use_all_core",
        "team:rllib",
        "torch_only",
    ],
)

py_test(
    name = "learning_tests_multi_agent_cartpole_impala_multi_cpu",
    size = "large",
    srcs = ["tuned_examples/impala/multi_agent_cartpole_impala.py"],
    args = [
        "--as-test",
        "--num-agents=2",
        "--num-learners=2",
        "--num-cpus=7",
    ],
    main = "tuned_examples/impala/multi_agent_cartpole_impala.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_discrete",
        "learning_tests_pytorch_use_all_core",
        "team:rllib",
        "torch_only",
    ],
)

py_test(
    name = "learning_tests_multi_agent_cartpole_impala_multi_gpu",
    size = "large",
    srcs = ["tuned_examples/impala/multi_agent_cartpole_impala.py"],
    args = [
        "--as-test",
        "--num-agents=2",
        "--num-learners=2",
        "--num-gpus-per-learner=1",
        "--num-cpus=7",
    ],
    main = "tuned_examples/impala/multi_agent_cartpole_impala.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_discrete",
        "learning_tests_pytorch_use_all_core",
        "multi_gpu",
        "team:rllib",
        "torch_only",
    ],
)

# StatelessCartPole
py_test(
    name = "learning_tests_stateless_cartpole_impala",
    size = "large",
    srcs = ["tuned_examples/impala/stateless_cartpole_impala.py"],
    args = [
        "--as-test",
    ],
    main = "tuned_examples/impala/stateless_cartpole_impala.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_discrete",
        "learning_tests_pytorch_use_all_core",
        "team:rllib",
        "torch_only",
    ],
)

py_test(
    name = "learning_tests_stateless_cartpole_impala_multi_gpu",
    size = "large",
    srcs = ["tuned_examples/impala/stateless_cartpole_impala.py"],
    args = [
        "--as-test",
        "--num-learners=2",
        "--num-gpus-per-learner=1",
    ],
    main = "tuned_examples/impala/stateless_cartpole_impala.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_discrete",
        "learning_tests_pytorch_use_all_core",
        "multi_gpu",
        "team:rllib",
        "torch_only",
    ],
)

# MultiAgentStatelessCartPole
py_test(
    name = "learning_tests_multi_agent_stateless_cartpole_impala",
    size = "large",
    srcs = ["tuned_examples/impala/multi_agent_stateless_cartpole_impala.py"],
    args = [
        "--as-test",
    ],
    main = "tuned_examples/impala/multi_agent_stateless_cartpole_impala.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_discrete",
        "learning_tests_pytorch_use_all_core",
        "team:rllib",
        "torch_only",
    ],
)
# py_test(
#    name = "learning_tests_multi_agent_stateless_cartpole_impala_multi_gpu",
#    main = "tuned_examples/impala/multi_agent_stateless_cartpole_impala.py",
#    tags = ["team:rllib", "exclusive", "learning_tests", "torch_only", "learning_tests_discrete", "learning_tests_pytorch_use_all_core", "multi_gpu"],
#    size = "large",
#    srcs = ["tuned_examples/impala/multi_agent_stateless_cartpole_impala.py"],
#    args = ["--as-test", "--num-learners=2", "--num-gpus-per-learner=1"]
# )

# IQL
# Pendulum-v1 (enormous)
py_test(
    name = "learning_tests_pendulum_iql",
    size = "large",
    srcs = ["tuned_examples/iql/pendulum_iql.py"],
    args = [
        "--as-test",
        "--num-cpus=32",
    ],
    # Include the offline data files.
    data = [
        "tests/data/pendulum/pendulum-v1_enormous",
    ],
    main = "tuned_examples/iql/pendulum_iql.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_continuous",
        "learning_tests_pytorch_use_all_core",
        "team:rllib",
        "torch_only",
    ],
)

# GPU training.
py_test(
    name = "learning_tests_pendulum_iql_gpu",
    size = "large",
    srcs = ["tuned_examples/iql/pendulum_iql.py"],
    args = [
        "--as-test",
        "--num-cpus=32",
        "--num-gpus-per-learner=1",
    ],
    # Include the offline data files.
    data = [
        "tests/data/pendulum/pendulum-v1_enormous",
    ],
    main = "tuned_examples/iql/pendulum_iql.py",
    tags = [
        "exclusive",
        "gpu",
        "learning_tests",
        "learning_tests_continuous",
        "learning_tests_pytorch_use_all_core",
        "team:rllib",
        "torch_only",
    ],
)

# MARWIL
# CartPole
py_test(
    name = "learning_tests_cartpole_marwil",
    size = "large",
    srcs = ["tuned_examples/marwil/cartpole_marwil.py"],
    args = [
        "--as-test",
    ],
    # Include the offline data files.
    data = [
        "tests/data/cartpole/cartpole-v1_large",
    ],
    main = "tuned_examples/marwil/cartpole_marwil.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_discrete",
        "learning_tests_pytorch_use_all_core",
        "team:rllib",
        "torch_only",
    ],
)

# GPU-training.
py_test(
    name = "learning_tests_cartpole_marwil_gpu",
    size = "large",
    srcs = ["tuned_examples/marwil/cartpole_marwil.py"],
    args = [
        "--as-test",
        "--num-gpus-per-learner=1",
    ],
    # Include the offline data files.
    data = [
        "tests/data/cartpole/cartpole-v1_large",
    ],
    main = "tuned_examples/marwil/cartpole_marwil.py",
    tags = [
        "exclusive",
        "gpu",
        "learning_tests",
        "learning_tests_discrete",
        "learning_tests_pytorch_use_all_core",
        "team:rllib",
        "torch_only",
    ],
)

# PPO
# CartPole
py_test(
    name = "learning_tests_cartpole_ppo",
    size = "large",
    srcs = ["tuned_examples/ppo/cartpole_ppo.py"],
    args = [
        "--as-test",
    ],
    main = "tuned_examples/ppo/cartpole_ppo.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_discrete",
        "team:rllib",
        "torch_only",
    ],
)

py_test(
    name = "learning_tests_cartpole_ppo_gpu",
    size = "large",
    srcs = ["tuned_examples/ppo/cartpole_ppo.py"],
    args = [
        "--as-test",
        "--num-learners=1",
        "--num-gpus-per-learner=1",
    ],
    main = "tuned_examples/ppo/cartpole_ppo.py",
    tags = [
        "exclusive",
        "gpu",
        "learning_tests",
        "learning_tests_discrete",
        "learning_tests_pytorch_use_all_core",
        "team:rllib",
        "torch_only",
    ],
)

py_test(
    name = "learning_tests_cartpole_ppo_multi_cpu",
    size = "large",
    srcs = ["tuned_examples/ppo/cartpole_ppo.py"],
    args = [
        "--as-test",
        "--num-learners=2",
    ],
    main = "tuned_examples/ppo/cartpole_ppo.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_discrete",
        "learning_tests_pytorch_use_all_core",
        "team:rllib",
        "torch_only",
    ],
)

py_test(
    name = "learning_tests_cartpole_ppo_multi_gpu",
    size = "large",
    srcs = ["tuned_examples/ppo/cartpole_ppo.py"],
    args = [
        "--as-test",
        "--num-learners=2",
        "--num-gpus-per-learner=1",
    ],
    main = "tuned_examples/ppo/cartpole_ppo.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_discrete",
        "learning_tests_pytorch_use_all_core",
        "multi_gpu",
        "team:rllib",
        "torch_only",
    ],
)

# MultiAgentCartPole
py_test(
    name = "learning_tests_multi_agent_cartpole_ppo",
    size = "large",
    srcs = ["tuned_examples/ppo/multi_agent_cartpole_ppo.py"],
    args = [
        "--as-test",
        "--num-agents=2",
    ],
    main = "tuned_examples/ppo/multi_agent_cartpole_ppo.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_discrete",
        "team:rllib",
        "torch_only",
    ],
)

py_test(
    name = "learning_tests_multi_agent_cartpole_ppo_gpu",
    size = "large",
    srcs = ["tuned_examples/ppo/multi_agent_cartpole_ppo.py"],
    args = [
        "--as-test",
        "--num-agents=2",
        "--num-learners=1",
        "--num-gpus-per-learner=1",
    ],
    main = "tuned_examples/ppo/multi_agent_cartpole_ppo.py",
    tags = [
        "exclusive",
        "gpu",
        "learning_tests",
        "learning_tests_discrete",
        "learning_tests_pytorch_use_all_core",
        "team:rllib",
        "torch_only",
    ],
)

py_test(
    name = "learning_tests_multi_agent_cartpole_ppo_multi_cpu",
    size = "large",
    srcs = ["tuned_examples/ppo/multi_agent_cartpole_ppo.py"],
    args = [
        "--as-test",
        "--num-agents=2",
        "--num-learners=2",
    ],
    main = "tuned_examples/ppo/multi_agent_cartpole_ppo.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_discrete",
        "learning_tests_pytorch_use_all_core",
        "team:rllib",
        "torch_only",
    ],
)

py_test(
    name = "learning_tests_multi_agent_cartpole_ppo_multi_gpu",
    size = "large",
    srcs = ["tuned_examples/ppo/multi_agent_cartpole_ppo.py"],
    args = [
        "--as-test",
        "--num-agents=2",
        "--num-learners=2",
        "--num-gpus-per-learner=1",
    ],
    main = "tuned_examples/ppo/multi_agent_cartpole_ppo.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_discrete",
        "learning_tests_pytorch_use_all_core",
        "multi_gpu",
        "team:rllib",
        "torch_only",
    ],
)

# CartPole (truncated)
py_test(
    name = "learning_tests_cartpole_truncated_ppo",
    size = "large",
    srcs = ["tuned_examples/ppo/cartpole_truncated_ppo.py"],
    args = [
        "--as-test",
    ],
    main = "tuned_examples/ppo/cartpole_truncated_ppo.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_discrete",
        "team:rllib",
        "torch_only",
    ],
)

# StatelessCartPole
py_test(
    name = "learning_tests_stateless_cartpole_ppo",
    size = "large",
    srcs = ["tuned_examples/ppo/stateless_cartpole_ppo.py"],
    args = [
        "--as-test",
    ],
    main = "tuned_examples/ppo/stateless_cartpole_ppo.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_discrete",
        "team:rllib",
        "torch_only",
    ],
)

py_test(
    name = "learning_tests_stateless_cartpole_ppo_gpu",
    size = "large",
    srcs = ["tuned_examples/ppo/stateless_cartpole_ppo.py"],
    args = [
        "--as-test",
        "--num-learners=1",
        "--num-gpus-per-learner=1",
    ],
    main = "tuned_examples/ppo/stateless_cartpole_ppo.py",
    tags = [
        "exclusive",
        "gpu",
        "learning_tests",
        "learning_tests_discrete",
        "learning_tests_pytorch_use_all_core",
        "team:rllib",
        "torch_only",
    ],
)

py_test(
    name = "learning_tests_stateless_cartpole_ppo_multi_cpu",
    size = "large",
    srcs = ["tuned_examples/ppo/stateless_cartpole_ppo.py"],
    args = [
        "--as-test",
        "--num-learners=2",
    ],
    main = "tuned_examples/ppo/stateless_cartpole_ppo.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_discrete",
        "learning_tests_pytorch_use_all_core",
        "team:rllib",
        "torch_only",
    ],
)

py_test(
    name = "learning_tests_stateless_cartpole_ppo_multi_gpu",
    size = "large",
    srcs = ["tuned_examples/ppo/stateless_cartpole_ppo.py"],
    args = [
        "--as-test",
        "--num-learners=2",
        "--num-gpus-per-learner=1",
    ],
    main = "tuned_examples/ppo/stateless_cartpole_ppo.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_discrete",
        "learning_tests_pytorch_use_all_core",
        "multi_gpu",
        "team:rllib",
        "torch_only",
    ],
)

# MultiAgentStatelessCartPole
py_test(
    name = "learning_tests_multi_agent_stateless_cartpole_ppo",
    size = "large",
    srcs = ["tuned_examples/ppo/multi_agent_stateless_cartpole_ppo.py"],
    args = [
        "--as-test",
        "--num-agents=2",
    ],
    main = "tuned_examples/ppo/multi_agent_stateless_cartpole_ppo.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_discrete",
        "team:rllib",
        "torch_only",
    ],
)

py_test(
    name = "learning_tests_multi_agent_stateless_cartpole_ppo_gpu",
    size = "large",
    srcs = ["tuned_examples/ppo/multi_agent_stateless_cartpole_ppo.py"],
    args = [
        "--as-test",
        "--num-agents=2",
        "--num-learners=1",
        "--num-gpus-per-learner=1",
    ],
    main = "tuned_examples/ppo/multi_agent_stateless_cartpole_ppo.py",
    tags = [
        "exclusive",
        "gpu",
        "learning_tests",
        "learning_tests_discrete",
        "learning_tests_pytorch_use_all_core",
        "team:rllib",
        "torch_only",
    ],
)

py_test(
    name = "learning_tests_multi_agent_stateless_cartpole_ppo_multi_cpu",
    size = "large",
    srcs = ["tuned_examples/ppo/multi_agent_stateless_cartpole_ppo.py"],
    args = [
        "--as-test",
        "--num-agents=2",
        "--num-learners=2",
    ],
    main = "tuned_examples/ppo/multi_agent_stateless_cartpole_ppo.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_discrete",
        "learning_tests_pytorch_use_all_core",
        "team:rllib",
        "torch_only",
    ],
)

py_test(
    name = "learning_tests_multi_agent_stateless_cartpole_ppo_multi_gpu",
    size = "large",
    srcs = ["tuned_examples/ppo/multi_agent_stateless_cartpole_ppo.py"],
    args = [
        "--as-test",
        "--num-agents=2",
        "--num-learners=2",
        "--num-gpus-per-learner=1",
    ],
    main = "tuned_examples/ppo/multi_agent_stateless_cartpole_ppo.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_discrete",
        "learning_tests_pytorch_use_all_core",
        "multi_gpu",
        "team:rllib",
        "torch_only",
    ],
)

# Footsies
py_test(
    name = "learning_tests_multi_agent_footsies_ppo",
    size = "large",
    srcs = ["tuned_examples/ppo/multi_agent_footsies_ppo.py"],
    args = [
        "--as-test",
        "--num-env-runners=6",
        "--evaluation-num-env-runners=2",
    ],
    main = "tuned_examples/ppo/multi_agent_footsies_ppo.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_discrete",
        "team:rllib",
    ],
)

py_test(
    name = "learning_tests_multi_agent_footsies_ppo_gpu",
    size = "large",
    srcs = ["tuned_examples/ppo/multi_agent_footsies_ppo.py"],
    args = [
        "--as-test",
        "--num-env-runners=20",
        "--evaluation-num-env-runners=3",
        "--num-learners=1",
        "--num-gpus-per-learner=1",
    ],
    main = "tuned_examples/ppo/multi_agent_footsies_ppo.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_discrete",
        "multi_gpu",
        "team:rllib",
    ],
)

py_test(
    name = "learning_tests_multi_agent_footsies_ppo_multi_cpu",
    size = "large",
    srcs = ["tuned_examples/ppo/multi_agent_footsies_ppo.py"],
    args = [
        "--as-test",
        "--num-env-runners=6",
        "--evaluation-num-env-runners=2",
        "--num-learners=2",
    ],
    main = "tuned_examples/ppo/multi_agent_footsies_ppo.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_discrete",
        "team:rllib",
    ],
)

py_test(
    name = "learning_tests_multi_agent_footsies_ppo_multi_gpu",
    size = "large",
    srcs = ["tuned_examples/ppo/multi_agent_footsies_ppo.py"],
    args = [
        "--as-test",
        "--num-env-runners=20",
        "--evaluation-num-env-runners=3",
        "--num-learners=2",
        "--num-gpus-per-learner=1",
    ],
    main = "tuned_examples/ppo/multi_agent_footsies_ppo.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_discrete",
        "multi_gpu",
        "team:rllib",
    ],
)

# Pendulum
py_test(
    name = "learning_tests_pendulum_ppo",
    size = "large",
    srcs = ["tuned_examples/ppo/pendulum_ppo.py"],
    args = [
        "--as-test",
    ],
    main = "tuned_examples/ppo/pendulum_ppo.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_continuous",
        "team:rllib",
        "torch_only",
    ],
)

py_test(
    name = "learning_tests_pendulum_ppo_gpu",
    size = "large",
    srcs = ["tuned_examples/ppo/pendulum_ppo.py"],
    args = [
        "--as-test",
        "--num-learners=1",
        "--num-gpus-per-learner=1",
    ],
    main = "tuned_examples/ppo/pendulum_ppo.py",
    tags = [
        "exclusive",
        "gpu",
        "learning_tests",
        "learning_tests_continuous",
        "learning_tests_pytorch_use_all_core",
        "team:rllib",
        "torch_only",
    ],
)

py_test(
    name = "learning_tests_pendulum_ppo_multi_cpu",
    size = "large",
    srcs = ["tuned_examples/ppo/pendulum_ppo.py"],
    args = [
        "--as-test",
        "--num-learners=2",
    ],
    main = "tuned_examples/ppo/pendulum_ppo.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_continuous",
        "learning_tests_pytorch_use_all_core",
        "team:rllib",
        "torch_only",
    ],
)

py_test(
    name = "learning_tests_pendulum_ppo_multi_gpu",
    size = "large",
    srcs = ["tuned_examples/ppo/pendulum_ppo.py"],
    args = [
        "--as-test",
        "--num-learners=2",
        "--num-gpus-per-learner=1",
    ],
    main = "tuned_examples/ppo/pendulum_ppo.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_continuous",
        "learning_tests_pytorch_use_all_core",
        "multi_gpu",
        "team:rllib",
        "torch_only",
    ],
)

# MultiAgentPendulum
py_test(
    name = "learning_tests_multi_agent_pendulum_ppo",
    size = "large",
    srcs = ["tuned_examples/ppo/multi_agent_pendulum_ppo.py"],
    args = [
        "--as-test",
        "--num-agents=2",
    ],
    main = "tuned_examples/ppo/multi_agent_pendulum_ppo.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_continuous",
        "team:rllib",
        "torch_only",
    ],
)

py_test(
    name = "learning_tests_multi_agent_pendulum_ppo_gpu",
    size = "large",
    srcs = ["tuned_examples/ppo/multi_agent_pendulum_ppo.py"],
    args = [
        "--as-test",
        "--num-agents=2",
        "--num-learners=1",
        "--num-gpus-per-learner=1",
    ],
    main = "tuned_examples/ppo/multi_agent_pendulum_ppo.py",
    tags = [
        "exclusive",
        "gpu",
        "learning_tests",
        "learning_tests_continuous",
        "learning_tests_pytorch_use_all_core",
        "team:rllib",
        "torch_only",
    ],
)

py_test(
    name = "learning_tests_multi_agent_pendulum_ppo_multi_cpu",
    size = "large",
    srcs = ["tuned_examples/ppo/multi_agent_pendulum_ppo.py"],
    args = [
        "--as-test",
        "--num-agents=2",
        "--num-learners=2",
    ],
    main = "tuned_examples/ppo/multi_agent_pendulum_ppo.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_continuous",
        "learning_tests_pytorch_use_all_core",
        "team:rllib",
        "torch_only",
    ],
)

py_test(
    name = "learning_tests_multi_agent_pendulum_ppo_multi_gpu",
    size = "large",
    srcs = ["tuned_examples/ppo/multi_agent_pendulum_ppo.py"],
    args = [
        "--as-test",
        "--num-agents=2",
        "--num-learners=2",
        "--num-gpus-per-learner=1",
    ],
    main = "tuned_examples/ppo/multi_agent_pendulum_ppo.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_continuous",
        "learning_tests_pytorch_use_all_core",
        "multi_gpu",
        "team:rllib",
        "torch_only",
    ],
)

# SAC
# MountainCar
py_test(
    name = "learning_tests_mountaincar_sac",
    size = "large",
    srcs = ["tuned_examples/sac/mountaincar_sac.py"],
    args = [
        "--as-test",
    ],
    main = "tuned_examples/sac/mountaincar_sac.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_discrete",
        "team:rllib",
        "torch_only",
    ],
)

py_test(
    name = "learning_tests_mountaincar_sac_gpu",
    size = "large",
    srcs = ["tuned_examples/sac/mountaincar_sac.py"],
    args = [
        "--as-test",
        "--num-learners=1",
        "--num-gpus-per-learner=1",
    ],
    main = "tuned_examples/sac/mountaincar_sac.py",
    tags = [
        "exclusive",
        "gpu",
        "learning_tests",
        "learning_tests_discrete",
        "team:rllib",
        "torch_only",
    ],
)

py_test(
    name = "learning_tests_mountaincar_sac_multi_cpu",
    size = "large",
    srcs = ["tuned_examples/sac/mountaincar_sac.py"],
    args = [
        "--as-test",
        "--num-learners=2",
    ],
    main = "tuned_examples/sac/mountaincar_sac.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_discrete",
        "team:rllib",
        "torch_only",
    ],
)

py_test(
    name = "learning_tests_mountaincar_sac_multi_gpu",
    size = "large",
    srcs = ["tuned_examples/sac/mountaincar_sac.py"],
    args = [
        "--as-test",
        "--num-learners=2",
        "--num-gpus-per-learner=1",
    ],
    main = "tuned_examples/sac/mountaincar_sac.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_discrete",
        "multi_gpu",
        "team:rllib",
        "torch_only",
    ],
)

# Pendulum
py_test(
    name = "learning_tests_pendulum_sac",
    size = "large",
    srcs = ["tuned_examples/sac/pendulum_sac.py"],
    args = [
        "--as-test",
    ],
    main = "tuned_examples/sac/pendulum_sac.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_continuous",
        "team:rllib",
        "torch_only",
    ],
)

py_test(
    name = "learning_tests_pendulum_sac_gpu",
    size = "large",
    srcs = ["tuned_examples/sac/pendulum_sac.py"],
    args = [
        "--as-test",
        "--num-learners=1",
        "--num-gpus-per-learner=1",
    ],
    main = "tuned_examples/sac/pendulum_sac.py",
    tags = [
        "exclusive",
        "gpu",
        "learning_tests",
        "learning_tests_continuous",
        "team:rllib",
        "torch_only",
    ],
)

py_test(
    name = "learning_tests_pendulum_sac_multi_cpu",
    size = "large",
    srcs = ["tuned_examples/sac/pendulum_sac.py"],
    args = [
        "--as-test",
        "--num-learners=2",
    ],
    main = "tuned_examples/sac/pendulum_sac.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_continuous",
        "team:rllib",
        "torch_only",
    ],
)

py_test(
    name = "learning_tests_pendulum_sac_multi_gpu",
    size = "large",
    srcs = ["tuned_examples/sac/pendulum_sac.py"],
    args = [
        "--as-test",
        "--num-learners=2",
        "--num-gpus-per-learner=1",
    ],
    main = "tuned_examples/sac/pendulum_sac.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_continuous",
        "multi_gpu",
        "team:rllib",
        "torch_only",
    ],
)

# MultiAgentPendulum
py_test(
    name = "learning_tests_multi_agent_pendulum_sac",
    size = "large",
    srcs = ["tuned_examples/sac/multi_agent_pendulum_sac.py"],
    args = [
        "--as-test",
        "--num-agents=2",
        "--num-cpus=4",
    ],
    main = "tuned_examples/sac/multi_agent_pendulum_sac.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_continuous",
        "team:rllib",
        "torch_only",
    ],
)

py_test(
    name = "learning_tests_multi_agent_pendulum_sac_gpu",
    size = "large",
    srcs = ["tuned_examples/sac/multi_agent_pendulum_sac.py"],
    args = [
        "--as-test",
        "--num-agents=2",
        "--num-cpus=4",
        "--num-learners=1",
        "--num-gpus-per-learner=1",
    ],
    main = "tuned_examples/sac/multi_agent_pendulum_sac.py",
    tags = [
        "exclusive",
        "gpu",
        "learning_tests",
        "learning_tests_continuous",
        "team:rllib",
        "torch_only",
    ],
)

py_test(
    name = "learning_tests_multi_agent_pendulum_sac_multi_cpu",
    size = "large",
    srcs = ["tuned_examples/sac/multi_agent_pendulum_sac.py"],
    args = [
        "--num-agents=2",
        "--num-learners=2",
    ],
    main = "tuned_examples/sac/multi_agent_pendulum_sac.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_continuous",
        "team:rllib",
        "torch_only",
    ],
)

py_test(
    name = "learning_tests_multi_agent_pendulum_sac_multi_gpu",
    size = "large",
    srcs = ["tuned_examples/sac/multi_agent_pendulum_sac.py"],
    args = [
        "--num-agents=2",
        "--num-learners=2",
        "--num-gpus-per-learner=1",
    ],
    main = "tuned_examples/sac/multi_agent_pendulum_sac.py",
    tags = [
        "exclusive",
        "learning_tests",
        "learning_tests_continuous",
        "multi_gpu",
        "team:rllib",
        "torch_only",
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
    name = "test_algorithm_save_load_checkpoint_learner",
    size = "medium",
    srcs = ["algorithms/tests/test_algorithm_save_load_checkpoint_learner.py"],
    tags = [
        "algorithms_dir",
        "algorithms_dir_generic",
        "team:rllib",
    ],
)

py_test(
    name = "test_algorithm_save_load_checkpoint_connectors",
    size = "medium",
    srcs = ["algorithms/tests/test_algorithm_save_load_checkpoint_connectors.py"],
    tags = [
        "algorithms_dir",
        "algorithms_dir_generic",
        "team:rllib",
    ],
)

py_test(
    name = "test_algorithm_rl_module_restore",
    size = "large",
    srcs = ["algorithms/tests/test_algorithm_rl_module_restore.py"],
    tags = [
        "algorithms_dir",
        "algorithms_dir_generic",
        "team:rllib",
    ],
)

py_test(
    name = "test_algorithm_imports",
    size = "small",
    srcs = ["algorithms/tests/test_algorithm_imports.py"],
    tags = [
        "algorithms_dir",
        "algorithms_dir_generic",
        "team:rllib",
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
    name = "test_env_runner_failures",
    size = "large",
    srcs = ["algorithms/tests/test_env_runner_failures.py"],
    tags = [
        "algorithms_dir",
        "algorithms_dir_generic",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "test_node_failures",
    size = "large",
    srcs = ["algorithms/tests/test_node_failures.py"],
    tags = [
        "exclusive",
        "team:rllib",
        "tests_dir",
    ],
)

# Specific Algorithms

# APPO
# @OldAPIStack
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
    # Include the offline data files.
    data = ["tests/data/cartpole/cartpole-v1_large"],
    tags = [
        "algorithms_dir",
        "team:rllib",
    ],
)

# CQL
# @OldAPIStack
py_test(
    name = "test_cql_old_api_stack",
    size = "large",
    srcs = ["algorithms/cql/tests/test_cql_old_api_stack.py"],
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

# IMPALA
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
    name = "test_vtrace_v2",
    size = "small",
    srcs = ["algorithms/impala/tests/test_vtrace_v2.py"],
    tags = [
        "algorithms_dir",
        "team:rllib",
    ],
)

# @OldAPIStack
py_test(
    name = "test_vtrace_old_api_stack",
    size = "small",
    srcs = ["algorithms/impala/tests/test_vtrace_old_api_stack.py"],
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
    # Include the offline data files.
    data = [
        "tests/data/cartpole/cartpole-v1_large",
        "tests/data/pendulum/pendulum-v1_large",
    ],
    tags = [
        "algorithms_dir",
        "team:rllib",
    ],
)

py_test(
    name = "test_marwil_rl_module",
    size = "large",
    srcs = ["algorithms/marwil/tests/test_marwil_rl_module.py"],
    # Include the json data file.
    data = [
        "tests/data/cartpole/large.json",
    ],
    tags = [
        "algorithms_dir",
        "team:rllib",
    ],
)

# PPO
py_test(
    name = "test_ppo",
    size = "medium",
    srcs = ["algorithms/ppo/tests/test_ppo.py"],
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

# --------------------------------------------------------------------
# Callback tests
# rllib/callbacks/
#
# Tag: callbacks
# --------------------------------------------------------------------
py_test(
    name = "test_callbacks_on_algorithm",
    size = "large",
    srcs = ["callbacks/tests/test_callbacks_on_algorithm.py"],
    tags = [
        "callbacks_dir",
        "team:rllib",
    ],
)

py_test(
    name = "test_callbacks_on_env_runner",
    size = "medium",
    srcs = ["callbacks/tests/test_callbacks_on_env_runner.py"],
    tags = [
        "callbacks_dir",
        "team:rllib",
    ],
)

# @OldAPIStack
py_test(
    name = "test_callbacks_old_api_stack",
    size = "medium",
    srcs = ["callbacks/tests/test_callbacks_old_api_stack.py"],
    tags = [
        "callbacks_dir",
        "team:rllib",
    ],
)

py_test(
    name = "test_multicallback",
    size = "medium",
    srcs = ["callbacks/tests/test_multicallback.py"],
    tags = [
        "callbacks_dir",
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
    name = "env/wrappers/tests/test_group_agents_wrapper",
    size = "small",
    srcs = ["env/wrappers/tests/test_group_agents_wrapper.py"],
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
    name = "env/tests/test_env_runner_group",
    size = "small",
    srcs = ["env/tests/test_env_runner_group.py"],
    tags = [
        "evaluation",
        "exclusive",
        "team:rllib",
    ],
)

# @OldAPIStack
py_test(
    name = "evaluation/tests/test_agent_collector",
    size = "small",
    srcs = ["evaluation/tests/test_agent_collector.py"],
    tags = [
        "evaluation",
        "team:rllib",
    ],
)

# @OldAPIStack
py_test(
    name = "evaluation/tests/test_env_runner_v2",
    size = "small",
    srcs = ["evaluation/tests/test_env_runner_v2.py"],
    tags = [
        "evaluation",
        "team:rllib",
    ],
)

# @OldAPIStack
py_test(
    name = "evaluation/tests/test_episode_v2",
    size = "small",
    srcs = ["evaluation/tests/test_episode_v2.py"],
    tags = [
        "evaluation",
        "team:rllib",
    ],
)

# @OldAPIStack
py_test(
    name = "evaluation/tests/test_postprocessing",
    size = "small",
    srcs = ["evaluation/tests/test_postprocessing.py"],
    tags = [
        "evaluation",
        "team:rllib",
    ],
)

# @OldAPIStack
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
    name = "test_multi_rl_module",
    size = "medium",
    srcs = ["core/rl_module/tests/test_multi_rl_module.py"],
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

# LearnerGroup
py_test(
    name = "test_learner_group_async_update",
    size = "large",
    srcs = ["core/learner/tests/test_learner_group.py"],
    args = ["TestLearnerGroupAsyncUpdate"],
    main = "core/learner/tests/test_learner_group.py",
    # TODO(#50114): mark as manual as it is flaky.
    tags = [
        "exclusive",
        "manual",
        "multi_gpu",
        "team:rllib",
    ],
)

py_test(
    name = "test_learner_group_sync_update",
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
    name = "test_learner_group_checkpoint_restore",
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
    name = "test_learner_group_save_and_restore_state",
    size = "large",
    srcs = ["core/learner/tests/test_learner_group.py"],
    args = ["TestLearnerGroupSaveAndRestoreState"],
    main = "core/learner/tests/test_learner_group.py",
    tags = [
        "exclusive",
        "multi_gpu",
        "team:rllib",
    ],
)

# Learner
py_test(
    name = "test_learner",
    size = "medium",
    srcs = ["core/learner/tests/test_learner.py"],
    tags = [
        "core",
        "exclusive",
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
        "exclusive",
        "ray_data",
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

py_test(
    name = "test_offline_env_runner",
    size = "small",
    srcs = ["offline/tests/test_offline_env_runner.py"],
    tags = [
        "offline",
        "team:rllib",
    ],
)

py_test(
    name = "test_offline_data",
    size = "medium",
    srcs = ["offline/tests/test_offline_data.py"],
    # Include the offline data files.
    data = [
        "tests/data/cartpole/cartpole-v1_large",
        "tests/data/cartpole/large.json",
    ],
    tags = [
        "offline",
        "team:rllib",
    ],
)

py_test(
    name = "test_offline_evaluation_runner",
    size = "medium",
    srcs = ["offline/tests/test_offline_evaluation_runner.py"],
    # Include the offline data files.
    data = [
        "tests/data/cartpole/cartpole-v1_large",
    ],
    tags = [
        "offline",
        "team:rllib",
    ],
)

py_test(
    name = "test_offline_evaluation_runner_group",
    size = "medium",
    srcs = ["offline/tests/test_offline_evaluation_runner_group.py"],
    # Include the offline data files.
    data = [
        "tests/data/cartpole/cartpole-v1_large",
    ],
    tags = [
        "offline",
        "team:rllib",
    ],
)

# TODO (sven, simon): This runs fine locally, but fails in the CI
# py_test(
#    # TODO(#50340): test is flaky.
#    name = "test_offline_prelearner",
#    tags = ["team:rllib", "offline"],
#    size = "medium",
#    srcs = ["offline/tests/test_offline_prelearner.py"],
#    # Include the offline data files.
#    data = [
#        "tests/data/cartpole/cartpole-v1_large",
#        "tests/data/cartpole/large.json",
#    ]
# )

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

# Checkpointables
py_test(
    name = "utils/tests/test_checkpointable",
    size = "large",
    srcs = ["utils/tests/test_checkpointable.py"],
    data = glob(["utils/tests/old_checkpoints/**"]),
    tags = [
        "team:rllib",
        "utils",
    ],
)

# Errors
py_test(
    name = "test_errors",
    size = "medium",
    srcs = ["utils/tests/test_errors.py"],
    tags = [
        "team:rllib",
        "utils",
    ],
)

# @OldAPIStack
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
    name = "test_serialization",
    size = "small",
    srcs = ["utils/tests/test_serialization.py"],
    tags = [
        "team:rllib",
        "utils",
    ],
)

# @OldAPIStack
py_test(
    name = "test_explorations",
    size = "large",
    srcs = ["utils/exploration/tests/test_explorations.py"],
    tags = [
        "team:rllib",
        "utils",
    ],
)

# Test metrics (metrics logger, stats)
py_test(
    name = "test_metrics_logger",
    size = "small",
    srcs = ["utils/metrics/tests/test_metrics_logger.py"],
    tags = [
        "team:rllib",
        "utils",
    ],
)

py_test(
    name = "test_stats",
    size = "small",
    srcs = ["utils/metrics/tests/test_stats.py"],
    tags = [
        "team:rllib",
        "utils",
    ],
)

# @OldAPIStack
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
    name = "test_tf_utils",
    size = "medium",
    srcs = ["utils/tests/test_tf_utils.py"],
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

# @OldAPIStack
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
    name = "test_multi_agent_episode_buffer",
    size = "small",
    srcs = ["utils/replay_buffers/tests/test_multi_agent_episode_buffer.py"],
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
    name = "test_multi_agent_prio_episode_buffer",
    size = "small",
    srcs = ["utils/replay_buffers/tests/test_multi_agent_prio_episode_buffer.py"],
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
    name = "test_prioritized_episode_buffer",
    size = "small",
    srcs = ["utils/replay_buffers/tests/test_prioritized_episode_buffer.py"],
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
    name = "tests/test_catalog",
    size = "medium",
    srcs = ["tests/test_catalog.py"],
    tags = [
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
    name = "tests/test_timesteps",
    size = "small",
    srcs = ["tests/test_timesteps.py"],
    tags = [
        "team:rllib",
        "tests_dir",
    ],
)

py_test(
    name = "tests/test_ray_client",
    size = "medium",
    srcs = ["tests/test_ray_client.py"],
    tags = [
        "team:rllib",
        "tests_dir",
    ],
)

py_test(
    name = "tests/test_telemetry",
    size = "small",
    srcs = ["tests/test_telemetry.py"],
    tags = [
        "team:rllib",
        "tests_dir",
    ],
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
    size = "medium",
    srcs = ["examples/_docs/rllib_on_rllib_readme.py"],
    main = "examples/_docs/rllib_on_rllib_readme.py",
    tags = [
        "documentation",
        "no_main",
        "team:rllib",
    ],
)

# ----------------------
# Old API stack examples
# ----------------------
# subdirectory: _old_api_stack/connectors/
py_test(
    name = "examples/_old_api_stack/connectors/run_connector_policy",
    size = "small",
    srcs = ["examples/_old_api_stack/connectors/run_connector_policy.py"],
    main = "examples/_old_api_stack/connectors/run_connector_policy.py",
    tags = [
        "examples",
        "exclusive",
        "old_api_stack",
        "team:rllib",
    ],
)

py_test(
    name = "examples/_old_api_stack/connectors/run_connector_policy_w_lstm",
    size = "small",
    srcs = ["examples/_old_api_stack/connectors/run_connector_policy.py"],
    args = ["--use-lstm"],
    main = "examples/_old_api_stack/connectors/run_connector_policy.py",
    tags = [
        "examples",
        "exclusive",
        "old_api_stack",
        "team:rllib",
    ],
)

# ----------------------
# New API stack
# Note: This includes to-be-translated-to-new-API-stack examples
# tagged by @OldAPIStack
# ----------------------

# subdirectory: actions/
# ....................................
py_test(
    name = "examples/actions/autoregressive_actions",
    size = "large",
    srcs = ["examples/actions/autoregressive_actions.py"],
    main = "examples/actions/autoregressive_actions.py",
    tags = [
        "examples",
        "team:rllib",
    ],
)

py_test(
    name = "examples/actions/custom_action_distribution",
    size = "large",
    srcs = ["examples/actions/custom_action_distribution.py"],
    args = [
        "--temperature=0.75",
    ],
    main = "examples/actions/custom_action_distribution.py",
    tags = [
        "examples",
        "team:rllib",
    ],
)

py_test(
    name = "examples/actions/nested_action_spaces_ppo",
    size = "large",
    srcs = ["examples/actions/nested_action_spaces.py"],
    args = [
        "--as-test",
        "--framework=torch",
        "--stop-reward=-500.0",
        "--algo=PPO",
    ],
    main = "examples/actions/nested_action_spaces.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/actions/nested_action_spaces_multi_agent_ppo",
    size = "large",
    srcs = ["examples/actions/nested_action_spaces.py"],
    args = [
        "--as-test",
        "--num-agents=2",
        "--framework=torch",
        "--stop-reward=-1000.0",
        "--algo=PPO",
    ],
    main = "examples/actions/nested_action_spaces.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

# subdirectory: algorithms/
# ....................................
py_test(
    name = "examples/algorithms/appo_custom_algorithm_w_shared_data_actor",
    size = "large",
    srcs = ["examples/algorithms/appo_custom_algorithm_w_shared_data_actor.py"],
    args = [
        "--as-test",
    ],
    main = "examples/algorithms/appo_custom_algorithm_w_shared_data_actor.py",
    tags = [
        "examples",
        "team:rllib",
    ],
)

py_test(
    name = "examples/algorithms/maml_lr_supervised_learning",
    size = "large",
    srcs = ["examples/algorithms/maml_lr_supervised_learning.py"],
    args = [
        "--as-test",
        "--stop-iters=70000",
        "--meta-lr=0.001",
        "--meta-train-batch-size=5",
        "--fine-tune-iters=10",
        "--fine-tune-batch-size=5",
        "--fine-tune-lr=0.01",
        "--noise-std=0.0",
        "--no-plot",
    ],
    main = "examples/algorithms/maml_lr_supervised_learning.py",
    tags = [
        "examples",
        "team:rllib",
    ],
)

py_test(
    name = "examples/algorithms/vpg_custom_algorithm",
    size = "large",
    srcs = ["examples/algorithms/vpg_custom_algorithm.py"],
    args = [
        "--as-test",
    ],
    main = "examples/algorithms/vpg_custom_algorithm.py",
    tags = [
        "examples",
        "team:rllib",
    ],
)

# subdirectory: catalogs/
# ....................................
py_test(
    name = "examples/catalogs/custom_action_distribution",
    size = "small",
    srcs = ["examples/catalogs/custom_action_distribution.py"],
    main = "examples/catalogs/custom_action_distribution.py",
    tags = [
        "examples",
        "no_main",
        "team:rllib",
    ],
)

py_test(
    name = "examples/catalogs/mobilenet_v2_encoder",
    size = "small",
    srcs = ["examples/catalogs/mobilenet_v2_encoder.py"],
    main = "examples/catalogs/mobilenet_v2_encoder.py",
    tags = [
        "examples",
        "no_main",
        "team:rllib",
    ],
)

# subdirectory: checkpoints/
# ....................................
py_test(
    name = "examples/checkpoints/change_config_during_training",
    size = "large",
    srcs = ["examples/checkpoints/change_config_during_training.py"],
    args = [
        "--as-test",
        "--stop-reward-first-config=150.0",
        "--stop-reward=450.0",
    ],
    main = "examples/checkpoints/change_config_during_training.py",
    tags = [
        "examples",
        "examples_use_all_core",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/checkpoints/checkpoint_by_custom_criteria",
    size = "large",
    srcs = ["examples/checkpoints/checkpoint_by_custom_criteria.py"],
    args = [
        "--stop-reward=150.0",
        "--num-cpus=8",
    ],
    main = "examples/checkpoints/checkpoint_by_custom_criteria.py",
    tags = [
        "examples",
        "examples_use_all_core",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/checkpoints/continue_training_from_checkpoint",
    size = "large",
    srcs = ["examples/checkpoints/continue_training_from_checkpoint.py"],
    args = [
        "--as-test",
    ],
    main = "examples/checkpoints/continue_training_from_checkpoint.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/checkpoints/continue_training_from_checkpoint_multi_agent",
    size = "large",
    srcs = ["examples/checkpoints/continue_training_from_checkpoint.py"],
    args = [
        "--as-test",
        "--num-agents=2",
        "--stop-reward-crash=400.0",
        "--stop-reward=900.0",
    ],
    main = "examples/checkpoints/continue_training_from_checkpoint.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

#@OldAPIStack
py_test(
    name = "examples/checkpoints/continue_training_from_checkpoint_old_api_stack",
    size = "large",
    srcs = ["examples/checkpoints/continue_training_from_checkpoint.py"],
    args = ["--as-test"],
    main = "examples/checkpoints/continue_training_from_checkpoint.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/checkpoints/cartpole_dqn_export",
    size = "small",
    srcs = ["examples/checkpoints/cartpole_dqn_export.py"],
    main = "examples/checkpoints/cartpole_dqn_export.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

# subdirectory: connectors/
# ....................................
# Framestacking examples only run in smoke-test mode (a few iters only).
# PPO
py_test(
    name = "examples/connectors/frame_stacking_ppo",
    size = "medium",
    srcs = ["examples/connectors/frame_stacking.py"],
    args = [
        "--stop-iter=2",
        "--framework=torch",
        "--algo=PPO",
    ],
    main = "examples/connectors/frame_stacking.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/connectors/frame_stacking_multi_agent_ppo",
    size = "medium",
    srcs = ["examples/connectors/frame_stacking.py"],
    args = [
        "--num-agents=2",
        "--stop-iter=2",
        "--framework=torch",
        "--algo=PPO",
        "--num-env-runners=4",
        "--num-cpus=6",
    ],
    main = "examples/connectors/frame_stacking.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

# IMPALA
py_test(
    name = "examples/connectors/frame_stacking_impala",
    size = "medium",
    srcs = ["examples/connectors/frame_stacking.py"],
    args = [
        "--stop-iter=2",
        "--framework=torch",
        "--algo=IMPALA",
    ],
    main = "examples/connectors/frame_stacking.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/connectors/frame_stacking_multi_agent_impala",
    size = "medium",
    srcs = ["examples/connectors/frame_stacking.py"],
    args = [
        "--num-agents=2",
        "--stop-iter=2",
        "--framework=torch",
        "--algo=IMPALA",
        "--num-env-runners=4",
        "--num-cpus=6",
    ],
    main = "examples/connectors/frame_stacking.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

# Nested observation spaces (flattening).
# PPO
py_test(
    name = "examples/connectors/flatten_observations_dict_space_ppo",
    size = "medium",
    srcs = ["examples/connectors/flatten_observations_dict_space.py"],
    args = [
        "--as-test",
        "--stop-reward=400.0",
        "--framework=torch",
        "--algo=PPO",
    ],
    main = "examples/connectors/flatten_observations_dict_space.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/connectors/flatten_observations_dict_space_multi_agent_ppo",
    size = "medium",
    srcs = ["examples/connectors/flatten_observations_dict_space.py"],
    args = [
        "--num-agents=2",
        "--as-test",
        "--stop-reward=800.0",
        "--framework=torch",
        "--algo=PPO",
    ],
    main = "examples/connectors/flatten_observations_dict_space.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

# IMPALA
py_test(
    name = "examples/connectors/flatten_observations_dict_space_impala",
    size = "large",
    srcs = ["examples/connectors/flatten_observations_dict_space.py"],
    args = [
        "--as-test",
        "--stop-reward=400.0",
        "--stop-timesteps=2000000",
        "--framework=torch",
        "--algo=IMPALA",
    ],
    main = "examples/connectors/flatten_observations_dict_space.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/connectors/flatten_observations_dict_space_multi_agent_impala",
    size = "large",
    srcs = ["examples/connectors/flatten_observations_dict_space.py"],
    args = [
        "--num-agents=2",
        "--as-test",
        "--stop-reward=800.0",
        "--stop-timesteps=2000000",
        "--framework=torch",
        "--algo=IMPALA",
    ],
    main = "examples/connectors/flatten_observations_dict_space.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
        # Test is failing: https://github.com/ray-project/ray/issues/47717
        "manual",
    ],
)

# Prev-r/prev actions + LSTM example.
py_test(
    name = "examples/connectors/prev_actions_prev_rewards_ppo",
    size = "large",
    srcs = ["examples/connectors/prev_actions_prev_rewards.py"],
    args = [
        "--as-test",
        "--stop-reward=200.0",
        "--framework=torch",
        "--algo=PPO",
        "--num-env-runners=4",
        "--num-cpus=6",
    ],
    main = "examples/connectors/prev_actions_prev_rewards.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/connectors/prev_actions_prev_rewards_multi_agent_ppo",
    size = "large",
    srcs = ["examples/connectors/prev_actions_prev_rewards.py"],
    args = [
        "--num-agents=2",
        "--as-test",
        "--stop-reward=400.0",
        "--framework=torch",
        "--algo=PPO",
        "--num-env-runners=4",
        "--num-cpus=6",
    ],
    main = "examples/connectors/prev_actions_prev_rewards.py",
    tags = [
        "examples",
        "examples_use_all_core",
        "exclusive",
        "team:rllib",
    ],
)

# MeanStd filtering example.
# PPO
py_test(
    name = "examples/connectors/mean_std_filtering_ppo",
    size = "medium",
    srcs = ["examples/connectors/mean_std_filtering.py"],
    args = [
        "--as-test",
        "--stop-reward=-300.0",
        "--framework=torch",
        "--algo=PPO",
        "--num-env-runners=2",
        "--num-cpus=4",
    ],
    main = "examples/connectors/mean_std_filtering.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
        # Disabled: https://github.com/ray-project/ray/issues/47435
        "manual",
    ],
)

py_test(
    name = "examples/connectors/mean_std_filtering_multi_agent_ppo",
    size = "large",
    srcs = ["examples/connectors/mean_std_filtering.py"],
    args = [
        "--num-agents=2",
        "--as-test",
        "--stop-reward=-600.0",
        "--framework=torch",
        "--algo=PPO",
        "--num-env-runners=5",
        "--num-cpus=7",
    ],
    main = "examples/connectors/mean_std_filtering.py",
    tags = [
        "examples",
        "examples_use_all_core",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/connectors/multi_agent_observation_preprocessor",
    size = "medium",
    srcs = ["examples/connectors/multi_agent_observation_preprocessor.py"],
    args = [
        "--num-agents=2",
        "--algo=PPO",
    ],
    main = "examples/connectors/multi_agent_observation_preprocessor.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/connectors/single_agent_observation_preprocessor",
    size = "medium",
    srcs = ["examples/connectors/single_agent_observation_preprocessor.py"],
    args = [
        "--algo=PPO",
    ],
    main = "examples/connectors/single_agent_observation_preprocessor.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

# subdirectory: curiosity/
# ....................................
py_test(
    name = "examples/curiosity/count_based_curiosity",
    size = "large",
    srcs = ["examples/curiosity/count_based_curiosity.py"],
    args = [
        "--as-test",
    ],
    main = "examples/curiosity/count_based_curiosity.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/curiosity/euclidian_distance_based_curiosity",
    size = "large",
    srcs = ["examples/curiosity/euclidian_distance_based_curiosity.py"],
    args = [
        "--as-test",
    ],
    main = "examples/curiosity/euclidian_distance_based_curiosity.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/curiosity/intrinsic_curiosity_model_based_curiosity_ppo",
    size = "large",
    srcs = ["examples/curiosity/intrinsic_curiosity_model_based_curiosity.py"],
    args = [
        "--as-test",
        "--algo=PPO",
    ],
    main = "examples/curiosity/intrinsic_curiosity_model_based_curiosity.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

# TODO (sven): Learns, but very slowly. Needs further tuning.
#  ICM seems to be broken due to a bug that's fixed in a still-open PR.
# py_test(
#    name = "examples/curiosity/intrinsic_curiosity_model_based_curiosity_dqn",
#    main = "examples/curiosity/intrinsic_curiosity_model_based_curiosity.py",
#    tags = ["team:rllib", "exclusive", "examples"],
#    size = "large",
#    srcs = ["examples/curiosity/intrinsic_curiosity_model_based_curiosity.py"],
#    args = ["--as-test", "--algo=DQN"]
# )

# subdirectory: curriculum/
# ....................................
py_test(
    name = "examples/curriculum/curriculum_learning",
    size = "medium",
    srcs = ["examples/curriculum/curriculum_learning.py"],
    args = [
        "--as-test",
    ],
    main = "examples/curriculum/curriculum_learning.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/curriculum/pong_curriculum_learning",
    size = "large",
    srcs = ["examples/curriculum/pong_curriculum_learning.py"],
    args = [
        "--as-test",
        "--num-env-runners=10",
        "--num-cpus=11",
        "--num-envs-per-env-runner=5",
        "--stop-iters=20",
        "--stop-reward=-21.0",
    ],
    main = "examples/curriculum/pong_curriculum_learning.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

# subdirectory: debugging/
# ....................................
py_test(
    name = "examples/debugging/deterministic_sampling_and_training",
    size = "medium",
    srcs = ["examples/debugging/deterministic_sampling_and_training.py"],
    args = [
        "--as-test",
        "--num-learners=2",
    ],
    main = "examples/debugging/deterministic_sampling_and_training.py",
    tags = [
        "examples",
        "exclusive",
        "multi_gpu",
        "team:rllib",
    ],
)

py_test(
    name = "examples/debugging/deterministic_sampling_and_training_multi_agent",
    size = "medium",
    srcs = ["examples/debugging/deterministic_sampling_and_training.py"],
    args = [
        "--as-test",
        "--num-learners=2",
        "--num-agents=2",
    ],
    main = "examples/debugging/deterministic_sampling_and_training.py",
    tags = [
        "examples",
        "exclusive",
        "multi_gpu",
        "team:rllib",
    ],
)

# subdirectory: envs/
# ....................................
py_test(
    name = "examples/envs/agents_act_in_sequence",
    size = "medium",
    srcs = ["examples/envs/agents_act_in_sequence.py"],
    args = [
        "--num-agents=2",
        "--stop-iters=3",
    ],
    main = "examples/envs/agents_act_in_sequence.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/envs/agents_act_simultaneously",
    size = "medium",
    srcs = ["examples/envs/agents_act_simultaneously.py"],
    args = [
        "--num-agents=2",
        "--stop-iters=3",
    ],
    main = "examples/envs/agents_act_simultaneously.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/envs/async_gym_env_vectorization",
    size = "medium",
    srcs = ["examples/envs/async_gym_env_vectorization.py"],
    args = [
        "--as-test",
        "--vectorize-mode=BOTH",
    ],
    main = "examples/envs/async_gym_env_vectorization.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/envs/custom_env_render_method",
    size = "medium",
    srcs = ["examples/envs/custom_env_render_method.py"],
    args = [
        "--num-agents=0",
    ],
    main = "examples/envs/custom_env_render_method.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/envs/custom_env_render_method_multi_agent",
    size = "medium",
    srcs = ["examples/envs/custom_env_render_method.py"],
    args = [
        "--num-agents=2",
    ],
    main = "examples/envs/custom_env_render_method.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/envs/custom_gym_env",
    size = "medium",
    srcs = ["examples/envs/custom_gym_env.py"],
    args = [
        "--as-test",
    ],
    main = "examples/envs/custom_gym_env.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/envs/env_connecting_to_rllib_w_tcp_client",
    size = "medium",
    srcs = ["examples/envs/env_connecting_to_rllib_w_tcp_client.py"],
    args = [
        "--as-test",
        "--port=12346",
        "--use-dummy-client",
    ],
    main = "examples/envs/env_connecting_to_rllib_w_tcp_client.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/envs/env_rendering_and_recording",
    size = "medium",
    srcs = ["examples/envs/env_rendering_and_recording.py"],
    args = [
        "--env=CartPole-v1",
        "--stop-iters=2",
    ],
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/envs/env_w_protobuf_observations",
    size = "medium",
    srcs = ["examples/envs/env_w_protobuf_observations.py"],
    args = [
        "--as-test",
    ],
    main = "examples/envs/env_w_protobuf_observations.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

#@OldAPIStack
py_test(
    name = "examples/envs/greyscale_env",
    size = "medium",
    srcs = ["examples/envs/greyscale_env.py"],
    args = ["--stop-iters=1 --as-test --framework torch"],
    tags = [
        "examples",
        "no_main",
        "team:rllib",
    ],
)

# subdirectory: evaluation/
# ....................................
py_test(
    name = "examples/evaluation/custom_evaluation",
    size = "medium",
    srcs = ["examples/evaluation/custom_evaluation.py"],
    args = [
        "--framework=torch",
        "--as-test",
        "--stop-reward=0.75",
        "--num-cpus=5",
    ],
    main = "examples/evaluation/custom_evaluation.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/evaluation/custom_evaluation_parallel_to_training_10_episodes",
    size = "medium",
    srcs = ["examples/evaluation/custom_evaluation.py"],
    args = [
        "--as-test",
        "--stop-reward=0.75",
        "--evaluation-parallel-to-training",
        "--num-cpus=5",
        "--evaluation-duration=10",
        "--evaluation-duration-unit=episodes",
    ],
    main = "examples/evaluation/custom_evaluation.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/evaluation/evaluation_parallel_to_training_duration_auto",
    size = "medium",
    srcs = ["examples/evaluation/evaluation_parallel_to_training.py"],
    args = [
        "--as-test",
        "--evaluation-parallel-to-training",
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
    name = "examples/evaluation/evaluation_parallel_to_training_multi_agent_duration_auto",
    size = "large",
    srcs = ["examples/evaluation/evaluation_parallel_to_training.py"],
    args = [
        "--num-agents=2",
        "--as-test",
        "--evaluation-parallel-to-training",
        "--stop-reward=400.0",
        "--num-cpus=6",
        "--evaluation-duration=auto",
        "--evaluation-duration-unit=episodes",
    ],
    main = "examples/evaluation/evaluation_parallel_to_training.py",
    tags = [
        "examples",
        "examples_use_all_core",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/evaluation/evaluation_parallel_to_training_1011ts",
    size = "medium",
    srcs = ["examples/evaluation/evaluation_parallel_to_training.py"],
    args = [
        "--as-test",
        "--evaluation-parallel-to-training",
        "--stop-reward=450.0",
        "--num-cpus=6",
        "--evaluation-num-env-runners=2",
        "--evaluation-duration=1011",
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
    name = "examples/evaluation/evaluation_parallel_to_training_multi_agent_2022ts",
    size = "medium",
    srcs = ["examples/evaluation/evaluation_parallel_to_training.py"],
    args = [
        "--num-agents=2",
        "--as-test",
        "--evaluation-parallel-to-training",
        "--stop-reward=900.0",
        "--num-cpus=6",
        "--evaluation-duration=2022",
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
    name = "examples/evaluation/evaluation_parallel_to_training_13_episodes",
    size = "medium",
    srcs = ["examples/evaluation/evaluation_parallel_to_training.py"],
    args = [
        "--as-test",
        "--evaluation-parallel-to-training",
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
    name = "examples/evaluation/evaluation_parallel_to_training_multi_agent_10_episodes",
    size = "medium",
    srcs = ["examples/evaluation/evaluation_parallel_to_training.py"],
    args = [
        "--num-agents=2",
        "--as-test",
        "--evaluation-parallel-to-training",
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

# @OldAPIStack
py_test(
    name = "examples/evaluation/evaluation_parallel_to_training_duration_auto_old_api_stack",
    size = "medium",
    srcs = ["examples/evaluation/evaluation_parallel_to_training.py"],
    args = [
        "--old-api-stack",
        "--as-test",
        "--evaluation-parallel-to-training",
        "--stop-reward=50.0",
        "--num-cpus=6",
        "--evaluation-duration=auto",
        "--evaluation-duration-unit=timesteps",
    ],
    main = "examples/evaluation/evaluation_parallel_to_training.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

# @OldAPIStack
py_test(
    name = "examples/evaluation/evaluation_parallel_to_training_211_ts_old_api_stack",
    size = "medium",
    srcs = ["examples/evaluation/evaluation_parallel_to_training.py"],
    args = [
        "--old-api-stack",
        "--as-test",
        "--evaluation-parallel-to-training",
        "--framework=torch",
        "--stop-reward=30.0",
        "--num-cpus=6",
        "--evaluation-num-env-runners=3",
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

# subdirectory: fault_tolerance/
# ....................................
py_test(
    name = "examples/fault_tolerance/crashing_cartpole_recreate_failed_env_runners_appo",
    size = "large",
    srcs = ["examples/fault_tolerance/crashing_and_stalling_env.py"],
    args = [
        "--algo=APPO",
        "--as-test",
        "--stop-reward=450.0",
    ],
    main = "examples/fault_tolerance/crashing_and_stalling_env.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/fault_tolerance/crashing_cartpole_restart_failed_envs_appo",
    size = "large",
    srcs = ["examples/fault_tolerance/crashing_and_stalling_env.py"],
    args = [
        "--algo=APPO",
        "--as-test",
        "--restart-failed-envs",
        "--stop-reward=450.0",
    ],
    main = "examples/fault_tolerance/crashing_and_stalling_env.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/fault_tolerance/crashing_and_stalling_cartpole_restart_failed_envs_ppo",
    size = "large",
    srcs = ["examples/fault_tolerance/crashing_and_stalling_env.py"],
    args = [
        "--algo=PPO",
        "--as-test",
        "--restart-failed-envs",
        "--stall",
        "--stop-reward=450.0",
    ],
    main = "examples/fault_tolerance/crashing_and_stalling_env.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/fault_tolerance/crashing_and_stalling_multi_agent_cartpole_restart_failed_envs_ppo",
    size = "large",
    srcs = ["examples/fault_tolerance/crashing_and_stalling_env.py"],
    args = [
        "--algo=PPO",
        "--num-agents=2",
        "--as-test",
        "--restart-failed-envs",
        "--stop-reward=800.0",
    ],
    main = "examples/fault_tolerance/crashing_and_stalling_env.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

# subdirectory: gpus/
# ....................................
py_test(
    name = "examples/gpus/float16_training_and_inference",
    size = "medium",
    srcs = ["examples/gpus/float16_training_and_inference.py"],
    args = [
        "--as-test",
        "--stop-reward=150.0",
    ],
    main = "examples/gpus/float16_training_and_inference.py",
    tags = [
        "examples",
        "exclusive",
        "gpu",
        "team:rllib",
    ],
)

py_test(
    name = "examples/gpus/gpus_on_env_runners",
    size = "medium",
    srcs = ["examples/gpus/gpus_on_env_runners.py"],
    args = [
        "--as-test",
        "--stop-reward=0.9",
        "--num-gpus-per-env-runner=0.5",
        "--num-gpus-per-learner=0",
    ],
    main = "examples/gpus/gpus_on_env_runners.py",
    tags = [
        "examples",
        "exclusive",
        "gpu",
        "team:rllib",
    ],
)

py_test(
    name = "examples/gpus/mixed_precision_training_float16_inference",
    size = "medium",
    srcs = ["examples/gpus/mixed_precision_training_float16_inference.py"],
    args = [
        "--as-test",
    ],
    main = "examples/gpus/mixed_precision_training_float16_inference.py",
    tags = [
        "examples",
        "exclusive",
        "gpu",
        "team:rllib",
    ],
)

py_test(
    name = "examples/gpus/fractional_0.5_gpus_per_learner",
    size = "medium",
    srcs = ["examples/gpus/fractional_gpus_per_learner.py"],
    args = [
        "--as-test",
        "--stop-reward=40.0",
        "--num-learners=1",
        "--num-gpus-per-learner=0.5",
    ],
    main = "examples/gpus/fractional_gpus_per_learner.py",
    tags = [
        "examples",
        "exclusive",
        "multi_gpu",
        "team:rllib",
    ],
)

py_test(
    name = "examples/gpus/fractional_0.2_gpus_per_learner",
    size = "medium",
    srcs = ["examples/gpus/fractional_gpus_per_learner.py"],
    args = [
        "--as-test",
        "--stop-reward=40.0",
        "--num-learners=1",
        "--num-gpus-per-learner=0.2",
    ],
    main = "examples/gpus/fractional_gpus_per_learner.py",
    tags = [
        "examples",
        "exclusive",
        "gpu",
        "team:rllib",
    ],
)

# subdirectory: hierarchical/
# ....................................
# TODO (sven): Add this script to the release tests as well. The problem is too hard to be solved
#  in < 10min on a few CPUs.
py_test(
    name = "examples/hierarchical/hierarchical_training",
    size = "medium",
    srcs = ["examples/hierarchical/hierarchical_training.py"],
    args = [
        "--stop-iters=5",
        "--map=small",
        "--time-limit=100",
        "--max-steps-low-level=15",
    ],
    main = "examples/hierarchical/hierarchical_training.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

# subdirectory: inference/
# ....................................
py_test(
    name = "examples/inference/policy_inference_after_training",
    size = "medium",
    srcs = ["examples/inference/policy_inference_after_training.py"],
    args = [
        "--stop-reward=100.0",
    ],
    main = "examples/inference/policy_inference_after_training.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/inference/policy_inference_after_training_w_onnx",
    size = "medium",
    srcs = ["examples/inference/policy_inference_after_training.py"],
    args = [
        "--stop-reward=100.0",
        "--use-onnx-for-inference",
    ],
    main = "examples/inference/policy_inference_after_training.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/inference/policy_inference_after_training_w_connector",
    size = "medium",
    srcs = ["examples/inference/policy_inference_after_training_w_connector.py"],
    args = [
        "--stop-reward=150.0",
    ],
    main = "examples/inference/policy_inference_after_training_w_connector.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/inference/policy_inference_after_training_w_connector_w_onnx",
    size = "medium",
    srcs = ["examples/inference/policy_inference_after_training_w_connector.py"],
    args = [
        "--stop-reward=150.0",
        "--use-onnx-for-inference",
    ],
    main = "examples/inference/policy_inference_after_training_w_connector.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

#@OldAPIStack
py_test(
    name = "examples/inference/policy_inference_after_training_with_lstm_tf",
    size = "medium",
    srcs = ["examples/inference/policy_inference_after_training_with_lstm.py"],
    args = [
        "--stop-iters=1",
        "--framework=tf",
    ],
    main = "examples/inference/policy_inference_after_training_with_lstm.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

#@OldAPIStack
py_test(
    name = "examples/inference/policy_inference_after_training_with_lstm_torch",
    size = "medium",
    srcs = ["examples/inference/policy_inference_after_training_with_lstm.py"],
    args = [
        "--stop-iters=1",
        "--framework=torch",
    ],
    main = "examples/inference/policy_inference_after_training_with_lstm.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

# subdirectory: learners/
# ....................................
py_test(
    name = "examples/learners/ppo_with_custom_loss_fn",
    size = "medium",
    srcs = ["examples/learners/ppo_with_custom_loss_fn.py"],
    args = [
        "--as-test",
    ],
    main = "examples/learners/ppo_with_custom_loss_fn.py",
    tags = [
        "examples",
        "team:rllib",
    ],
)

py_test(
    name = "examples/learners/ppo_with_torch_lr_schedulers",
    size = "medium",
    srcs = ["examples/learners/ppo_with_torch_lr_schedulers.py"],
    args = [
        "--as-test",
    ],
    main = "examples/learners/ppo_with_torch_lr_schedulers.py",
    tags = [
        "examples",
        "team:rllib",
    ],
)

py_test(
    name = "examples/learners/separate_vf_lr_and_optimizer",
    size = "medium",
    srcs = ["examples/learners/separate_vf_lr_and_optimizer.py"],
    args = [
        "--as-test",
    ],
    main = "examples/learners/separate_vf_lr_and_optimizer.py",
    tags = [
        "examples",
        "team:rllib",
    ],
)

# subdirectory: metrics/
# ....................................

py_test(
    name = "examples/metrics/custom_metrics_in_algorithm_training_step",
    size = "medium",
    srcs = ["examples/metrics/custom_metrics_in_algorithm_training_step.py"],
    main = "examples/metrics/custom_metrics_in_algorithm_training_step.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/metrics/custom_metrics_in_env_runners",
    size = "medium",
    srcs = ["examples/metrics/custom_metrics_in_env_runners.py"],
    args = [
        "--stop-iters=3",
    ],
    main = "examples/metrics/custom_metrics_in_env_runners.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

# subdirectory: multi_agent/
# ....................................
py_test(
    name = "examples/multi_agent/custom_heuristic_policy",
    size = "large",
    srcs = ["examples/multi_agent/custom_heuristic_policy.py"],
    args = [
        "--num-agents=2",
        "--as-test",
        "--framework=torch",
        "--stop-reward=450.0",
    ],
    main = "examples/multi_agent/custom_heuristic_policy.py",
    tags = [
        "examples",
        "examples_use_all_core",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/multi_agent/different_spaces_for_agents_ppo",
    size = "small",
    srcs = ["examples/multi_agent/different_spaces_for_agents.py"],
    args = [
        "--algo=PPO",
        "--stop-iters=4",
        "--framework=torch",
    ],
    main = "examples/multi_agent/different_spaces_for_agents.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/multi_agent/multi_agent_cartpole",
    size = "large",
    srcs = ["examples/multi_agent/multi_agent_cartpole.py"],
    args = [
        "--num-agents=2",
        "--as-test",
        "--framework=torch",
        "--stop-reward=600.0",
        "--num-cpus=4",
    ],
    main = "examples/multi_agent/multi_agent_cartpole.py",
    tags = [
        "examples",
        "examples_use_all_core",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/multi_agent/multi_agent_pendulum_multi_gpu",
    size = "large",
    srcs = ["examples/multi_agent/multi_agent_pendulum.py"],
    args = [
        "--num-agents=2",
        "--as-test",
        "--framework=torch",
        "--stop-reward=-500.0",
        "--num-cpus=5",
        "--num-learners=2",
        "--num-gpus-per-learner=1",
    ],
    main = "examples/multi_agent/multi_agent_pendulum.py",
    tags = [
        "examples",
        "exclusive",
        "multi_gpu",
        "team:rllib",
    ],
)

py_test(
    name = "examples/multi_agent/pettingzoo_independent_learning",
    size = "large",
    srcs = ["examples/multi_agent/pettingzoo_independent_learning.py"],
    args = [
        "--num-agents=2",
        "--as-test",
        "--framework=torch",
        "--stop-reward=-200.0",
        "--num-cpus=4",
    ],
    main = "examples/multi_agent/pettingzoo_independent_learning.py",
    tags = [
        "examples",
        "team:rllib",
    ],
)

py_test(
    name = "examples/multi_agent/pettingzoo_parameter_sharing",
    size = "large",
    srcs = ["examples/multi_agent/pettingzoo_parameter_sharing.py"],
    args = [
        "--num-agents=2",
        "--as-test",
        "--framework=torch",
        "--stop-reward=-210.0",
        "--num-cpus=4",
    ],
    main = "examples/multi_agent/pettingzoo_parameter_sharing.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

# TODO (sven): Activate this test once this script is ready.
# py_test(
#    name = "examples/multi_agent/pettingzoo_shared_value_function",
#    main = "examples/multi_agent/pettingzoo_shared_value_function.py",
#    tags = ["team:rllib", "exclusive", "examples"],
#    size = "large",
#    srcs = ["examples/multi_agent/pettingzoo_shared_value_function.py"],
#    args = ["--num-agents=2", "--as-test", "--framework=torch", "--stop-reward=-100.0", "--num-cpus=4"],
# )

py_test(
    name = "examples/checkpoints/restore_1_of_n_agents_from_checkpoint",
    size = "large",
    srcs = ["examples/checkpoints/restore_1_of_n_agents_from_checkpoint.py"],
    args = [
        "--as-test",
        "--num-agents=2",
        "--framework=torch",
        "--checkpoint-freq=20",
        "--checkpoint-at-end",
        "--num-cpus=4",
        "--algo=PPO",
    ],
    main = "examples/checkpoints/restore_1_of_n_agents_from_checkpoint.py",
    tags = [
        "examples",
        "examples_use_all_core",
        "exclusive",
        "no_main",
        "team:rllib",
    ],
)

py_test(
    name = "examples/multi_agent/rock_paper_scissors_heuristic_vs_learned",
    size = "medium",
    srcs = ["examples/multi_agent/rock_paper_scissors_heuristic_vs_learned.py"],
    args = [
        "--num-agents=2",
        "--as-test",
        "--framework=torch",
        "--stop-reward=6.5",
    ],
    main = "examples/multi_agent/rock_paper_scissors_heuristic_vs_learned.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/multi_agent/rock_paper_scissors_heuristic_vs_learned_w_lstm",
    size = "large",
    srcs = ["examples/multi_agent/rock_paper_scissors_heuristic_vs_learned.py"],
    args = [
        "--num-agents=2",
        "--as-test",
        "--framework=torch",
        "--stop-reward=7.2",
        "--use-lstm",
        "--num-env-runners=4",
        "--num-cpus=6",
    ],
    main = "examples/multi_agent/rock_paper_scissors_heuristic_vs_learned.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/multi_agent/rock_paper_scissors_learned_vs_learned",
    size = "medium",
    srcs = ["examples/multi_agent/rock_paper_scissors_learned_vs_learned.py"],
    args = [
        "--num-agents=2",
        "--framework=torch",
        "--stop-iter=10",
    ],
    main = "examples/multi_agent/rock_paper_scissors_learned_vs_learned.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/multi_agent/self_play_footsies",
    size = "large",
    srcs = ["examples/multi_agent/self_play_footsies.py"],
    args = [
        "--as-test",
        "--num-cpus=4",
    ],
    main = "examples/multi_agent/self_play_footsies.py",
    tags = [
        "examples",
        "examples_use_all_core",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/multi_agent/self_play_league_based_with_open_spiel_connect_4_ppo_torch",
    size = "large",
    srcs = ["examples/multi_agent/self_play_league_based_with_open_spiel.py"],
    args = [
        "--framework=torch",
        "--env=connect_four",
        "--win-rate-threshold=0.8",
        "--num-episodes-human-play=0",
        "--min-league-size=8",
    ],
    main = "examples/multi_agent/self_play_league_based_with_open_spiel.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

# @OldAPIStack
py_test(
    name = "examples/multi_agent/self_play_with_open_spiel_connect_4_ppo_tf_old_api_stack",
    size = "medium",
    srcs = ["examples/multi_agent/self_play_with_open_spiel.py"],
    args = [
        "--old-api-stack",
        "--framework=tf",
        "--env=connect_four",
        "--win-rate-threshold=0.9",
        "--num-episodes-human-play=0",
        "--min-league-size=3",
    ],
    main = "examples/multi_agent/self_play_with_open_spiel.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

# @OldAPIStack
py_test(
    name = "examples/multi_agent/self_play_with_open_spiel_connect_4_ppo_torch_old_api_stack",
    size = "medium",
    srcs = ["examples/multi_agent/self_play_with_open_spiel.py"],
    args = [
        "--old-api-stack",
        "--framework=torch",
        "--env=connect_four",
        "--win-rate-threshold=0.9",
        "--num-episodes-human-play=0",
        "--min-league-size=3",
    ],
    main = "examples/multi_agent/self_play_with_open_spiel.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/multi_agent/self_play_with_open_spiel_connect_4_ppo_torch",
    size = "medium",
    srcs = ["examples/multi_agent/self_play_with_open_spiel.py"],
    args = [
        "--framework=torch",
        "--env=connect_four",
        "--win-rate-threshold=0.9",
        "--num-episodes-human-play=0",
        "--min-league-size=4",
    ],
    main = "examples/multi_agent/self_play_with_open_spiel.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/multi_agent/shared_encoder_cartpole",
    size = "medium",
    srcs = ["examples/multi_agent/shared_encoder_cartpole.py"],
    args = [
        "--stop-iter=10",
    ],
    main = "examples/multi_agent/shared_encoder_cartpole.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/multi_agent/two_step_game_with_grouped_agents",
    size = "medium",
    srcs = ["examples/multi_agent/two_step_game_with_grouped_agents.py"],
    args = [
        "--num-agents=2",
        "--as-test",
        "--framework=torch",
        "--stop-reward=7.0",
    ],
    main = "examples/multi_agent/two_step_game_with_grouped_agents.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

# subdirectory: offline_rl/
# ....................................

# Does run into scheduling problems in CI tests. Works on local
# and GCP cloud.
# py_test(
#     name = "examples/offline_rl/cartpole_recording",
#     main = "examples/offline_rl/cartpole_recording.py",
#     tags = ["team:rllib", "examples", "exclusive"],
#     size = "large",
#     srcs = ["examples/offline_rl/cartpole_recording.py"],
#     args = ["--as-test", "--framework=torch", "--num-cpus=12"],
# )

py_test(
    name = "examples/offline_rl/train_w_bc_finetune_w_ppo",
    size = "medium",
    srcs = ["examples/offline_rl/train_w_bc_finetune_w_ppo.py"],
    args = [
        "--as-test",
        "--framework=torch",
    ],
    # Include the offline data files.
    data = ["tests/data/cartpole/cartpole-v1_large"],
    main = "examples/offline_rl/train_w_bc_finetune_w_ppo.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

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
py_test(
    name = "examples/offline_rl/offline_rl_torch_old_api_stack",
    size = "medium",
    srcs = ["examples/offline_rl/offline_rl.py"],
    args = [
        "--as-test",
        "--stop-reward=-300",
        "--stop-iters=1",
    ],
    main = "examples/offline_rl/offline_rl.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

# subdirectory: ray_serve/
# ....................................
py_test(
    name = "examples/ray_serve/ray_serve_with_rllib",
    size = "medium",
    srcs = ["examples/ray_serve/ray_serve_with_rllib.py"],
    args = [
        "--stop-iters=2",
        "--num-episodes-served=2",
        "--no-render",
        "--port=12345",
    ],
    data = glob(["examples/ray_serve/classes/**"]),
    main = "examples/ray_serve/ray_serve_with_rllib.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

# subdirectory: ray_tune/
# ....................................
py_test(
    name = "examples/ray_tune/custom_experiment",
    size = "medium",
    srcs = ["examples/ray_tune/custom_experiment.py"],
    main = "examples/ray_tune/custom_experiment.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/ray_tune/custom_logger",
    size = "medium",
    srcs = ["examples/ray_tune/custom_logger.py"],
    main = "examples/ray_tune/custom_logger.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

py_test(
    name = "examples/ray_tune/custom_progress_reporter",
    size = "medium",
    srcs = ["examples/ray_tune/custom_progress_reporter.py"],
    main = "examples/ray_tune/custom_progress_reporter.py",
    tags = [
        "examples",
        "exclusive",
        "team:rllib",
    ],
)

# subdirectory: rl_modules/
# ....................................
py_test(
    name = "examples/rl_modules/action_masking_rl_module",
    size = "medium",
    srcs = ["examples/rl_modules/action_masking_rl_module.py"],
    args = [
        "--stop-iters=5",
    ],
    main = "examples/rl_modules/action_masking_rl_module.py",
    tags = [
        "examples",
        "team:rllib",
    ],
)

py_test(
    name = "examples/rl_modules/custom_cnn_rl_module",
    size = "medium",
    srcs = ["examples/rl_modules/custom_cnn_rl_module.py"],
    args = [
        "--stop-iters=3",
    ],
    main = "examples/rl_modules/custom_cnn_rl_module.py",
    tags = [
        "examples",
        "team:rllib",
    ],
)

py_test(
    name = "examples/rl_modules/custom_lstm_rl_module",
    size = "large",
    srcs = ["examples/rl_modules/custom_lstm_rl_module.py"],
    args = [
        "--as-test",
    ],
    main = "examples/rl_modules/custom_lstm_rl_module.py",
    tags = [
        "examples",
        "team:rllib",
    ],
)

py_test(
    name = "examples/rl_modules/classes/mobilenet_rlm",
    size = "small",
    srcs = ["examples/rl_modules/classes/mobilenet_rlm.py"],
    main = "examples/rl_modules/classes/mobilenet_rlm.py",
    tags = [
        "examples",
        "no_main",
        "team:rllib",
    ],
)

py_test(
    name = "examples/rl_modules/migrate_modelv2_to_new_api_stack_by_config",
    size = "large",
    srcs = ["examples/rl_modules/migrate_modelv2_to_new_api_stack_by_config.py"],
    main = "examples/rl_modules/migrate_modelv2_to_new_api_stack_by_config.py",
    tags = [
        "examples",
        "team:rllib",
    ],
)

py_test(
    name = "examples/rl_modules/migrate_modelv2_to_new_api_stack_by_policy_checkpoint",
    size = "large",
    srcs = ["examples/rl_modules/migrate_modelv2_to_new_api_stack_by_policy_checkpoint.py"],
    main = "examples/rl_modules/migrate_modelv2_to_new_api_stack_by_policy_checkpoint.py",
    tags = [
        "examples",
        "team:rllib",
    ],
)

py_test(
    name = "examples/rl_modules/pretraining_single_agent_training_multi_agent",
    size = "medium",
    srcs = ["examples/rl_modules/pretraining_single_agent_training_multi_agent.py"],
    args = [
        "--as-test",
        "--num-agents=2",
        "--stop-reward-pretraining=250.0",
        "--stop-reward=250.0",
        "--stop-iters=3",
    ],
    main = "examples/rl_modules/pretraining_single_agent_training_multi_agent.py",
    tags = [
        "examples",
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

# --------------------------------------------------------------------
# Manual/disabled tests
# --------------------------------------------------------------------
py_test_module_list(
    size = "large",
    extra_srcs = [],
    files = [
        "algorithms/dreamerv3/tests/test_dreamerv3.py",
        "offline/tests/test_offline_prelearner.py",
        "utils/tests/test_utils.py",
    ],
    tags = [
        "manual",
        "no_main",
        "team:rllib",
    ],
    deps = [],
)
