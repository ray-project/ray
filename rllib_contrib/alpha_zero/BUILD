# Examples

py_test(
    name = "example_alpha_zero_cartpole_sparse_rewards",
    size = "large",
    srcs = ["examples/alpha_zero_cartpole_sparse_rewards.py"],
    args = ["--run-as-test"],
    main = "alpha_zero_cartpole_sparse_rewards.py",
    tags = [
        "example",
        "team:rllib",
    ],
)

# Learning Tests

py_test(
    name = "learning_tests_cartpole_sparse_rewards_alpha_zero",
    size = "medium",
    srcs = ["run_regression_tests.py"],
    args = [
        "--dir=alpha_zero/tuned_examples/",
        "--num-cpus=8",
    ],
    data = ["tuned_examples/cartpole-sparse-rewards-alpha-zero.yaml"],
    main = "run_regression_tests.py",
    tags = [
        "learning_tests",
        "rllib_contrib",
        "team:rllib",
        "torch_only",
    ],
)

# Compilation Tests

py_test(
    name = "test_alpha_zero",
    size = "large",
    srcs = ["tests/test_alpha_zero.py"],
    tags = [
        "algorithms_dir",
        "team:rllib",
    ],
)
