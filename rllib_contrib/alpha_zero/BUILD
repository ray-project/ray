# Learning Tests

py_test(
    name = "learning_tests_cartpole_sparse_rewards_alpha_zero",
    tags = ["team:rllib", "exclusive", "torch_only", "learning_tests", "learning_tests_discrete"],
    main = "run_regression_tests.py",
    size = "medium",
    srcs = ["run_regression_tests.py"],
    data = ["tuned_examples/cartpole-sparse-rewards-alpha-zero.yaml"],
    args = ["--dir=alpha_zero/tuned_examples/", "--num-cpus=8"]
)

# Compilation Tests

py_test(
    name = "test_alpha_zero",
    tags = ["team:rllib", "algorithms_dir"],
    size = "large",
    srcs = ["tests/test_alpha_zero.py"]
)
