# Learning Tests

py_test(
    name = "learning_tests_cartpole_a3c",
    main = "tests/run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete"],
    size = "large",
    srcs = ["tests/run_regression_tests.py"],
    data = ["tuned_examples/cartpole-a3c.yaml"],
    args = ["--dir=tuned_examples/"]
)

# Compilation Tests

py_test(
    name = "test_a3c",
    tags = ["team:rllib", "algorithms_dir"],
    size = "large",
    srcs = ["tests/test_a3c.py"]
)
