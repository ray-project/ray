# Examples

py_test(
    name = "example_a3c_cartpole_v1",
    main = "a3c_cartpole_v1.py",
    tags = ["team:rllib", "example"],
    size = "large",
    srcs = ["examples/a3c_cartpole_v1.py"],
    args = ["--run-as-test"]
)

# Learning Tests

py_test(
    name = "learning_tests_cartpole_a3c",
    main = "run_regression_tests.py",
    tags = ["team:rllib", "learning_tests", "rllib_contrib"],
    size = "large",
    srcs = ["run_regression_tests.py"],
    data = ["tuned_examples/cartpole-a3c.yaml"],
    args = ["--dir=a3c/tuned_examples/"]
)

# Compilation Tests

py_test(
    name = "test_a3c",
    tags = ["team:rllib", "algorithms_dir"],
    size = "large",
    srcs = ["tests/test_a3c.py"]
)
