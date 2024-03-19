# Examples

py_test(
    name = "example_a3c_cartpole_v1",
    size = "large",
    srcs = ["examples/a3c_cartpole_v1.py"],
    args = ["--run-as-test"],
    main = "a3c_cartpole_v1.py",
    tags = [
        "example",
        "team:rllib",
    ],
)

# Learning Tests

py_test(
    name = "learning_tests_cartpole_a3c",
    size = "large",
    srcs = ["run_regression_tests.py"],
    args = ["--dir=a3c/tuned_examples/"],
    data = ["tuned_examples/cartpole-a3c.yaml"],
    main = "run_regression_tests.py",
    tags = [
        "learning_tests",
        "rllib_contrib",
        "team:rllib",
    ],
)

# Compilation Tests

py_test(
    name = "test_a3c",
    size = "large",
    srcs = ["tests/test_a3c.py"],
    tags = [
        "algorithms_dir",
        "team:rllib",
    ],
)
