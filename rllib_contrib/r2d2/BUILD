# Examples

py_test(
    name = "example_r2d2_stateless_cartpole",
    main = "r2d2_stateless_cartpole.py",
    tags = ["team:rllib", "example"],
    size = "large",
    srcs = ["examples/r2d2_stateless_cartpole.py"],
    args = ["--run-as-test"]
)

# Learning Tests

py_test(
    name = "learning_tests_stateless_cartpole_r2d2",
    main = "run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete", "stateless_cartpole"],
    size = "enormous",
    srcs = ["run_regression_tests.py"],
    data = ["tuned_examples/stateless-cartpole-r2d2.yaml"],
    args = ["--dir=r2d2/tuned_examples/"]
)

py_test(
    name = "learning_tests_stateless_cartpole_r2d2_fake_gpus",
    main = "run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_cartpole", "stateless_cartpole", "fake_gpus"],
    size = "enormous",
    srcs = ["run_regression_tests.py"],
    data = ["tuned_examples/stateless-cartpole-r2d2-fake-gpus.yaml"],
    args = ["--dir=r2d2/tuned_examples/"]
)

# Compilation Tests

py_test(
    name = "test_r2d2",
    tags = ["team:rllib", "algorithms_dir"],
    size = "large",
    srcs = ["tests/test_r2d2.py"]
)
