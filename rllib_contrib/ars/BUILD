# Examples

py_test(
    name = "example_ars_cartpole_v1",
    main = "ars_cartpole_v1.py",
    tags = ["team:rllib", "example"],
    size = "large",
    srcs = ["examples/ars_cartpole_v1.py"],
    args = ["--run-as-test"]
)

# Learning Tests

py_test(
    name = "learning_tests_cartpole_ars",
    main = "run_regression_tests.py",
    tags = ["team:rllib", "learning_tests", "rllib_contrib"],
    size = "medium",
    srcs = ["run_regression_tests.py"],
    data = ["tuned_examples/cartpole-ars.yaml"],
    args = ["--dir=ars/tuned_examples/"]
)

# Compilation Tests

py_test(
    name = "test_ars",
    tags = ["team:rllib", "algorithms_dir"],
    size = "large",
    srcs = ["tests/test_ars.py"]
)
