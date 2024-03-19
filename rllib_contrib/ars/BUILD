# Examples

py_test(
    name = "example_ars_cartpole_v1",
    size = "large",
    srcs = ["examples/ars_cartpole_v1.py"],
    args = ["--run-as-test"],
    main = "ars_cartpole_v1.py",
    tags = [
        "example",
        "team:rllib",
    ],
)

# Learning Tests

py_test(
    name = "learning_tests_cartpole_ars",
    size = "medium",
    srcs = ["run_regression_tests.py"],
    args = ["--dir=ars/tuned_examples/"],
    data = ["tuned_examples/cartpole-ars.yaml"],
    main = "run_regression_tests.py",
    tags = [
        "learning_tests",
        "rllib_contrib",
        "team:rllib",
    ],
)

# Compilation Tests

py_test(
    name = "test_ars",
    size = "large",
    srcs = ["tests/test_ars.py"],
    tags = [
        "algorithms_dir",
        "team:rllib",
    ],
)
