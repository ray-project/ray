# Examples

py_test(
    name = "example_td3_pendulum_v1",
    size = "large",
    srcs = ["examples/td3_pendulum_v1.py"],
    args = ["--run-as-test"],
    main = "td3_pendulum_v1.py",
    tags = [
        "example",
        "team:rllib",
    ],
)

# Learning Tests

# py_test(
#    name = "learning_tests_pendulum_td3",
#    main = "run_regression_tests.py",
#    tags = ["team:rllib", "learning_tests", "rllib_contrib"],
#    size = "large",
#    srcs = ["run_regression_tests.py"],
#    data = ["tuned_examples/pendulum-td3.yaml"],
#    args = ["--dir=td3/tuned_examples/"]
# )

# Compilation Tests

py_test(
    name = "test_td3",
    size = "large",
    srcs = ["tests/test_td3.py"],
    tags = [
        "algorithms_dir",
        "team:rllib",
    ],
)
