# Examples

py_test(
    name = "example_td3_pendulum_v1",
    main = "td3_pendulum_v1.py",
    tags = ["team:rllib", "example"],
    size = "large",
    srcs = ["examples/td3_pendulum_v1.py"],
    args = ["--run-as-test"]
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
    tags = ["team:rllib", "algorithms_dir"],
    size = "large",
    srcs = ["tests/test_td3.py"]
)
