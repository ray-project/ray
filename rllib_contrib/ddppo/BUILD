# Examples

py_test(
    name = "example_ddppo_pendulum_v1",
    main = "ddppo_pendulum_v1.py",
    tags = ["team:rllib", "example"],
    size = "large",
    srcs = ["examples/ddppo_pendulum_v1.py"],
    args = ["--run-as-test"]
)

# Learning Tests

py_test(
    name = "learning_tests_cartpole_ddppo",
    main = "run_regression_tests.py",
    tags = ["team:rllib", "learning_tests", "rllib_contrib", "torch_only"],
    size = "small",
    srcs = ["run_regression_tests.py"],
    data = glob(["tuned_examples/cartpole-ddppo.yaml"]),
    args = ["--dir=ddppo/tuned_examples/"]
)

py_test(
    name = "learning_tests_pendulum_ddppo",
    main = "run_regression_tests.py",
    tags = ["team:rllib", "learning_tests", "rllib_contrib", "torch_only"],
    size = "enormous",
    srcs = ["run_regression_tests.py"],
    data = glob(["tuned_examples/pendulum-ddppo.yaml"]),
    args = ["--dir=ddppo/tuned_examples/"]
)


# Compilation Tests

py_test(
    name = "test_ddppo",
    tags = ["team:rllib", "algorithms_dir"],
    size = "large",
    srcs = ["tests/test_ddppo.py"]
)
