# Learning Tests

py_test(
    name = "learning_tests_cartpole_ddppo",
    main = "run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "torch_only", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete"],
    size = "small",
    srcs = ["run_regression_tests.py"],
    data = glob(["tuned_examples/cartpole-ddppo.yaml"]),
    args = ["--dir=tuned_examples/"]
)

py_test(
    name = "learning_tests_pendulum_ddppo",
    main = "run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "torch_only", "learning_tests", "learning_tests_pendulum", "learning_tests_continuous"],
    size = "enormous",
    srcs = ["run_regression_tests.py"],
    data = glob(["tuned_examples/pendulum-ddppo.yaml"]),
    args = ["--dir=tuned_examples/"]
)


# Compilation Tests

py_test(
    name = "test_ddppo",
    tags = ["team:rllib", "algorithms_dir"],
    size = "large",
    srcs = ["tests/test_ddppo.py"]
)
