# Examples

py_test(
    name = "example_ddppo_pendulum_v1",
    size = "large",
    srcs = ["examples/ddppo_pendulum_v1.py"],
    args = ["--run-as-test"],
    main = "ddppo_pendulum_v1.py",
    tags = [
        "example",
        "team:rllib",
    ],
)

# Learning Tests

py_test(
    name = "learning_tests_cartpole_ddppo",
    size = "small",
    srcs = ["run_regression_tests.py"],
    args = ["--dir=ddppo/tuned_examples/"],
    data = glob(["tuned_examples/cartpole-ddppo.yaml"]),
    main = "run_regression_tests.py",
    tags = [
        "learning_tests",
        "rllib_contrib",
        "team:rllib",
        "torch_only",
    ],
)

py_test(
    name = "learning_tests_pendulum_ddppo",
    size = "enormous",
    srcs = ["run_regression_tests.py"],
    args = ["--dir=ddppo/tuned_examples/"],
    data = glob(["tuned_examples/pendulum-ddppo.yaml"]),
    main = "run_regression_tests.py",
    tags = [
        "learning_tests",
        "rllib_contrib",
        "team:rllib",
        "torch_only",
    ],
)

# Compilation Tests

py_test(
    name = "test_ddppo",
    size = "large",
    srcs = ["tests/test_ddppo.py"],
    tags = [
        "algorithms_dir",
        "team:rllib",
    ],
)
