# Examples

py_test(
    name = "example_ddpg_pendulum_v1",
    size = "large",
    srcs = ["examples/ddpg_pendulum_v1.py"],
    args = ["--run-as-test"],
    main = "ddpg_pendulum_v1.py",
    tags = [
        "example",
        "team:rllib",
    ],
)

# Learning Tests

py_test(
    name = "learning_tests_pendulum_ddpg_fake_gpus",
    size = "large",
    srcs = ["run_regression_tests.py"],
    args = ["--dir=ddpg/tuned_examples/"],
    data = ["tuned_examples/pendulum-ddpg-fake-gpus.yaml"],
    main = "run_regression_tests.py",
    tags = [
        "learning_tests",
        "no_tf_eager_tracing",
        "rllib_contrib",
        "team:rllib",
    ],
)

# Compilation Tests

py_test(
    name = "test_ddpg",
    size = "large",
    srcs = ["tests/test_ddpg.py"],
    tags = [
        "algorithms_dir",
        "team:rllib",
    ],
)
