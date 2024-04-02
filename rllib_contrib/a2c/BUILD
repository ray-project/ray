# Examples

py_test(
    name = "example_a2c_cartpole_v1",
    size = "large",
    srcs = ["examples/a2c_cartpole_v1.py"],
    args = ["--run-as-test"],
    main = "a2c_cartpole_v1.py",
    tags = [
        "example",
        "team:rllib",
    ],
)

# Learning Tests

py_test(
    name = "learning_tests_cartpole_a2c",
    size = "large",
    srcs = ["run_regression_tests.py"],
    args = ["--dir=a2c/tuned_examples/"],
    data = ["tuned_examples/cartpole-a2c.yaml"],
    main = "run_regression_tests.py",
    tags = [
        "learning_tests",
        "rllib_contrib",
        "team:rllib",
    ],
)

py_test(
    name = "learning_tests_cartpole_a2c_microbatch",
    size = "large",
    srcs = ["run_regression_tests.py"],
    args = ["--dir=a2c/tuned_examples/"],
    data = ["tuned_examples/cartpole-a2c-microbatch.yaml"],
    main = "run_regression_tests.py",
    tags = [
        "learning_tests",
        "rllib_contrib",
        "team:rllib",
    ],
)

py_test(
    name = "learning_tests_cartpole_a2c_fake_gpus",
    size = "medium",
    srcs = ["run_regression_tests.py"],
    args = ["--dir=a2c/tuned_examples/"],
    data = ["tuned_examples/cartpole-a2c-fake-gpus.yaml"],
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
    name = "test_a2c",
    size = "large",
    srcs = ["tests/test_a2c.py"],
    tags = [
        "algorithms_dir",
        "team:rllib",
    ],
)
