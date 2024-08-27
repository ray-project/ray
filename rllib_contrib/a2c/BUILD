# Examples

py_test(
    name = "example_a2c_cartpole_v1",
    main = "a2c_cartpole_v1.py",
    tags = ["team:rllib", "example"],
    size = "large",
    srcs = ["examples/a2c_cartpole_v1.py"],
    args = ["--run-as-test"]
)

# Learning Tests

py_test(
    name = "learning_tests_cartpole_a2c",
    main = "run_regression_tests.py",
    tags = ["team:rllib", "learning_tests", "rllib_contrib"],
    size = "large",
    srcs = ["run_regression_tests.py"],
    data = ["tuned_examples/cartpole-a2c.yaml"],
    args = ["--dir=a2c/tuned_examples/"]
)

py_test(
    name = "learning_tests_cartpole_a2c_microbatch",
    main = "run_regression_tests.py",
    tags = ["team:rllib", "learning_tests", "rllib_contrib"],
    size = "large",
    srcs = ["run_regression_tests.py"],
    data = ["tuned_examples/cartpole-a2c-microbatch.yaml"],
    args = ["--dir=a2c/tuned_examples/"]
)

py_test(
    name = "learning_tests_cartpole_a2c_fake_gpus",
    main = "run_regression_tests.py",
    tags = ["team:rllib", "learning_tests", "rllib_contrib", "no_tf_eager_tracing"],
    size = "medium",
    srcs = ["run_regression_tests.py"],
    data = ["tuned_examples/cartpole-a2c-fake-gpus.yaml"],
    args = ["--dir=a2c/tuned_examples/"]
)

# Compilation Tests

py_test(
    name = "test_a2c",
    tags = ["team:rllib", "algorithms_dir"],
    size = "large",
    srcs = ["tests/test_a2c.py"]
)
