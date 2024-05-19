# Examples

py_test(
    name = "example_simple_q_cartpole_v1",
    main = "simple_q_cartpole_v1.py",
    tags = ["team:rllib", "example"],
    size = "large",
    srcs = ["examples/simple_q_cartpole_v1.py"],
    args = ["--run-as-test"]
)

# Learning Tests

py_test(
    name = "learning_tests_cartpole_simpleq",
    main = "run_regression_tests.py",
    tags = ["team:rllib", "learning_tests", "rllib_contrib"],
    size = "medium",
    srcs = ["run_regression_tests.py"],
    data = ["tuned_examples/cartpole-simpleq.yaml"],
    args = ["--dir=simple_q/tuned_examples"]
)

py_test(
    name = "learning_tests_cartpole_simpleq_fake_gpus",
    main = "run_regression_tests.py",
    tags = ["team:rllib", "learning_tests", "rllib_contrib", "no_tf_eager_tracing"],
    size = "medium",
    srcs = ["run_regression_tests.py"],
    data = ["tuned_examples/cartpole-simpleq-fake-gpus.yaml"],
    args = ["--dir=simple_q/tuned_examples"]
)


# Compilation Tests

py_test(
    name = "test_simple_q",
    tags = ["team:rllib", "algorithms_dir"],
    size = "large",
    srcs = ["tests/test_simple_q.py"]
)

py_test(
    name = "test_repro_simple_q",
    tags = ["team:rllib", "algorithms_dir"],
    size = "large",
    srcs = ["tests/test_repro_simple_q.py"]
)
