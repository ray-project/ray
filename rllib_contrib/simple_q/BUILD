# Examples

py_test(
    name = "example_simple_q_cartpole_v1",
    size = "large",
    srcs = ["examples/simple_q_cartpole_v1.py"],
    args = ["--run-as-test"],
    main = "simple_q_cartpole_v1.py",
    tags = [
        "example",
        "team:rllib",
    ],
)

# Learning Tests

py_test(
    name = "learning_tests_cartpole_simpleq",
    size = "medium",
    srcs = ["run_regression_tests.py"],
    args = ["--dir=simple_q/tuned_examples"],
    data = ["tuned_examples/cartpole-simpleq.yaml"],
    main = "run_regression_tests.py",
    tags = [
        "learning_tests",
        "rllib_contrib",
        "team:rllib",
    ],
)

py_test(
    name = "learning_tests_cartpole_simpleq_fake_gpus",
    size = "medium",
    srcs = ["run_regression_tests.py"],
    args = ["--dir=simple_q/tuned_examples"],
    data = ["tuned_examples/cartpole-simpleq-fake-gpus.yaml"],
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
    name = "test_simple_q",
    size = "large",
    srcs = ["tests/test_simple_q.py"],
    tags = [
        "algorithms_dir",
        "team:rllib",
    ],
)

py_test(
    name = "test_repro_simple_q",
    size = "large",
    srcs = ["tests/test_repro_simple_q.py"],
    tags = [
        "algorithms_dir",
        "team:rllib",
    ],
)
