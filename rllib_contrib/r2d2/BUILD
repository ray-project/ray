# Examples

py_test(
    name = "example_r2d2_stateless_cartpole",
    size = "large",
    srcs = ["examples/r2d2_stateless_cartpole.py"],
    args = ["--run-as-test"],
    main = "r2d2_stateless_cartpole.py",
    tags = [
        "example",
        "team:rllib",
    ],
)

# Learning Tests

py_test(
    name = "learning_tests_stateless_cartpole_r2d2",
    size = "enormous",
    srcs = ["run_regression_tests.py"],
    args = ["--dir=r2d2/tuned_examples/"],
    data = ["tuned_examples/stateless-cartpole-r2d2.yaml"],
    main = "run_regression_tests.py",
    tags = [
        "learning_tests",
        "rllib_contrib",
        "team:rllib",
    ],
)

py_test(
    name = "learning_tests_stateless_cartpole_r2d2_fake_gpus",
    size = "enormous",
    srcs = ["run_regression_tests.py"],
    args = ["--dir=r2d2/tuned_examples/"],
    data = ["tuned_examples/stateless-cartpole-r2d2-fake-gpus.yaml"],
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
    name = "test_r2d2",
    size = "large",
    srcs = ["tests/test_r2d2.py"],
    tags = [
        "algorithms_dir",
        "team:rllib",
    ],
)
