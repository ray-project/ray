# Examples

py_test(
    name = "example_pg_cartpole_v1",
    size = "large",
    srcs = ["examples/pg_cartpole_v1.py"],
    args = ["--run-as-test"],
    main = "pg_cartpole_v1.py",
    tags = [
        "example",
        "team:rllib",
    ],
)

# Learning Tests

py_test(
    name = "learning_tests_cartpole_pg",
    size = "large",
    srcs = ["run_regression_tests.py"],
    args = ["--dir=pg/tuned_examples"],
    data = ["tuned_examples/cartpole-pg.yaml"],
    main = "run_regression_tests.py",
    tags = [
        "learning_tests",
        "rllib_contrib",
        "team:rllib",
    ],
)

# Compilation Tests

py_test(
    name = "test_pg",
    size = "large",
    srcs = ["tests/test_pg.py"],
    tags = [
        "algorithms_dir",
        "team:rllib",
    ],
)
