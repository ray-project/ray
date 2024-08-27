# Examples

py_test(
    name = "example_pg_cartpole_v1",
    main = "pg_cartpole_v1.py",
    tags = ["team:rllib", "example"],
    size = "large",
    srcs = ["examples/pg_cartpole_v1.py"],
    args = ["--run-as-test"]
)

# Learning Tests

py_test(
    name = "learning_tests_cartpole_pg",
    main = "run_regression_tests.py",
    tags = ["team:rllib", "learning_tests", "rllib_contrib"],
    size = "large",
    srcs = ["run_regression_tests.py"],
    data = ["tuned_examples/cartpole-pg.yaml"],
    args = ["--dir=pg/tuned_examples"]
)

# Compilation Tests

py_test(
    name = "test_pg",
    tags = ["team:rllib", "algorithms_dir"],
    size = "large",
    srcs = ["tests/test_pg.py"]
)
