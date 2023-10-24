# Learning Tests

py_test(
    name = "learning_tests_cartpole_a2c",
    main = "run_regression_tests.py",
    tags = ["team:rllib", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete"],
    size = "large",
    srcs = ["run_regression_tests.py"],
    data = ["tuned_examples/cartpole-a2c.yaml"],
    args = ["--dir=a2c/tuned_examples/"]
)

py_test(
    name = "learning_tests_cartpole_a2c_microbatch",
    main = "run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete"],
    size = "large",
    srcs = ["run_regression_tests.py"],
    data = ["tuned_examples/cartpole-a2c-microbatch.yaml"],
    args = ["--dir=a2c/tuned_examples/"]
)

py_test(
    name = "learning_tests_cartpole_a2c_fake_gpus",
    main = "run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete", "fake_gpus"],
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
