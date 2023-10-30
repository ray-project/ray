# Examples

py_test(
    name = "example_ddpg_pendulum_v1",
    main = "ddpg_pendulum_v1.py",
    tags = ["team:rllib", "example"],
    size = "large",
    srcs = ["examples/ddpg_pendulum_v1.py"],
    args = ["--run-as-test"]
)

# Learning Tests

py_test(
   name = "learning_tests_pendulum_ddpg_fake_gpus",
   main = "run_regression_tests.py",
    tags = ["team:rllib", "learning_tests", "rllib_contrib", "no_tf_eager_tracing"],
   size = "large",
   srcs = ["run_regression_tests.py"],
   data = ["tuned_examples/pendulum-ddpg-fake-gpus.yaml"],
   args = ["--dir=ddpg/tuned_examples/"]
)

# Compilation Tests

py_test(
    name = "test_ddpg",
    tags = ["team:rllib", "algorithms_dir"],
    size = "large",
    srcs = ["tests/test_ddpg.py"]
)
