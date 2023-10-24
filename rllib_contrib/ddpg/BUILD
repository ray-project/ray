# Learning Tests

py_test(
   name = "learning_tests_pendulum_ddpg_fake_gpus",
   main = "run_regression_tests.py",
   tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_pendulum", "learning_tests_continuous", "fake_gpus"],
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
