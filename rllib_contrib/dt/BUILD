# Examples

py_test(
    name = "example_dt_cartpole_v1",
    main = "dt_cartpole_v1.py",
    tags = ["team:rllib", "example"],
    size = "medium",
    srcs = ["examples/dt_cartpole_v1.py"],
    args = ["--run-as-test"]
)

# Learning Tests

py_test(
   name = "learning_tests_pendulum_dt",
   main = "run_regression_tests.py",
   tags = ["team:rllib", "torch_only", "learning_tests", "learning_tests_pendulum", "learning_tests_continuous", "learning_tests_with_ray_data"],
   size = "large",
   srcs = ["run_regression_tests.py"],
   # Include an offline json data file as well.
   data = [
       "tuned_examples/pendulum-v1-dt.yaml",
       "tests/data/pendulum/pendulum_expert_sac_50eps.zip",
   ],
   args = ["--dir=dt/tuned_examples/"]
)

py_test(
    name = "learning_tests_cartpole_dt",
    main = "run_regression_tests.py",
    tags = ["team:rllib", "torch_only", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete", "learning_tests_with_ray_data"],
    size = "medium",
    srcs = ["run_regression_tests.py"],
    # Include an offline json data file as well.
    data = [
        "tuned_examples/cartpole-v1-dt.yaml",
        "tests/data/cartpole/large.json",
    ],
    args = ["--dir=dt/tuned_examples/"]
)


# Compilation Tests

py_test(
    name = "test_dt",
    tags = ["team:rllib", "algorithms_dir"],
    size = "large",
    srcs = ["tests/test_dt.py"]
)

py_test(
    name = "test_dt_model",
    tags = ["team:rllib", "algorithms_dir"],
    size = "large",
    srcs = ["tests/test_dt_model.py"]
)

py_test(
    name = "test_dt_policy",
    tags = ["team:rllib", "algorithms_dir"],
    size = "large",
    srcs = ["tests/test_dt_policy.py"]
)

py_test(
    name = "test_segmentation_buffer",
    tags = ["team:rllib", "algorithms_dir"],
    size = "large",
    srcs = ["tests/test_segmentation_buffer.py"]
)
