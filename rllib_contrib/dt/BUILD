# Examples

py_test(
    name = "example_dt_cartpole_v1",
    main = "dt_cartpole_v1.py",
    tags = ["team:rllib", "example"],
    size = "large",
    srcs = ["examples/dt_cartpole_v1.py"],
    args = ["--run-as-test"]
)

# Learning Tests

# py_test(
#    name = "learning_tests_pendulum_dt",
#    main = "run_regression_tests.py",
#    tags = ["team:rllib", "learning_tests", "rllib_contrib", "torch_only"],
#    size = "large",
#    srcs = ["run_regression_tests.py"],
#    data = [
#        "tuned_examples/pendulum-v1-dt.yaml",
#        # Include the offline json data file as well.
#        "tuned_examples/pendulum_expert_sac_50eps.zip",
#    ],
#    args = ["--dir=dt/tuned_examples/"]
# )

py_test(
    name = "learning_tests_cartpole_dt",
    main = "run_regression_tests.py",
    tags = ["team:rllib", "learning_tests", "rllib_contrib", "torch_only"],
    size = "medium",
    srcs = ["run_regression_tests.py"],
    data = ["tuned_examples/cartpole-v1-dt.yaml"],
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
