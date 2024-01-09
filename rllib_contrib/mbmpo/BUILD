# Examples

py_test(
    name = "example_mbmpo_cartpole_v1_model_based",
    main = "mbmpo_cartpole_v1_model_based.py",
    tags = ["team:rllib", "example"],
    size = "large",
    srcs = ["examples/mbmpo_cartpole_v1_model_based.py"],
    args = ["--run-as-test"]
)

# Learning Tests

# Working, but takes a long time to learn (>15min).
# Removed due to Higher API conflicts with Pytorch-Import tests
# MB-MPO
# py_test(
#    name = "learning_tests_pendulum_mbmpo",
#    main = "run_regression_tests.py",
#    tags = ["team:rllib", "learning_tests", "rllib_contrib", "torch_only"],
#    size = "large",
#    srcs = ["run_regression_tests.py"],
#    data = ["tuned_examples/pendulum-mbmpo.yaml"],
#    args = ["--dir=mbmpo/tuned_examples/"]
# )

# Compilation Tests

py_test(
    name = "test_mbmpo",
    tags = ["team:rllib", "algorithms_dir"],
    size = "large",
    srcs = ["tests/test_mbmpo.py"]
)
