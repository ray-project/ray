# Examples

py_test(
    name = "example_crr_cartpole_v1",
    main = "crr_cartpole_v1.py",
    tags = ["team:rllib", "example"],
    size = "large",
    srcs = ["examples/crr_cartpole_v1.py"],
    args = ["--run-as-test"]
)

# Learning Tests

# py_test(
#   name = "learning_tests_pendulum_crr",
#   main = "run_regression_tests.py",
#    tags = ["team:rllib", "learning_tests", "rllib_contrib", "torch_only"],
#   size = "enormous",
#   srcs = ["run_regression_tests.py"],
#   data = [
#        "tuned_examples/pendulum-v1-crr.yaml",
#        # Include the offline json data file as well.
#        "tuned_examples/pendulum_replay_v1.1.0.zip",
#   ],
#   args = ["--dir=crr/tuned_examples/"]
# )


# Compilation Tests

py_test(
    name = "test_crr",
    tags = ["team:rllib", "algorithms_dir"],
    size = "large",
    srcs = ["tests/test_crr.py"]
)
