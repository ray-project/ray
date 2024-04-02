# Examples

py_test(
    name = "example_dt_cartpole_v1",
    size = "large",
    srcs = ["examples/dt_cartpole_v1.py"],
    args = ["--run-as-test"],
    main = "dt_cartpole_v1.py",
    tags = [
        "example",
        "team:rllib",
    ],
)

py_test(
    name = "policy_inference_after_training_with_dt_torch",
    size = "medium",
    srcs = ["examples/policy_inference_after_training_with_dt.py"],
    main = "policy_inference_after_training_with_dt.py",
    tags = [
        "examples",
        "ray_data",
        "team:rllib",
    ],
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
    size = "medium",
    srcs = ["run_regression_tests.py"],
    args = ["--dir=dt/tuned_examples/"],
    data = ["tuned_examples/cartpole-v1-dt.yaml"],
    main = "run_regression_tests.py",
    tags = [
        "learning_tests",
        "rllib_contrib",
        "team:rllib",
        "torch_only",
    ],
)

# Compilation Tests

py_test(
    name = "test_dt",
    size = "large",
    srcs = ["tests/test_dt.py"],
    tags = [
        "algorithms_dir",
        "team:rllib",
    ],
)

py_test(
    name = "test_dt_model",
    size = "large",
    srcs = ["tests/test_dt_model.py"],
    tags = [
        "algorithms_dir",
        "team:rllib",
    ],
)

py_test(
    name = "test_dt_policy",
    size = "large",
    srcs = ["tests/test_dt_policy.py"],
    tags = [
        "algorithms_dir",
        "team:rllib",
    ],
)

py_test(
    name = "test_segmentation_buffer",
    size = "large",
    srcs = ["tests/test_segmentation_buffer.py"],
    tags = [
        "algorithms_dir",
        "team:rllib",
    ],
)
