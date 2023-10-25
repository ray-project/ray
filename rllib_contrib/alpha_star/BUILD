# Examples

py_test(
    name = "example_multi_agent_cartpole_alpha_star",
    main = "multi-agent-cartpole-alpha-star.py",
    tags = ["team:rllib", "example"],
    size = "medium",
    srcs = ["examples/multi-agent-cartpole-alpha-star.py"],
    args = ["--run-as-test"]
)

# Learning Tests

py_test(
    name = "learning_tests_cartpole_alpha_star",
    main = "run_regression_tests.py",
    tags = ["team:rllib", "exclusive", "learning_tests", "learning_tests_cartpole", "learning_tests_discrete"],
    size = "medium",
    srcs = ["run_regression_tests.py"],
    data = ["tuned_examples/multi-agent-cartpole-alpha-star.yaml"],
    args = ["--dir=alpha_star/tuned_examples/", "--num-cpus=10"]
)

# Compilation Tests

py_test(
    name = "test_a2c",
    tags = ["team:rllib", "algorithms_dir"],
    size = "large",
    srcs = ["tests/test_alpha_star.py"]
)
