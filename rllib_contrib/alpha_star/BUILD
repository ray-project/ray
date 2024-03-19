# Examples

py_test(
    name = "example_multi_agent_cartpole_alpha_star",
    size = "large",
    srcs = ["examples/multi-agent-cartpole-alpha-star.py"],
    args = ["--run-as-test"],
    main = "multi-agent-cartpole-alpha-star.py",
    tags = [
        "example",
        "team:rllib",
    ],
)

# Learning Tests

py_test(
    name = "learning_tests_cartpole_alpha_star",
    size = "medium",
    srcs = ["run_regression_tests.py"],
    args = [
        "--dir=alpha_star/tuned_examples/",
        "--num-cpus=10",
    ],
    data = ["tuned_examples/multi-agent-cartpole-alpha-star.yaml"],
    main = "run_regression_tests.py",
    tags = [
        "learning_tests",
        "rllib_contrib",
        "team:rllib",
    ],
)

# Compilation Tests

py_test(
    name = "test_alpha_star",
    size = "large",
    srcs = ["tests/test_alpha_star.py"],
    tags = [
        "algorithms_dir",
        "team:rllib",
    ],
)
