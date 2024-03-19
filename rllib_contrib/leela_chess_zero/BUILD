# Examples

py_test(
    name = "example_leela_chess_zero_connect_4",
    size = "large",
    srcs = ["examples/leela_chess_zero_connect_4.py"],
    args = ["--run-as-test"],
    main = "leela_chess_zero_connect_4.py",
    tags = [
        "example",
        "team:rllib",
    ],
)

# Learning Tests

py_test(
    name = "learning_tests_multi_agent_chess_leela_chess_zero",
    size = "large",
    srcs = ["run_regression_tests.py"],
    args = [
        "--dir=leela_chess_zero/tuned_examples/",
        "--framework=torch",
    ],
    data = ["tuned_examples/multi-agent-chess-leela-chess-zero.py"],
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
    name = "test_leela_chess_zero",
    size = "large",
    srcs = ["tests/test_leela_chess_zero.py"],
    tags = [
        "algorithms_dir",
        "team:rllib",
    ],
)
