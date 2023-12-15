# Examples

py_test(
    name = "example_leela_chess_zero_connect_4",
    main = "leela_chess_zero_connect_4.py",
    tags = ["team:rllib", "example"],
    size = "large",
    srcs = ["examples/leela_chess_zero_connect_4.py"],
    args = ["--run-as-test"]
)

# Learning Tests

py_test(
    name = "learning_tests_multi_agent_chess_leela_chess_zero",
    main = "run_regression_tests.py",
    tags = ["team:rllib", "learning_tests", "rllib_contrib", "torch_only"],
    size = "large",
    srcs = ["run_regression_tests.py"],
    data = ["tuned_examples/multi-agent-chess-leela-chess-zero.py"],
    args = ["--dir=leela_chess_zero/tuned_examples/", "--framework=torch"]
)


# Compilation Tests

py_test(
    name = "test_leela_chess_zero",
    tags = ["team:rllib", "algorithms_dir"],
    size = "large",
    srcs = ["tests/test_leela_chess_zero.py"]
)
