# Examples

py_test(
    name = "example_qmix_two_step_game",
    main = "qmix_two_step_game.py",
    tags = ["team:rllib", "example"],
    size = "large",
    srcs = ["examples/qmix_two_step_game.py"],
    args = ["--run-as-test"]
)

# Learning Tests

py_test(
    name = "learning_tests_two_step_game_qmix",
    main = "run_regression_tests.py",
    tags = ["team:rllib", "learning_tests", "rllib_contrib", "torch_only"],
    size = "large",
    srcs = ["run_regression_tests.py"],
    data = ["tuned_examples/two-step-game-qmix.yaml"],
    args = ["--dir=qmix/tuned_examples/"]
)

py_test(
    name = "learning_tests_two_step_game_qmix_vdn_mixer",
    main = "run_regression_tests.py",
    tags = ["team:rllib", "learning_tests", "rllib_contrib", "torch_only"],
    size = "large",
    srcs = ["run_regression_tests.py"],
    data = ["tuned_examples/two-step-game-qmix-vdn-mixer.yaml"],
    args = ["--dir=qmix/tuned_examples/"]
)

py_test(
    name = "learning_tests_two_step_game_qmix_no_mixer",
    main = "run_regression_tests.py",
    tags = ["team:rllib", "learning_tests", "rllib_contrib", "torch_only"],
    size = "large",
    srcs = ["run_regression_tests.py"],
    data = ["tuned_examples/two-step-game-qmix-no-mixer.yaml"],
    args = ["--dir=qmix/tuned_examples/"]
)

# Compilation Tests

py_test(
    name = "test_qmix",
    tags = ["team:rllib", "algorithms_dir"],
    size = "large",
    srcs = ["tests/test_qmix.py"]
)
