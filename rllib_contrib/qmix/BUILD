# Examples

py_test(
    name = "example_qmix_two_step_game",
    size = "large",
    srcs = ["examples/qmix_two_step_game.py"],
    args = ["--run-as-test"],
    main = "qmix_two_step_game.py",
    tags = [
        "example",
        "team:rllib",
    ],
)

# Learning Tests

py_test(
    name = "learning_tests_two_step_game_qmix",
    size = "large",
    srcs = ["run_regression_tests.py"],
    args = ["--dir=qmix/tuned_examples/"],
    data = ["tuned_examples/two-step-game-qmix.yaml"],
    main = "run_regression_tests.py",
    tags = [
        "learning_tests",
        "rllib_contrib",
        "team:rllib",
        "torch_only",
    ],
)

py_test(
    name = "learning_tests_two_step_game_qmix_vdn_mixer",
    size = "large",
    srcs = ["run_regression_tests.py"],
    args = ["--dir=qmix/tuned_examples/"],
    data = ["tuned_examples/two-step-game-qmix-vdn-mixer.yaml"],
    main = "run_regression_tests.py",
    tags = [
        "learning_tests",
        "rllib_contrib",
        "team:rllib",
        "torch_only",
    ],
)

py_test(
    name = "learning_tests_two_step_game_qmix_no_mixer",
    size = "large",
    srcs = ["run_regression_tests.py"],
    args = ["--dir=qmix/tuned_examples/"],
    data = ["tuned_examples/two-step-game-qmix-no-mixer.yaml"],
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
    name = "test_qmix",
    size = "large",
    srcs = ["tests/test_qmix.py"],
    tags = [
        "algorithms_dir",
        "team:rllib",
    ],
)
