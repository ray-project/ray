# Examples

py_test(
    name = "example_two_step_game_maddpg",
    size = "large",
    srcs = ["examples/two_step_game.py"],
    args = ["--run-as-test"],
    main = "two_step_game.py",
    tags = [
        "example",
        "team:rllib",
    ],
)

# Learning Tests

py_test(
    name = "learning_tests_two_step_game_maddpg",
    size = "large",  # bazel may complain about it being too long sometimes - large is on purpose as some frameworks take longer
    srcs = ["run_regression_tests.py"],
    args = [
        "--dir=maddpg/tuned_examples/",
        "--framework=tf",
    ],
    data = ["tuned_examples/two-step-game-maddpg.yaml"],
    main = "run_regression_tests.py",
    tags = [
        "learning_tests",
        "rllib_contrib",
        "team:rllib",
        "tf_only",
    ],
)

# Compilation Tests

py_test(
    name = "test_maddpg",
    size = "large",
    srcs = ["tests/test_maddpg.py"],
    tags = [
        "algorithms_dir",
        "team:rllib",
    ],
)
