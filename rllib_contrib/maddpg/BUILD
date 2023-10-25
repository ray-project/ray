# Examples

py_test(
    name = "example_two_step_game_maddpg",
    main = "two_step_game.py",
    tags = ["team:rllib", "example"],
    size = "large",
    srcs = ["examples/two_step_game.py"],
    args = ["--run-as-test"]
)

# Learning Tests

py_test(
    name = "learning_tests_two_step_game_maddpg",
    main = "run_regression_tests.py",
    tags = ["team:rllib", "learning_tests", "rllib_contrib", "tf_only"],
    size = "large", # bazel may complain about it being too long sometimes - large is on purpose as some frameworks take longer
    srcs = ["run_regression_tests.py"],
    data = ["tuned_examples/two-step-game-maddpg.yaml"],
    args = ["--dir=maddpg/tuned_examples/", "--framework=tf"]
)

# Compilation Tests

py_test(
    name = "test_maddpg",
    tags = ["team:rllib", "algorithms_dir"],
    size = "large",
    srcs = ["tests/test_maddpg.py"]
)
