# Examples

# Learning Tests

# py_test(
#    name = "learning_tests_interest_evolution_10_candidates_recsim_env_slate_q",
#    main = "run_regression_tests.py",
#    tags = ["team:rllib", "learning_tests", "rllib_contrib"],
#    size = "large",
#    srcs = ["run_regression_tests.py"],
#    data = ["tuned_examples/interest-evolution-10-candidates-recsim-env-slate-q.yaml"],
#    args = ["--dir=slate_q/tuned_examples/"]
# )

py_test(
    name = "learning_tests_interest_evolution_10_candidates_recsim_env_slate_q_fake_gpus",
    main = "run_regression_tests.py",
    tags = ["team:rllib", "learning_tests", "rllib_contrib", "no_tf_eager_tracing"],
    size = "large",
    srcs = ["run_regression_tests.py"],
    data = ["tuned_examples/interest-evolution-10-candidates-recsim-env-slate-q.yaml"],
    args = ["--dir=slate_q/tuned_examples/"]
)

# Compilation Tests

py_test(
    name = "test_slate_q",
    tags = ["team:rllib", "algorithms_dir"],
    size = "large",
    srcs = ["tests/test_slate_q.py"]
)
