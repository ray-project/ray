py_test(
    name = "test_actor_reuse",
    size = "medium",
    srcs = ["tests/test_actor_reuse.py"],
    tags = ["jenkins_only"],
    deps = [":tune_lib"],
)

py_test(
    name = "test_automl_searcher",
    size = "small",
    srcs = ["tests/test_automl_searcher.py"],
    deps = [":tune_lib"],
)

py_test(
    name = "test_checkpoint_manager",
    size = "small",
    srcs = ["tests/test_checkpoint_manager.py"],
    deps = [":tune_lib"],
)

py_test(
    name = "test_cluster",
    size = "large",
    srcs = ["tests/test_cluster.py"],
    deps = [":tune_lib"],
    tags = ["jenkins_only", "exclusive"],
)

py_test(
    name = "test_commands",
    size = "small",
    srcs = ["tests/test_commands.py"],
    deps = [":tune_lib"],
    tags = ["exclusive"],
)

py_test(
    name = "test_dependency",
    size = "small",
    srcs = ["tests/test_dependency.py"],
    deps = [":tune_lib"],
)

py_test(
    name = "test_experiment_analysis",
    size = "medium",
    srcs = ["tests/test_experiment_analysis.py"],
    deps = [":tune_lib"],
)

py_test(
    name = "test_experiment_analysis_mem",
    size = "small",
    srcs = ["tests/test_experiment_analysis_mem.py"],
    deps = [":tune_lib"],
)

py_test(
    name = "test_experiment",
    size = "small",
    srcs = ["tests/test_experiment.py"],
    deps = [":tune_lib"],
)

py_test(
    name = "test_logger",
    size = "small",
    srcs = ["tests/test_logger.py"],
    deps = [":tune_lib"],
    tags = ["jenkins_only"],
)

py_test(
    name = "test_progress_reporter",
    size = "medium",
    srcs = ["tests/test_progress_reporter.py"],
    deps = [":tune_lib"],
)

py_test(
    name = "test_ray_trial_executor",
    size = "medium",
    srcs = ["tests/test_ray_trial_executor.py"],
    deps = [":tune_lib"],
)

py_test(
    name = "test_track",
    size = "small",
    srcs = ["tests/test_track.py"],
    deps = [":tune_lib"],
)

py_test(
    name = "test_run_experiment",
    size = "medium",
    srcs = ["tests/test_run_experiment.py"],
    deps = [":tune_lib"],
    tags = ["exclusive"],
)

py_test(
    name = "test_trial_runner",
    size = "medium",
    srcs = ["tests/test_trial_runner.py"],
    deps = [":tune_lib"],
    tags = ["exclusive"],
)

py_test(
    name = "test_trial_runner_2",
    size = "medium",
    srcs = ["tests/test_trial_runner_2.py"],
    deps = [":tune_lib"],
    tags = ["exclusive"],
)

py_test(
    name = "test_trial_runner_3",
    size = "medium",
    srcs = ["tests/test_trial_runner_3.py"],
    deps = [":tune_lib"],
    tags = ["exclusive"],
)

py_test(
    name = "test_var",
    size = "small",
    srcs = ["tests/test_var.py"],
    deps = [":tune_lib"],
    tags = ["exclusive"],
)

py_test(
    name = "test_api",
    size = "medium",
    srcs = ["tests/test_api.py"],
    deps = [":tune_lib"],
    tags = ["exclusive"],
)

py_test(
    name = "test_sync",
    size = "medium",
    srcs = ["tests/test_sync.py"],
    deps = [":tune_lib"],
    tags = ["exclusive"],
)

py_test(
    name = "test_trainable_util",
    size = "small",
    srcs = ["tests/test_trainable_util.py"],
    deps = [":tune_lib"],
)

py_test(
    name = "test_trial_scheduler",
    size = "medium",
    srcs = ["tests/test_trial_scheduler.py"],
    deps = [":tune_lib"],
    tags = ["exclusive"],
)

py_test(
    name = "test_tune_restore",
    size = "large",
    srcs = ["tests/test_tune_restore.py"],
    deps = [":tune_lib"],
    tags = ["jenkins_only", "exclusive"],
)

py_test(
    name = "test_tune_save_restore",
    size = "small",
    srcs = ["tests/test_tune_save_restore.py"],
    deps = [":tune_lib"],
    tags = ["exclusive"],
)

py_test(
    name = "test_tune_server",
    size = "small",
    srcs = ["tests/test_tune_server.py"],
    deps = [":tune_lib"],
    tags = ["exclusive"],
)

# This is a dummy test dependency that causes the above tests to be
# re-run if any of these files changes.
py_library(
    name = "tune_lib",
    srcs = glob(["**/*.py"], exclude=["tests/*.py"]),
)
