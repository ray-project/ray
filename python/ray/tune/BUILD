# --------------------------------------------------------------------
# Tests from the python/ray/tune/tests directory.
# Covers all tests starting with `test_`.
# Please keep these sorted alphabetically.
# --------------------------------------------------------------------
py_test(
    name = "test_actor_reuse",
    size = "medium",
    srcs = ["tests/test_actor_reuse.py"],
    tags = ["jenkins_only"],
    deps = [":tune_lib"],
)

py_test(
    name = "test_api",
    size = "large",
    srcs = ["tests/test_api.py"],
    deps = [":tune_lib"],
    tags = ["exclusive"],
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
    tags = ["flaky", "jenkins_only", "exclusive"],
)

py_test(
    name = "test_commands",
    size = "small",
    srcs = ["tests/test_commands.py"],
    deps = [":tune_lib"],
    tags = ["exclusive"],
)

py_test(
    name = "test_convergence_gaussian_process",
    size = "small",
    srcs = ["tests/test_convergence_gaussian_process.py"],
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
    name = "test_experiment",
    size = "small",
    srcs = ["tests/test_experiment.py"],
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
    name = "test_function_api",
    size = "medium",
    srcs = ["tests/test_function_api.py"],
    deps = [":tune_lib"],
    tags = ["exclusive"],
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
    name = "test_run_experiment",
    size = "medium",
    srcs = ["tests/test_run_experiment.py"],
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
    name = "test_track",
    size = "small",
    srcs = ["tests/test_track.py"],
    deps = [":tune_lib"],
)

py_test(
    name = "test_trainable_util",
    size = "small",
    srcs = ["tests/test_trainable_util.py"],
    deps = [":tune_lib"],
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
    name = "test_trial_scheduler",
    size = "medium",
    srcs = ["tests/test_trial_scheduler.py"],
    deps = [":tune_lib"],
    tags = ["exclusive"],
)

py_test(
    name = "test_trial_scheduler_pbt",
    size = "medium",
    srcs = ["tests/test_trial_scheduler_pbt.py"],
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
    size = "medium",
    srcs = ["tests/test_tune_server.py"],
    deps = [":tune_lib"],
    tags = ["exclusive"],
)

# --------------------------------------------------------------------
# Tests from the python/ray/tune/tests directory.
# Covers all remaining tests that do not start with `test_`.
# Please keep these sorted alphabetically.
# --------------------------------------------------------------------
py_test(
    name = "example",
    size = "small",
    srcs = ["tests/example.py"],
    deps = [":tune_lib"],
    tags = ["exclusive", "example"],
)

# Todo: Ensure MPLBACKEND=Agg
py_test(
    name = "tutorial",
    size = "medium",
    srcs = ["tests/tutorial.py"],
    deps = [":tune_lib"],
    tags = ["exclusive", "example"],
)

# --------------------------------------------------------------------
# Examples from the python/ray/tune/examples directory.
# Please keep these sorted alphabetically.
# --------------------------------------------------------------------
py_test(
    name = "async_hyperband_example",
    size = "small",
    srcs = ["examples/async_hyperband_example.py"],
    deps = [":tune_lib"],
    tags = ["exclusive", "example"],
    args = ["--smoke-test"]
)

py_test(
    name = "ax_example",
    size = "medium",
    srcs = ["examples/ax_example.py"],
    deps = [":tune_lib"],
    tags = ["exclusive", "example", "py37"],
    args = ["--smoke-test"]
)

py_test(
    name = "bayesopt_example",
    size = "medium",
    srcs = ["examples/bayesopt_example.py"],
    deps = [":tune_lib"],
    tags = ["exclusive", "example"],
    args = ["--smoke-test"]
)

py_test(
    name = "bohb_example",
    size = "small",
    srcs = ["examples/bohb_example.py"],
    deps = [":tune_lib"],
    tags = ["exclusive", "example"]
)

py_test(
    name = "cifar10_pytorch",
    size = "medium",
    srcs = ["examples/cifar10_pytorch.py"],
    deps = [":tune_lib"],
    tags = ["exclusive", "example", "pytorch"],
    args = ["--smoke-test"]
)

py_test(
    name = "ddp_mnist_torch",
    size = "small",
    srcs = ["examples/ddp_mnist_torch.py"],
    deps = [":tune_lib"],
    tags = ["exclusive", "example", "pytorch"],
    args = ["--num-workers=2"]
)

py_test(
    name = "dragonfly_example",
    size = "medium",
    srcs = ["examples/dragonfly_example.py"],
    deps = [":tune_lib"],
    tags = ["exclusive", "example"],
    args = ["--smoke-test"]
)

# Does not work without awscli tool installed. Check if can be mocked.
# py_test(
#     name = "durable_trainable_example",
#     size = "medium",
#     srcs = ["examples/durable_trainable_example.py"],
#     deps = [":tune_lib"],
#     tags = ["exclusive", "example"],
#     args = ["--local", "--mock-storage"]
# )

py_test(
    name = "genetic_example",
    size = "small",
    srcs = ["examples/genetic_example.py"],
    deps = [":tune_lib"],
    tags = ["exclusive", "example"],
    args = ["--smoke-test"]
)

py_test(
    name = "hyperband_example",
    size = "medium",
    srcs = ["examples/hyperband_example.py"],
    deps = [":tune_lib"],
    tags = ["exclusive", "example"]
)

py_test(
    name = "hyperband_function_example",
    size = "medium",
    srcs = ["examples/hyperband_function_example.py"],
    deps = [":tune_lib"],
    tags = ["exclusive", "example"],
    args = ["--smoke-test"]
)

py_test(
    name = "hyperopt_example",
    size = "medium",
    srcs = ["examples/hyperopt_example.py"],
    deps = [":tune_lib"],
    tags = ["exclusive", "example"],
    args = ["--smoke-test"]
)

py_test(
    name = "lightgbm_example",
    size = "medium",
    srcs = ["examples/lightgbm_example.py"],
    deps = [":tune_lib"],
    tags = ["exclusive", "example"]
)

py_test(
    name = "logging_example",
    size = "medium",
    srcs = ["examples/logging_example.py"],
    deps = [":tune_lib"],
    tags = ["exclusive", "example"],
    args = ["--smoke-test"]
)

# Commenting out for now because it is not idempotent
# py_test(
#     name = "mlflow_example",
#     size = "medium",
#     srcs = ["examples/mlflow_example.py"],
#     deps = [":tune_lib"],
#     tags = ["exclusive", "example"]
# )

py_test(
    name = "mnist_pytorch",
    size = "small",
    srcs = ["examples/mnist_pytorch.py"],
    deps = [":tune_lib"],
    tags = ["exclusive", "example", "pytorch"],
    args = ["--smoke-test"]
)

py_test(
    name = "mnist_pytorch_lightning",
    size = "medium",
    srcs = ["examples/mnist_pytorch_lightning.py"],
    deps = [":tune_lib"],
    tags = ["exclusive", "example", "pytorch"],
    args = ["--smoke-test"]
)

py_test(
    name = "mnist_pytorch_trainable",
    size = "small",
    srcs = ["examples/mnist_pytorch_trainable.py"],
    deps = [":tune_lib"],
    tags = ["exclusive", "example", "pytorch"],
    args = ["--smoke-test"]
)

py_test(
    name = "nevergrad_example",
    size = "medium",
    srcs = ["examples/nevergrad_example.py"],
    deps = [":tune_lib"],
    tags = ["exclusive", "example"],
    args = ["--smoke-test"]
)

py_test(
    name = "pbt_convnet_example",
    size = "medium",
    srcs = ["examples/pbt_convnet_example.py"],
    deps = [":tune_lib"],
    tags = ["exclusive", "example"],
    args = ["--smoke-test"]
)

py_test(
    name = "pbt_example",
    size = "medium",
    srcs = ["examples/pbt_example.py"],
    deps = [":tune_lib"],
    tags = ["exclusive", "example"],
    args = ["--smoke-test"]
)

py_test(
    name = "pbt_function",
    size = "medium",
    srcs = ["examples/pbt_function.py"],
    deps = [":tune_lib"],
    tags = ["exclusive", "example"],
    args = ["--smoke-test"]
)

py_test(
    name = "pbt_memnn_example",
    size = "medium",
    srcs = ["examples/pbt_memnn_example.py"],
    deps = [":tune_lib"],
    tags = ["flaky", "exclusive", "example"],
    args = ["--smoke-test"]
)

# Requires GPUs. Add smoke test?
# py_test(
#     name = "pbt_ppo_example",
#     size = "medium",
#     srcs = ["examples/pbt_ppo_example.py"],
#     deps = [":tune_lib"],
#     tags = ["exclusive", "example"],
#     args = ["--smoke-test"]
# )

# Requires GPUs. Add smoke test?
# py_test(
#     name = "pbt_tune_cifar10_with_keras",
#     size = "medium",
#     srcs = ["examples/pbt_tune_cifar10_with_keras.py"],
#     deps = [":tune_lib"],
#     tags = ["exclusive", "example"],
#     args = ["--smoke-test"]
# )

# Needs SigOpt API key.
# py_test(
#     name = "sigopt_example",
#     size = "medium",
#     srcs = ["examples/sigopt_example.py"],
#     deps = [":tune_lib"],
#     tags = ["exclusive", "example"],
#     args = ["--smoke-test"]
# )

py_test(
    name = "skopt_example",
    size = "medium",
    srcs = ["examples/skopt_example.py"],
    deps = [":tune_lib"],
    tags = ["exclusive", "example"],
    args = ["--smoke-test"]
)

py_test(
    name = "tf_mnist_example",
    size = "medium",
    srcs = ["examples/tf_mnist_example.py"],
    deps = [":tune_lib"],
    tags = ["exclusive", "example", "tf"],
    args = ["--smoke-test"]
)

# Downloads too much data.
# py_test(
#     name = "tune_cifar10_gluon",
#     size = "medium",
#     srcs = ["examples/tune_cifar10_gluon.py"],
#     deps = [":tune_lib"],
#     tags = ["exclusive", "example"],
#     args = ["--model SqueezeNet1.0", "--smoke-test"]
# )

py_test(
    name = "tune_mnist_keras",
    size = "medium",
    srcs = ["examples/tune_mnist_keras.py"],
    deps = [":tune_lib"],
    tags = ["exclusive", "example"],
    args = ["--smoke-test"]
)

py_test(
    name = "xgboost_example",
    size = "small",
    srcs = ["examples/xgboost_example.py"],
    deps = [":tune_lib"],
    tags = ["exclusive", "example"]
)

py_test(
    name = "zoopt_example",
    size = "small",
    srcs = ["examples/zoopt_example.py"],
    deps = [":tune_lib"],
    tags = ["exclusive", "example"],
    args = ["--smoke-test"]
)

# This is a dummy test dependency that causes the above tests to be
# re-run if any of these files changes.
py_library(
    name = "tune_lib",
    srcs = glob(["**/*.py"], exclude=["tests/*.py"]),
)
