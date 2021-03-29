# --------------------------------------------------------------------
# Tests from the python/ray/tune/tests directory.
# Covers all tests starting with `test_`.
# Please keep these sorted alphabetically.
# --------------------------------------------------------------------
py_test(
    name = "test_actor_reuse",
    size = "medium",
    srcs = ["tests/test_actor_reuse.py"],
    tags = ["exclusive"],
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
    name = "test_convergence",
    size = "medium",
    srcs = ["tests/test_convergence.py"],
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
    size = "medium",
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
    name = "test_integration_docker",
    size = "small",
    srcs = ["tests/test_integration_docker.py"],
    deps = [":tune_lib"],
    tags = ["exclusive"],
)

py_test(
    name = "test_integration_kubernetes",
    size = "small",
    srcs = ["tests/test_integration_kubernetes.py"],
    deps = [":tune_lib"],
    tags = ["exclusive"],
)

py_test(
    name = "test_integration_pytorch_lightning",
    size = "small",
    srcs = ["tests/test_integration_pytorch_lightning.py"],
    deps = [":tune_lib"],
    tags = ["exclusive"],
)

py_test(
    name = "test_integration_wandb",
    size = "small",
    srcs = ["tests/test_integration_wandb.py"],
    deps = [":tune_lib"],
    tags = ["exclusive"],
)

py_test(
    name = "test_integration_mlflow",
    size = "small",
    srcs = ["tests/test_integration_mlflow.py"],
    deps = [":tune_lib"],
    tags = ["exclusive"]
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
    name = "test_remote",
    size = "medium",
    srcs = ["tests/test_remote.py"],
    deps = [":tune_lib"],
    tags = ["exclusive"],
)

py_test(
    name = "test_sample",
    size = "medium",
    srcs = ["tests/test_sample.py"],
    deps = [":tune_lib"],
    tags = ["exclusive"],
)

py_test(
    name = "test_searchers",
    size = "medium",
    srcs = ["tests/test_searchers.py"],
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
    name = "test_trial_runner_pg",
    size = "medium",
    srcs = ["tests/test_trial_runner_pg.py"],
    deps = [":tune_lib"],
    tags = ["exclusive"],
)

py_test(
    name = "test_trial_runner_callbacks",
    size = "small",
    srcs = ["tests/test_trial_runner_callbacks.py"],
    deps = [":tune_lib"],
    tags = ["exclusive"],
)

py_test(
    name = "test_var",
    size = "medium",
    srcs = ["tests/test_var.py"],
    deps = [":tune_lib"],
    tags = ["exclusive"],
)

py_test(
    name = "test_trial_scheduler",
    size = "large",
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
    tags = ["exclusive"],
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

py_test(
    name = "ext_pytorch",
    size = "large",
    srcs = ["tests/ext_pytorch.py"],
    deps = [":tune_lib"],
    tags = ["exclusive", "example"],
)

# Todo: Ensure MPLBACKEND=Agg
# py_test(
#     name = "tutorial",
#     size = "medium",
#     srcs = ["tests/tutorial.py"],
#     deps = [":tune_lib"],
#     tags = ["exclusive", "example"],
# )

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
    name = "test_torch_trainable",
    size = "medium",
    srcs = ["tests/test_torch_trainable.py"],
    tags = ["exclusive", "example", "pytorch"],
    deps = [":tune_lib"],
)

py_test(
    name = "test_tensorflow_trainable",
    size = "medium",
    srcs = ["tests/test_tensorflow_trainable.py"],
    tags = ["exclusive", "example", "tensorflow"],
    deps = [":tune_lib"],
)

py_test(
    name = "test_horovod",
    size = "medium",
    srcs = ["tests/test_horovod.py"],
    tags = ["exclusive", "example", "py37"],
    deps = [":tune_lib"],
)

# py_test(
#     name = "ddp_mnist_torch",
#     size = "small",
#     srcs = ["examples/ddp_mnist_torch.py"],
#     deps = [":tune_lib"],
#     tags = ["exclusive", "example", "pytorch"],
#     args = ["--num-workers=2"]
# )

py_test(
    name = "tf_distributed_keras_example",
    size = "small",
    srcs = ["examples/tf_distributed_keras_example.py"],
    deps = [":tune_lib"],
    tags = ["exclusive", "example", "tensorflow"],
    args = ["--smoke-test"]
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
    name = "hebo_example",
    size = "medium",
    srcs = ["examples/hebo_example.py"],
    deps = [":tune_lib"],
    tags = ["exclusive", "example"],
    args = ["--smoke-test"]
)

py_test(
    name = "horovod_simple",
    size = "medium",
    srcs = ["examples/horovod_simple.py"],
    tags = ["exclusive", "example", "py37"],
    deps = [":tune_lib"],
    args = ["--smoke-test"]
)

py_test(
    name = "hyperband_example",
    size = "medium",
    srcs = ["examples/hyperband_example.py"],
    deps = [":tune_lib"],
    tags = ["exclusive", "example"],
    args = ["--smoke-test"]
)

py_test(
    name = "hyperband_function_example",
    size = "small",
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
    size = "small",
    srcs = ["examples/lightgbm_example.py"],
    deps = [":tune_lib"],
    tags = ["exclusive", "example"]
)

py_test(
    name = "logging_example",
    size = "small",
    srcs = ["examples/logging_example.py"],
    deps = [":tune_lib"],
    tags = ["exclusive", "example"],
    args = ["--smoke-test"]
)

py_test(
 name = "mlflow_example",
 size = "medium",
 srcs = ["examples/mlflow_example.py"],
 deps = [":tune_lib"],
 tags = ["exclusive", "example"]
)

# py_test(
#     name = "mlflow_ptl",
#     size = "medium",
#     srcs = ["examples/mlflow_ptl.py"],
#     deps = [":tune_lib"],
#     tags = ["exclusive", "example", "py37", "pytorch"],
#     args = ["--smoke-test"]
# )

# py_test(
#     name = "mnist_pytorch",
#     size = "small",
#     srcs = ["examples/mnist_pytorch.py"],
#     deps = [":tune_lib"],
#     tags = ["exclusive", "example", "pytorch"],
#     args = ["--smoke-test"]
# )

# py_test(
#     name = "mnist_pytorch_lightning",
#     size = "medium",
#     srcs = ["examples/mnist_pytorch_lightning.py"],
#     deps = [":tune_lib"],
#     tags = ["exclusive", "example", "pytorch"],
#     args = ["--smoke-test"]
# )

# py_test(
#     name = "mnist_ptl_mini",
#     size = "medium",
#     srcs = ["examples/mnist_ptl_mini.py"],
#     deps = [":tune_lib"],
#     tags = ["exclusive", "example", "pytorch"],
#     args = ["--smoke-test"]
# )

# py_test(
#     name = "mnist_pytorch_trainable",
#     size = "small",
#     srcs = ["examples/mnist_pytorch_trainable.py"],
#     deps = [":tune_lib"],
#     tags = ["exclusive", "example", "pytorch"],
#     args = ["--smoke-test"]
# )

py_test(
    name = "mxnet_example",
    size = "small",
    srcs = ["examples/mxnet_example.py"],
    deps = [":tune_lib"],
    tags = ["exclusive", "example"],
    args = ["--smoke-test"]
)

py_test(
    name = "nevergrad_example",
    size = "small",
    srcs = ["examples/nevergrad_example.py"],
    deps = [":tune_lib"],
    tags = ["exclusive", "example"],
    args = ["--smoke-test"]
)

py_test(
    name = "optuna_example",
    size = "small",
    srcs = ["examples/optuna_example.py"],
    deps = [":tune_lib"],
    tags = ["exclusive", "example"],
    args = ["--smoke-test"]
)

py_test(
    name = "pb2_example",
    size = "medium",
    srcs = ["examples/pb2_example.py"],
    deps = [":tune_lib"],
    tags = ["exclusive", "example"],
    args = ["--smoke-test"]
)

# py_test(
#     name = "pbt_convnet_example",
#     size = "small",
#     srcs = ["examples/pbt_convnet_example.py"],
#     deps = [":tune_lib"],
#     tags = ["exclusive", "example"],
#     args = ["--smoke-test"]
# )

# py_test(
#     name = "pbt_convnet_function_example",
#     size = "small",
#     srcs = ["examples/pbt_convnet_function_example.py"],
#     deps = [":tune_lib"],
#     tags = ["exclusive", "example"],
#     args = ["--smoke-test"]
# )

# py_test(
#     name = "pbt_dcgan_mnist_func",
#     size = "medium",
#     srcs = ["examples/pbt_dcgan_mnist/pbt_dcgan_mnist_func.py"],
#     deps = [":tune_lib"],
#     tags = ["exclusive", "example"],
#     args = ["--smoke-test"]
# )

# py_test(
#     name = "pbt_dcgan_mnist_trainable",
#     size = "medium",
#     srcs = ["examples/pbt_dcgan_mnist/pbt_dcgan_mnist_trainable.py"],
#     deps = [":tune_lib"],
#     tags = ["exclusive", "example"],
#     args = ["--smoke-test"]
# )

py_test(
    name = "pbt_example",
    size = "small",
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

py_test(
    name = "pbt_transformers",
    size = "large",
    srcs = ["examples/pbt_transformers/pbt_transformers.py"],
    deps = [":tune_lib"],
    tags = ["flaky", "exclusive", "example"],
    args = ["--smoke-test"]
)


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
py_test(
    name = "sigopt_example",
    size = "medium",
    srcs = ["examples/sigopt_example.py"],
    deps = [":tune_lib"],
    tags = ["exclusive", "example"],
    args = ["--smoke-test"]
)


# Needs SigOpt API key.
py_test(
    name = "sigopt_multi_objective_example",
    size = "medium",
    srcs = ["examples/sigopt_multi_objective_example.py"],
    deps = [":tune_lib"],
    tags = ["exclusive", "example"],
    args = ["--smoke-test"]
)

# Needs SigOpt API key.
py_test(
    name = "sigopt_prior_beliefs_example",
    size = "medium",
    srcs = ["examples/sigopt_prior_beliefs_example.py"],
    deps = [":tune_lib"],
    tags = ["exclusive", "example"],
    args = ["--smoke-test"]
)

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

py_test(
    name = "tune_basic_example",
    size = "small",
    srcs = ["examples/tune_basic_example.py"],
    deps = [":tune_lib"],
    tags = ["exclusive", "example"],
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
    name = "wandb_example",
    size = "small",
    srcs = ["examples/wandb_example.py"],
    deps = [":tune_lib"],
    tags = ["exclusive", "example"],
    args = ["--mock-api"]
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
