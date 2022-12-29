load("//bazel:python.bzl", "py_test_module_list")

# --------------------------------------------------------------------
# Tests from the python/ray/tune/tests directory.
# Covers all tests starting with `test_`.
# Please keep these sorted alphabetically.
#
# Tags:
# "team:ml": Tag indicating this test is owned by the ML team.
# "client": Test uses the ray client.
# "example": Test runs a tune example script.
# "exclusive": ???
# "py37": Test requires py3.7 or later.
# "soft_imports": Tests checking whether Tune runs without any of its soft dependencies.
# "pytorch": Test uses PyTorch.
# "tensorflow": Test uses TensorFlow.
# --------------------------------------------------------------------
py_test(
    name = "test_actor_reuse",
    size = "medium",
    srcs = ["tests/test_actor_reuse.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":tune_lib"],
)

py_test(
    name = "test_api",
    size = "large",
    srcs = ["tests/test_api.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "rllib"],
)

py_test(
    name = "test_automl_searcher",
    size = "small",
    srcs = ["tests/test_automl_searcher.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive"],
)

py_test(
    name = "test_checkpoint_manager",
    size = "small",
    srcs = ["tests/test_checkpoint_manager.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive"],
)

py_test(
    name = "test_client",
    size = "large",
    srcs = ["tests/test_client.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "client", "py37", "exclusive"]
)

py_test(
    name = "test_cluster",
    size = "large",
    srcs = ["tests/test_cluster.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "rllib"],
)

py_test(
    name = "test_cluster_searcher",
    size = "large",
    srcs = ["tests/test_cluster_searcher.py"],
    data = ["tests/_test_cluster_interrupt_searcher.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive"],
)

py_test(
    name = "test_commands",
    size = "medium",
    srcs = ["tests/test_commands.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive"],
)

py_test(
    name = "test_convergence",
    size = "medium",
    srcs = ["tests/test_convergence.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive"],
)

py_test(
    name = "test_dependency",
    size = "small",
    srcs = ["tests/test_dependency.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive"],
)

py_test(
    name = "test_experiment",
    size = "small",
    srcs = ["tests/test_experiment.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive"],
)

py_test(
    name = "test_experiment_analysis",
    size = "medium",
    srcs = ["tests/test_experiment_analysis.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive"],
)

py_test(
    name = "test_experiment_analysis_mem",
    size = "medium",
    srcs = ["tests/test_experiment_analysis_mem.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive"],
)

py_test(
    name = "test_function_api",
    size = "medium",
    srcs = ["tests/test_function_api.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive"],
)

py_test(
    name = "test_function_api_legacy",
    size = "small",
    srcs = ["tests/test_function_api_legacy.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive"],
)

py_test(
    name = "test_legacy_import",
    size = "medium",
    srcs = ["tests/test_legacy_import.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive"],
)

py_test(
    name = "test_integration_comet",
    size = "small",
    srcs = ["tests/test_integration_comet.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive"],
)

py_test(
    name = "test_integration_pytorch_lightning",
    size = "small",
    srcs = ["tests/test_integration_pytorch_lightning.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive"],
)

py_test(
    name = "test_integration_wandb",
    size = "small",
    srcs = ["tests/test_integration_wandb.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive"],
)

py_test(
    name = "test_integration_mlflow",
    size = "small",
    srcs = ["tests/test_integration_mlflow.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive"]
)

py_test(
    name = "test_logger",
    size = "small",
    srcs = ["tests/test_logger.py"],
    deps = [":tune_lib"],
    tags = ["team:ml"],
)

py_test(
    name = "test_multinode_sync",
    size = "large",
    srcs = ["tests/test_multinode_sync.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "multinode"],
)

py_test(
    name = "test_progress_reporter",
    size = "medium",
    srcs = ["tests/test_progress_reporter.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive"],
)

py_test(
    name = "test_resource_updater",
    size = "small",
    srcs = ["tests/test_resource_updater.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive"],
)

py_test(
    name = "test_ray_trial_executor",
    size = "large",
    srcs = ["tests/test_ray_trial_executor.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive"],
)

py_test(
    name = "test_run_experiment",
    size = "medium",
    srcs = ["tests/test_run_experiment.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive"],
)

py_test(
    name = "test_remote",
    size = "medium",
    srcs = ["tests/test_remote.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive"],
)

py_test(
    name = "test_result_grid",
    size = "medium",
    srcs = ["tests/test_result_grid.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive"],
)

py_test(
    name = "test_warnings",
    size = "medium",
    srcs = ["tests/test_warnings.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive"],
)

py_test(
    name = "test_sample",
    size = "large",
    srcs = ["tests/test_sample.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "medium_instance"],
)

py_test(
    name = "test_searchers",
    size = "large",
    srcs = ["tests/test_searchers.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "medium_instance"],
)

py_test(
    name = "test_soft_imports",
    size = "small",
    srcs = ["tests/test_soft_imports.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "soft_imports"],
)

py_test(
    name = "test_stopper",
    size = "small",
    srcs = ["tests/test_stopper.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive"],
)

py_test(
    name = "test_util_file_transfer",
    size = "medium",
    srcs = ["tests/test_util_file_transfer.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive"],
)

py_test(
    name = "test_syncer",
    size = "medium",
    srcs = ["tests/test_syncer.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive"],
)

py_test(
    name = "test_syncer_callback",
    size = "medium",
    srcs = ["tests/test_syncer_callback.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive"],
)

py_test(
    name = "test_trainable",
    size = "medium",
    srcs = ["tests/test_trainable.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive"],
)

py_test(
    name = "test_trainable_util",
    size = "small",
    srcs = ["tests/test_trainable_util.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive"],
)

py_test(
    name = "test_trial_relative_logdir",
    size = "medium",
    srcs = ["tests/test_trial_relative_logdir.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive"],
)

py_test(
    name = "test_trial_runner",
    size = "large",
    srcs = ["tests/test_trial_runner.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive"],
)

py_test(
    name = "test_trial_runner_2",
    size = "large",
    srcs = ["tests/test_trial_runner_2.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive"],
)

py_test(
    name = "test_trial_runner_3",
    size = "large",
    srcs = ["tests/test_trial_runner_3.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "medium_instance"],
)

py_test(
    name = "test_trial_runner_pg",
    size = "large",
    srcs = ["tests/test_trial_runner_pg.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive"],
)

py_test(
    name = "test_trial_runner_callbacks",
    size = "small",
    srcs = ["tests/test_trial_runner_callbacks.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive"],
)

py_test(
    name = "test_var",
    size = "medium",
    srcs = ["tests/test_var.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive"],
)

py_test(
    name = "test_trial_scheduler",
    size = "large",
    srcs = ["tests/test_trial_scheduler.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "medium_instance"],
)

py_test(
    name = "test_trial_scheduler_pbt",
    size = "large",
    srcs = ["tests/test_trial_scheduler_pbt.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "medium_instance"],
)

py_test(
    name = "test_trial_scheduler_resource_changing",
    size = "small",
    srcs = ["tests/test_trial_scheduler_resource_changing.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive"],
)

py_test(
    name = "test_tune_restore_warm_start",
    size = "large",
    srcs = ["tests/test_tune_restore_warm_start.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive"],
)

py_test(
    name = "test_tune_restore",
    size = "large",
    srcs = ["tests/test_tune_restore.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "rllib"],
)

py_test(
    name = "test_tune_save_restore",
    size = "small",
    srcs = ["tests/test_tune_save_restore.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive"],
)

py_test(
    name = "test_tune_server",
    size = "medium",
    srcs = ["tests/test_tune_server.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive"],
)

py_test(
    name = "test_tuner",
    size = "large",
    srcs = ["tests/test_tuner.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "medium_instance"],
)

py_test(
    name = "test_tuner_restore",
    size = "large",
    srcs = ["tests/test_tuner_restore.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive"],
)

py_test(
    name = "test_utils",
    size = "small",
    srcs = ["tests/test_utils.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive"],
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
    tags = ["team:ml", "exclusive", "example", "no_main"],
)

py_test(
    name = "ext_pytorch",
    size = "large",
    srcs = ["tests/ext_pytorch.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "example"],
)

# Todo: Ensure MPLBACKEND=Agg
py_test(
    name = "tutorial",
    size = "medium",
    srcs = ["tests/tutorial.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "example", "no_main"],
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
    tags = ["team:ml", "exclusive", "example"],
    args = ["--smoke-test"]
)

py_test(
    name = "ax_example",
    size = "small",
    srcs = ["examples/ax_example.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "example", "py37"],
    args = ["--smoke-test"]
)

py_test(
    name = "bayesopt_example",
    size = "medium",
    srcs = ["examples/bayesopt_example.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "example"],
    args = ["--smoke-test"]
)

py_test(
    name = "blendsearch_example",
    size = "medium",
    srcs = ["examples/blendsearch_example.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "example"],
    args = ["--smoke-test"]
)

py_test(
    name = "bohb_example",
    size = "medium",
    srcs = ["examples/bohb_example.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "example"]
)

py_test(
    name = "cfo_example",
    size = "small",
    srcs = ["examples/cfo_example.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "example"],
    args = ["--smoke-test"]
)

py_test(
    name = "cifar10_pytorch",
    size = "medium",
    srcs = ["examples/cifar10_pytorch.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "example", "pytorch"],
    args = ["--smoke-test"]
)

py_test(
    name = "custom_func_checkpointing",
    size = "small",
    srcs = ["examples/custom_func_checkpointing.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "example"],
    args = ["--smoke-test"]
)

py_test(
    name = "dragonfly_example",
    size = "medium",
    srcs = ["examples/dragonfly_example.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "example"],
    args = ["--smoke-test"]
)

py_test(
    name = "genetic_example",
    size = "small",
    srcs = ["examples/genetic_example.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "example"],
    args = ["--smoke-test"]
)

py_test(
    name = "hebo_example",
    size = "medium",
    srcs = ["examples/hebo_example.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "example"],
    args = ["--smoke-test"]
)

py_test(
    name = "hyperband_example",
    size = "medium",
    srcs = ["examples/hyperband_example.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "example"],
    args = ["--smoke-test"]
)

py_test(
    name = "hyperband_function_example",
    size = "small",
    srcs = ["examples/hyperband_function_example.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "example"],
    args = ["--smoke-test"]
)

py_test(
    name = "hyperopt_conditional_search_space_example",
    size = "small",
    srcs = ["examples/hyperopt_conditional_search_space_example.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "example"],
    args = ["--smoke-test"]
)

py_test(
    name = "lightgbm_example",
    size = "small",
    srcs = ["examples/lightgbm_example.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "example"]
)

py_test(
    name = "lightgbm_example_cv",
    size = "small",
    main = "examples/lightgbm_example.py",
    srcs = ["examples/lightgbm_example.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "example"],
    args = ["--use-cv"]
)

py_test(
    name = "logging_example",
    size = "small",
    srcs = ["examples/logging_example.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "example"],
    args = ["--smoke-test"]
)

py_test(
 name = "mlflow_example",
 size = "medium",
 srcs = ["examples/mlflow_example.py"],
 deps = [":tune_lib"],
 tags = ["team:ml", "exclusive", "example"]
)

py_test(
    name = "mlflow_ptl",
    size = "medium",
    srcs = ["examples/mlflow_ptl.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "example", "py37", "pytorch"],
    args = ["--smoke-test"]
)

py_test(
    name = "mnist_pytorch",
    size = "small",
    srcs = ["examples/mnist_pytorch.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "example", "pytorch"],
    args = ["--smoke-test"]
)

py_test(
    name = "mnist_pytorch_lightning",
    size = "medium",
    srcs = ["examples/mnist_pytorch_lightning.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "example", "pytorch"],
    args = ["--smoke-test"]
)

py_test(
    name = "mnist_ptl_mini",
    size = "medium",
    srcs = ["examples/mnist_ptl_mini.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "example", "pytorch"],
    args = ["--smoke-test"]
)

py_test(
    name = "mnist_pytorch_trainable",
    size = "small",
    srcs = ["examples/mnist_pytorch_trainable.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "example", "pytorch"],
    args = ["--smoke-test"]
)

py_test(
    name = "mxnet_example",
    size = "small",
    srcs = ["examples/mxnet_example.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "example"],
    args = ["--smoke-test"]
)

py_test(
    name = "nevergrad_example",
    size = "small",
    srcs = ["examples/nevergrad_example.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "example"],
    args = ["--smoke-test"]
)

py_test(
    name = "optuna_define_by_run_example",
    size = "small",
    srcs = ["examples/optuna_define_by_run_example.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "example"],
    args = ["--smoke-test"]
)

py_test(
    name = "optuna_example",
    size = "small",
    srcs = ["examples/optuna_example.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "example"],
    args = ["--smoke-test"]
)

py_test(
    name = "optuna_multiobjective_example",
    size = "medium",
    srcs = ["examples/optuna_multiobjective_example.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "example", "medium_instance"],
    args = ["--smoke-test"]
)

py_test(
    name = "pb2_example",
    size = "small",
    srcs = ["examples/pb2_example.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "example"],
    args = ["--smoke-test"]
)

py_test(
    name = "pbt_convnet_example",
    size = "small",
    srcs = ["examples/pbt_convnet_example.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "example"],
    args = ["--smoke-test"]
)

py_test(
    name = "pbt_convnet_function_example",
    size = "small",
    srcs = ["examples/pbt_convnet_function_example.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "example"],
    args = ["--smoke-test"]
)

py_test(
    name = "pbt_dcgan_mnist_func",
    size = "medium",
    srcs = ["examples/pbt_dcgan_mnist/pbt_dcgan_mnist_func.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "example"],
    args = ["--smoke-test"]
)

py_test(
    name = "pbt_dcgan_mnist_trainable",
    size = "medium",
    srcs = ["examples/pbt_dcgan_mnist/pbt_dcgan_mnist_trainable.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "example"],
    args = ["--smoke-test"]
)

py_test(
    name = "pbt_example",
    size = "small",
    srcs = ["examples/pbt_example.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "example"],
    args = ["--smoke-test"]
)

py_test(
    name = "pbt_function",
    size = "small",
    srcs = ["examples/pbt_function.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "example"],
    args = ["--smoke-test"]
)

py_test(
    name = "pbt_memnn_example",
    size = "small",
    srcs = ["examples/pbt_memnn_example.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "example"],
    args = ["--smoke-test"]
)

# Requires GPUs. Add smoke test?
# py_test(
#     name = "pbt_ppo_example",
#     size = "medium",
#     srcs = ["examples/pbt_ppo_example.py"],
#     deps = [":tune_lib"],
#     tags = ["team:ml", "exclusive", "example"],
#     args = ["--smoke-test"]
# )

py_test(
    name = "pbt_transformers",
    size = "small",
    srcs = ["examples/pbt_transformers/pbt_transformers.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "example"],
    args = ["--smoke-test"]
)


# Requires GPUs. Add smoke test?
# py_test(
#     name = "pbt_tune_cifar10_with_keras",
#     size = "medium",
#     srcs = ["examples/pbt_tune_cifar10_with_keras.py"],
#     deps = [":tune_lib"],
#     tags = ["team:ml", "exclusive", "example"],
#     args = ["--smoke-test"]
# )

# Needs SigOpt API key.
py_test(
    name = "sigopt_example",
    size = "medium",
    srcs = ["examples/sigopt_example.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "example"],
    args = ["--smoke-test"]
)


# Needs SigOpt API key.
py_test(
    name = "sigopt_multi_objective_example",
    size = "medium",
    srcs = ["examples/sigopt_multi_objective_example.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "example"],
    args = ["--smoke-test"]
)

# Needs SigOpt API key.
py_test(
    name = "sigopt_prior_beliefs_example",
    size = "medium",
    srcs = ["examples/sigopt_prior_beliefs_example.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "example"],
    args = ["--smoke-test"]
)

py_test(
    name = "skopt_example",
    size = "medium",
    srcs = ["examples/skopt_example.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "example"],
    args = ["--smoke-test"]
)

py_test(
    name = "tf_mnist_example",
    size = "medium",
    srcs = ["examples/tf_mnist_example.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "example", "tf"],
    args = ["--smoke-test"]
)

py_test(
    name = "tune_basic_example",
    size = "small",
    srcs = ["examples/tune_basic_example.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "example"],
    args = ["--smoke-test"]
)

# Downloads too much data.
# py_test(
#     name = "tune_cifar10_gluon",
#     size = "medium",
#     srcs = ["examples/tune_cifar10_gluon.py"],
#     deps = [":tune_lib"],
#     tags = ["team:ml", "exclusive", "example"],
#     args = ["--model SqueezeNet1.0", "--smoke-test"]
# )

py_test(
    name = "tune_mnist_keras",
    size = "medium",
    srcs = ["examples/tune_mnist_keras.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "example"],
    args = ["--smoke-test"]
)

py_test(
    name = "wandb_example",
    size = "medium",
    srcs = ["examples/wandb_example.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "example"],
    args = ["--mock-api"]
)

py_test(
    name = "xgboost_example",
    size = "small",
    srcs = ["examples/xgboost_example.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "example"]
)

py_test(
    name = "xgboost_example_cv",
    size = "small",
    main = "examples/xgboost_example.py",
    srcs = ["examples/xgboost_example.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "example"],
    args = ["--use-cv"]
)

py_test(
    name = "xgboost_dynamic_resources_example",
    size = "large",
    srcs = ["examples/xgboost_dynamic_resources_example.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "example"],
    args = ["--test"]
)

py_test(
    name = "zoopt_example",
    size = "small",
    srcs = ["examples/zoopt_example.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "example"],
    args = ["--smoke-test"]
)

# This is a dummy test dependency that causes the above tests to be
# re-run if any of these files changes.
py_library(
    name = "tune_lib",
    srcs = glob(["**/*.py"], exclude=["tests/*.py"]),
)
