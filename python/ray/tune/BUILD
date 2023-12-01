load("//bazel:python.bzl", "py_test_module_list")
load("//bazel:python.bzl", "doctest")

doctest(
    files = glob(
        ["**/*.py"],
        exclude=[
            "**/examples/**",
            "**/tests/**",
            "suggest/**",
            "impl/test_utils.py",
            # Deprecated
            "automl/**",
            "cluster_info.py",
            "config_parser.py",
            "function_runner.py",
            "insufficient_resources_manager.py",
            "sample.py",
            "session.py",
            "trial.py",
            "trial_runner.py",
            "utils/placement_groups.py",
            "utils/trainable.py",
        ]
    ),
    tags = ["team:ml"]
)

py_library(
    name = "conftest",
    srcs = ["tests/conftest.py"]
)

# --------------------------------------------------------------------
# Tests from the python/ray/tune/tests directory.
# Covers all tests starting with `test_`.
# Please keep these sorted alphabetically.
#
# Tags:
# "team:ml": Tag indicating this test is owned by the ML team.
# "example": Test runs a tune example script.
# "exclusive": ???
# "soft_imports": Tests checking whether Tune runs without any of its soft dependencies.
# "pytorch": Test uses PyTorch.
# "tensorflow": Test uses TensorFlow.
# --------------------------------------------------------------------
py_test(
    name = "test_actor_reuse",
    size = "large",
    srcs = ["tests/test_actor_reuse.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":tune_lib", ":conftest"],
)

py_test(
    name = "test_api",
    size = "large",
    srcs = ["tests/test_api.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "rllib"],
)

py_test(
    name = "test_api_checkpoint_integration",
    size = "medium",
    srcs = ["tests/test_api_checkpoint_integration.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive"],
)

py_test(
    name = "test_callbacks",
    size = "small",
    srcs = ["tests/test_callbacks.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive"],
)

py_test(
    name = "test_cluster",
    size = "large",
    srcs = ["tests/test_cluster.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "rllib"],
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
    size = "small",
    srcs = ["tests/test_experiment_analysis.py"],
    deps = [":tune_lib", ":conftest"],
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
    name = "test_integration_pytorch_lightning",
    size = "small",
    srcs = ["tests/test_integration_pytorch_lightning.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive"],
)

py_test(
    name = "test_logger",
    size = "small",
    srcs = ["tests/test_logger.py"],
    deps = [":tune_lib"],
    tags = ["team:ml"],
)

py_test(
    name = "test_multi_tenancy",
    size = "medium",
    srcs = [
        "tests/test_multi_tenancy.py",
        "tests/_test_multi_tenancy_run.py"
    ],
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
    deps = [":tune_lib", ":conftest"],
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
    name = "test_placeholder",
    size = "small",
    srcs = ["tests/test_placeholder.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive"],
)

py_test(
    name = "test_searcher_utils",
    size = "small",
    srcs = ["tests/test_searcher_utils.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive"],
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
    name = "test_util_object_cache",
    size = "small",
    srcs = ["tests/test_util_object_cache.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive"],
)

py_test(
    name = "test_syncer",
    size = "medium",
    srcs = ["tests/test_syncer.py"],
    deps = [":tune_lib", ":conftest"],
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
    name = "test_trial",
    size = "small",
    srcs = ["tests/test_trial.py"],
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
    deps = [":tune_lib", ":conftest"],
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

# Todo: Ensure MPLBACKEND=Agg
py_test(
    name = "tutorial",
    size = "medium",
    srcs = ["tests/tutorial.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "example", "no_main"],
)


# --------------------------------------------------------------------
# Tests from the python/ray/tune/tests/execution directory.
# Covers all remaining tests that do not start with `test_`.
# Please keep these sorted alphabetically.
# --------------------------------------------------------------------

py_test(
    name = "test_actor_caching",
    size = "small",
    srcs = ["tests/execution/test_actor_caching.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive"]
)

py_test(
    name = "test_controller_callback_integration",
    size = "large",
    srcs = ["tests/execution/test_controller_callback_integration.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive"]
)

py_test(
    name = "test_controller_checkpointing_integration",
    size = "large",
    srcs = ["tests/execution/test_controller_checkpointing_integration.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive"]
)

py_test(
    name = "test_controller_control_integration",
    size = "large",
    srcs = ["tests/execution/test_controller_control_integration.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive"]
)

py_test(
    name = "test_controller_errors_integration",
    size = "large",
    srcs = ["tests/execution/test_controller_errors_integration.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive"]
)

py_test(
    name = "test_controller_resources_integration",
    size = "large",
    srcs = ["tests/execution/test_controller_resources_integration.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive"]
)

py_test(
    name = "test_controller_search_alg_integration",
    size = "large",
    srcs = ["tests/execution/test_controller_search_alg_integration.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive"]
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
    tags = ["team:ml", "exclusive", "example"],
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
    name = "bohb_example",
    size = "medium",
    srcs = ["examples/bohb_example.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "example"]
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
    tags = ["team:ml", "exclusive", "example", "pytorch"],
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

# pbt_transformers relies on legacy Tune APIs.
# py_test(
#     name = "pbt_transformers",
#     size = "small",
#     srcs = ["examples/pbt_transformers/pbt_transformers.py"],
#     deps = [":tune_lib"],
#     tags = ["team:ml", "exclusive", "example"],
#     args = ["--smoke-test"]
# )


# Requires GPUs. Add smoke test?
# py_test(
#     name = "pbt_tune_cifar10_with_keras",
#     size = "medium",
#     srcs = ["examples/pbt_tune_cifar10_with_keras.py"],
#     deps = [":tune_lib"],
#     tags = ["team:ml", "exclusive", "example"],
#     args = ["--smoke-test"]
# )


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

py_test(
    name = "tune_mnist_keras",
    size = "medium",
    srcs = ["examples/tune_mnist_keras.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive", "example"],
    args = ["--smoke-test"]
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

# --------------------------------------------------------------------
# Tests from the python/ray/tune/tests/output directory.
# Please keep these sorted alphabetically.
# --------------------------------------------------------------------

py_test(
    name = "test_output",
    size = "small",
    srcs = ["tests/output/test_output.py"],
    deps = [":tune_lib"],
    tags = ["team:ml", "exclusive"]
)


# This is a dummy test dependency that causes the above tests to be
# re-run if any of these files changes.
py_library(
    name = "tune_lib",
    srcs = glob(["**/*.py"], exclude=["tests/*.py"]),
)
