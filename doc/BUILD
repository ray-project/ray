# --------------------------------------------------------------------
# Tests from the doc directory.
# Please keep these sorted alphabetically, but start with the
# root directory.
# --------------------------------------------------------------------

# Support for Dask has been dropped in 3.6.
py_test(
    name = "dask_xgboost",
    size = "medium",
    main = "source/ray-core/_examples/dask_xgboost/dask_xgboost.py",
    srcs = ["source/ray-core/_examples/dask_xgboost/dask_xgboost.py"],
    tags = ["exclusive", "team:ml", "py37"],
    args = ["--smoke-test", "--address ''", "--num-actors 4",
            "--cpus-per-actor 1", "--num-actors-inference 4",
             "--cpus-per-actor-inference 1"]
)

# Support for Modin has been dropped in 3.6.
py_test(
    name = "modin_xgboost",
    size = "medium",
    main = "source/ray-core/_examples/modin_xgboost/modin_xgboost.py",
    srcs = ["source/ray-core/_examples/modin_xgboost/modin_xgboost.py"],
    tags = ["exclusive", "team:ml", "py37"],
    args = ["--smoke-test", "--address ''", "--num-actors 4",
            "--cpus-per-actor 1", "--num-actors-inference 4",
             "--cpus-per-actor-inference 1"]
)

py_test(
    name = "big_data_ingestion",
    size = "small",
    srcs = ["source/data/_examples/big_data_ingestion.py"],
    tags = ["exclusive", "team:core", "py37"]
)

py_test(
    name = "datasets_train",
    size = "medium",
    srcs = ["source/ray-core/_examples/datasets_train/datasets_train.py"],
    tags = ["exclusive", "team:ml", "py37", "datasets_train"],
    args = ["--smoke-test", "--num-workers=2", "--use-gpu"]
)

py_test(
    name = "plot_hyperparameter",
    size = "small",
    srcs = ["source/ray-core/_examples/plot_hyperparameter.py"],
    tags = ["exclusive", "team:ml"]
)

py_test(
    name = "plot_parameter_server",
    size = "medium",
    srcs = ["source/ray-core/_examples/plot_parameter_server.py"],
    tags = ["exclusive", "team:ml"]
)

py_test(
    name = "plot_pong_example",
    size = "large",
    srcs = ["source/ray-core/_examples/plot_pong_example.py"],
    tags = ["exclusive", "team:ml"]
)

py_test(
    name = "progress_bar",
    size = "small",
    srcs = ["source/ray-core/_examples/progress_bar.py"],
    tags = ["exclusive", "team:ml"]
)


# Directory: source/ray-core/_examples/doc_code
py_test(
    name = "doc_code_tf_example",
    size = "small",
    main = "source/ray-core/_examples/doc_code/tf_example.py",
    srcs = ["source/ray-core/_examples/doc_code/tf_example.py"],
    tags = ["exclusive", "tf", "team:ml"]
)

py_test(
    name = "doc_code_torch_example",
    size = "small",
    main = "source/ray-core/_examples/doc_code/torch_example.py",
    srcs = ["source/ray-core/_examples/doc_code/torch_example.py"],
    tags = ["exclusive", "pytorch", "team:ml"]
)

py_test(
    name = "doc_code_metrics_example",
    size = "small",
    main = "source/ray-core/_examples/doc_code/metrics_example.py",
    srcs = ["source/ray-core/_examples/doc_code/metrics_example.py"],
    tags = ["exclusive", "team:serve"]
)

py_test(
    name = "doc_code_runtime_env_example",
    size = "small",
    main = "source/ray-core/_examples/doc_code/runtime_env_example.py",
    srcs = ["source/ray-core/_examples/doc_code/runtime_env_example.py"],
    tags = ["exclusive", "post_wheel_build", "team:serve"]
)

# --------------------------------------------------------------------
# Tests from the doc/source/tune/_tutorials directory.
# Please keep these sorted alphabetically.
# --------------------------------------------------------------------

py_test(
    name = "tune_sklearn",
    size = "medium",
    main = "source/tune/_tutorials/tune-sklearn.py",
    srcs = ["source/tune/_tutorials/tune-sklearn.py"],
    tags = ["exclusive", "example", "team:ml"],
    args = ["--smoke-test"]
)

py_test(
    name = "tune_serve_integration_mnist",
    size = "medium",
    main = "source/tune/_tutorials/tune-serve-integration-mnist.py",
    srcs = ["source/tune/_tutorials/tune-serve-integration-mnist.py"],
    tags = ["exclusive", "example", "team:ml"],
    args = ["--smoke-test", "--from_scratch", "--day 0"]
)
