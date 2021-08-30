# --------------------------------------------------------------------
# Tests from the doc/examples directory.
# Please keep these sorted alphabetically, but start with the
# root directory.
# --------------------------------------------------------------------
py_test(
    name = "plot_hyperparameter",
    size = "small",
    srcs = ["examples/plot_hyperparameter.py"],
    tags = ["exclusive", "team:ml"]
)

py_test(
    name = "plot_parameter_server",
    size = "medium",
    srcs = ["examples/plot_parameter_server.py"],
    tags = ["exclusive", "team:ml"]
)

py_test(
    name = "plot_pong_example",
    size = "large",
    srcs = ["examples/plot_pong_example.py"],
    tags = ["exclusive", "team:ml"]
)

py_test(
    name = "progress_bar",
    size = "small",
    srcs = ["examples/progress_bar.py"],
    tags = ["exclusive", "team:ml"]
)


# Directory: examples/doc_code
py_test(
    name = "doc_code_tf_example",
    size = "small",
    main = "examples/doc_code/tf_example.py",
    srcs = ["examples/doc_code/tf_example.py"],
    tags = ["exclusive", "tf", "team:ml"]
)

py_test(
    name = "doc_code_torch_example",
    size = "small",
    main = "examples/doc_code/torch_example.py",
    srcs = ["examples/doc_code/torch_example.py"],
    tags = ["exclusive", "pytorch", "team:ml"]
)

py_test(
    name = "doc_code_metrics_example",
    size = "small",
    main = "examples/doc_code/metrics_example.py",
    srcs = ["examples/doc_code/metrics_example.py"],
    tags = ["exclusive", "team:serve"]
)

py_test(
    name = "doc_code_runtime_env_example",
    size = "small",
    main = "examples/doc_code/runtime_env_example.py",
    srcs = ["examples/doc_code/runtime_env_example.py"],
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
