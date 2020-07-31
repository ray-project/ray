# This is a dummy test dependency that causes the above tests to be
# re-run if any of these files changes.
py_library(
    name = "serve_lib",
    srcs = glob(["**/*.py"], exclude=["tests/*.py"]),
)

serve_tests_srcs = glob(["tests/*.py"],
     exclude=["tests/test_nonblocking.py",
              "tests/test_controller_crashes.py",
              "tests/test_serve.py",
             ])

py_test(
    name = "test_api",
    size = "medium",
    srcs = serve_tests_srcs,
    tags = ["exclusive"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_backend_worker",
    size = "small",
    srcs = serve_tests_srcs,
    tags = ["exclusive"],
    deps = [":serve_lib"],
)


py_test(
    name = "test_config",
    size = "small",
    srcs = serve_tests_srcs,
    tags = ["exclusive"],
    deps = [":serve_lib"],
)


py_test(
    name = "test_failure",
    size = "medium",
    srcs = serve_tests_srcs,
    tags = ["exclusive"],
    deps = [":serve_lib"],
)


py_test(
    name = "test_handle",
    size = "small",
    srcs = serve_tests_srcs,
    tags = ["exclusive"],
    deps = [":serve_lib"],
)


py_test(
    name = "test_kv_store",
    size = "small",
    srcs = serve_tests_srcs,
    tags = ["exclusive"],
    deps = [":serve_lib"],
)


py_test(
    name = "test_metric",
    size = "small",
    srcs = serve_tests_srcs,
    tags = ["exclusive"],
    deps = [":serve_lib"],
)


py_test(
    name = "test_persistence",
    size = "small",
    srcs = serve_tests_srcs,
    tags = ["exclusive"],
    deps = [":serve_lib"],
)


py_test(
    name = "test_router",
    size = "small",
    srcs = serve_tests_srcs,
    tags = ["exclusive"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_regression",
    size = "small",
    srcs = serve_tests_srcs,
    tags = ["exclusive"],
    deps = [":serve_lib"],
)


py_test(
    name = "test_scaling",
    size = "small",
    srcs = serve_tests_srcs,
    tags = ["exclusive"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_util",
    size = "small",
    srcs = serve_tests_srcs,
    tags = ["exclusive"],
    deps = [":serve_lib"],
)


# Runs test_api and test_failure with injected failures in the controller.
# TODO(edoakes): reenable this once we're using GCS actor fault tolerance.
# py_test(
    # name = "test_controller_crashes",
    # size = "medium",
    # srcs = glob(["tests/test_controller_crashes.py",
                 # "tests/test_api.py",
                 # "tests/test_failure.py"],
                # exclude=["tests/test_nonblocking.py",
                         # "tests/test_serve.py"]),
# )

py_test(
    name = "echo_full",
    size = "small",
    srcs = glob(["examples/*.py"]),
    tags = ["exclusive"],
    deps = [":serve_lib"]
)

py_test(
    name = "test_nonblocking",
    size = "small",
    srcs = glob(["tests/test_nonblocking.py"]),
    tags = ["exclusive"],
    deps = [":serve_lib"],
)

# Make sure the example showing in doc is tested
py_test(
    name = "quickstart_class",
    size = "small",
    srcs = glob(["examples/doc/*.py"]),
    tags = ["exclusive"],
    deps = [":serve_lib"]
)

py_test(
    name = "quickstart_function",
    size = "small",
    srcs = glob(["examples/doc/*.py"]),
    tags = ["exclusive"],
    deps = [":serve_lib"]
)

py_test(
    name = "tutorial_tensorflow",
    size = "small",
    srcs = glob(["examples/doc/*.py"]),
    tags = ["exclusive"],
    deps = [":serve_lib"]
)

py_test(
    name = "tutorial_pytorch",
    size = "small",
    srcs = glob(["examples/doc/*.py"]),
    tags = ["exclusive"],
    deps = [":serve_lib"]
)

py_test(
    name = "tutorial_sklearn",
    size = "small",
    srcs = glob(["examples/doc/*.py"]),
    tags = ["exclusive"],
    deps = [":serve_lib"]
)

py_test(
    name = "tutorial_batch",
    size = "small",
    srcs = glob(["examples/doc/*.py"]),
    tags = ["exclusive"],
    deps = [":serve_lib"]
)

py_test(
    name = "snippet_model_composition",
    size = "small",
    srcs = glob(["examples/doc/*.py"]),
    tags = ["exclusive"],
    deps = [":serve_lib"]
)

py_test(
    name = "snippet_metric_export",
    size = "small",
    srcs = glob(["examples/doc/*.py"]),
    tags = ["exclusive"],
    deps = [":serve_lib"]
)

# Disable the deployment tutorial test because it requires
# ray start --head in the background.
# py_test(
    #name = "tutorial_deploy",
    #size = "small",
    #srcs = glob(["examples/doc/*.py"]),
    #tags = ["exclusive"],
    #deps = [":serve_lib"]
# )

