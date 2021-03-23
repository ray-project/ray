# This is a dummy test dependency that causes the above tests to be
# re-run if any of these files changes.
py_library(
    name = "serve_lib",
    srcs = glob(["**/*.py"], exclude=["tests/*.py"]),
)

serve_tests_srcs = glob(["tests/*.py"],
     exclude=["tests/test_serve.py",
             ])

py_test(
    name = "test_api",
    size = "large",
    srcs = serve_tests_srcs,
    tags = ["exclusive"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_deploy",
    size = "medium",
    srcs = serve_tests_srcs,
    tags = ["exclusive"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_advanced",
    size = "medium",
    srcs = serve_tests_srcs,
    tags = ["exclusive"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_metrics",
    size = "medium",
    srcs = serve_tests_srcs,
    tags = ["exclusive"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_batching",
    size = "medium",
    srcs = serve_tests_srcs,
    tags = ["exclusive"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_controller",
    size = "small",
    srcs = serve_tests_srcs,
    tags = ["exclusive"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_ray_client",
    size = "small",
    srcs = serve_tests_srcs,
    tags = ["exclusive"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_async_goal_manager",
    size = "small",
    srcs = serve_tests_srcs,
    tags = ["exclusive"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_backend_state",
    size = "small",
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
    size = "medium",
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
    name = "test_long_poll",
    size = "small",
    srcs = serve_tests_srcs,
    tags = ["exclusive"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_standalone",
    size = "medium",
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

py_test(
    name = "test_imported_backend",
    size = "small",
    srcs = serve_tests_srcs,
    tags = ["exclusive"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_pydantic_serialization",
    size = "small",
    srcs = serve_tests_srcs,
    tags = ["exclusive"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_fastapi",
    size = "small",
    srcs = serve_tests_srcs,
    tags = ["exclusive"],
    deps = [":serve_lib"],
)

# Runs test_api and test_failure with injected failures in the controller.
# TODO(simon): Tests are disabled until #11683 is fixed.
# py_test(
#     name = "test_controller_crashes",
#     size = "large",
#     srcs = glob(["tests/test_controller_crashes.py",
#                 "tests/test_api.py",
#                 "tests/test_failure.py",
#                 "**/conftest.py"],
#                 exclude=["tests/test_serve.py"]),
# )

py_test(
    name = "echo_full",
    size = "small",
    srcs = glob(["examples/*.py"]),
    tags = ["exclusive"],
    deps = [":serve_lib"]
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
    name = "tutorial_rllib",
    size = "medium",
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

# Disable the deployment tutorial test because it requires
# ray start --head in the background.
# py_test(
    #name = "tutorial_deploy",
    #size = "small",
    #srcs = glob(["examples/doc/*.py"]),
    #tags = ["exclusive"],
    #deps = [":serve_lib"]
# )

