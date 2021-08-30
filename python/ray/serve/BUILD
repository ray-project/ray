# This is a dummy test dependency that causes the above tests to be
# re-run if any of these files changes.
py_library(
    name = "serve_lib",
    srcs = glob(["**/*.py"], exclude=["tests/**/*.py"]),
    visibility = ["//python/ray/serve:__subpackages__", "//release:__pkg__"],
)

serve_tests_srcs = glob(["tests/**/*.py"])

py_test(
    name = "test_api",
    size = "medium",
    srcs = serve_tests_srcs,
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_deploy",
    size = "large",
    srcs = serve_tests_srcs,
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_get_deployment",
    size = "small",
    srcs = serve_tests_srcs,
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_http_prefix_matching",
    size = "small",
    srcs = serve_tests_srcs,
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_http_routes",
    size = "medium",
    srcs = serve_tests_srcs,
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_advanced",
    size = "small",
    srcs = serve_tests_srcs,
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_metrics",
    size = "small",
    srcs = serve_tests_srcs,
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_batching",
    size = "small",
    srcs = serve_tests_srcs,
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_controller",
    size = "small",
    srcs = serve_tests_srcs,
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_constructor_failure",
    size = "small",
    srcs = serve_tests_srcs,
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_ray_client",
    size = "small",
    srcs = serve_tests_srcs,
    tags = ["exclusive", "team:serverless"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_async_goal_manager",
    size = "small",
    srcs = serve_tests_srcs,
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_backend_state",
    size = "small",
    srcs = serve_tests_srcs,
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"],
)


py_test(
    name = "test_config",
    size = "small",
    srcs = serve_tests_srcs,
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"],
)


py_test(
    name = "test_failure",
    size = "small",
    srcs = serve_tests_srcs,
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"],
)


py_test(
    name = "test_handle",
    size = "small",
    srcs = serve_tests_srcs,
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"],
)


py_test(
    name = "test_kv_store",
    size = "small",
    srcs = serve_tests_srcs,
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"],
)


py_test(
    name = "test_persistence",
    size = "small",
    srcs = serve_tests_srcs,
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"],
)


py_test(
    name = "test_router",
    size = "small",
    srcs = serve_tests_srcs,
    tags = ["exclusive", "team:serve", "flaky"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_regression",
    size = "small",
    srcs = serve_tests_srcs,
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_long_poll",
    size = "small",
    srcs = serve_tests_srcs,
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_standalone",
    size = "medium",
    srcs = serve_tests_srcs,
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_cluster",
    size = "small",
    srcs = serve_tests_srcs,
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_util",
    size = "small",
    srcs = serve_tests_srcs,
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_fastapi",
    size = "medium",
    srcs = serve_tests_srcs,
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_runtime_env",
    size = "medium",
    srcs = serve_tests_srcs,
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_cli",
    size = "small",
    srcs = serve_tests_srcs,
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"],
)

# Runs test_api and test_failure with injected failures in the controller.
py_test(
   name = "test_controller_crashes",
   size = "medium",
   srcs = glob(["tests/test_controller_crashes.py",
               "tests/test_api.py",
               "tests/test_failure.py",
               "**/conftest.py"]),
   tags = ["exclusive", "team:serve"],
   deps = [":serve_lib"],
)

# Make sure the example showing in doc is tested
py_test(
    name = "quickstart_class",
    size = "small",
    srcs = glob(["examples/doc/*.py"]),
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"]
)

py_test(
    name = "quickstart_function",
    size = "small",
    srcs = glob(["examples/doc/*.py"]),
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"]
)

py_test(
    name = "tutorial_tensorflow",
    size = "small",
    srcs = glob(["examples/doc/*.py"]),
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"]
)

py_test(
    name = "tutorial_pytorch",
    size = "small",
    srcs = glob(["examples/doc/*.py"]),
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"]
)

py_test(
    name = "tutorial_sklearn",
    size = "small",
    srcs = glob(["examples/doc/*.py"]),
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"]
)

py_test(
    name = "tutorial_batch",
    size = "small",
    srcs = glob(["examples/doc/*.py"]),
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"]
)

py_test(
    name = "tutorial_rllib",
    size = "small",
    srcs = glob(["examples/doc/*.py"]),
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"]
)

py_test(
    name = "snippet_model_composition",
    size = "small",
    srcs = glob(["examples/doc/*.py"]),
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"]
)
