# This is a dummy test dependency that causes the above tests to be
# re-run if any of these files changes.
py_library(
    name = "serve_lib",
    srcs = glob(["**/*.py"], exclude=["tests/**/*.py"]),
    visibility = [
        "//python/ray/serve:__pkg__",
        "//python/ray/serve:__subpackages__",
        "//release:__pkg__"
    ],
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
    name = "test_application",
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
    name = "test_deploy_2",
    size = "large",
    srcs = serve_tests_srcs,
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_healthcheck",
    size = "medium",
    srcs = serve_tests_srcs,
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_get_deployment",
    size = "medium",
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
    name = "test_http_state",
    size = "small",
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
    name = "test_autoscaling_metrics",
    size = "small",
    srcs = serve_tests_srcs,
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_metrics",
    size = "medium",
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
    size = "medium",
    srcs = serve_tests_srcs,
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_ray_client",
    size = "medium",
    srcs = serve_tests_srcs,
    tags = ["exclusive", "team:serverless"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_deployment_state",
    size = "small",
    srcs = serve_tests_srcs,
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_deployment_version",
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
    size = "medium",
    srcs = serve_tests_srcs,
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"],
)


py_test(
    name = "test_handle",
    size = "medium",
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
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_regression",
    size = "medium",
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
    name = "test_schema",
    size = "medium",
    srcs = serve_tests_srcs,
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_standalone",
    size = "large",
    srcs = serve_tests_srcs,
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_standalone2",
    size = "medium",
    srcs = serve_tests_srcs,
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_cluster",
    size = "medium",
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
    name = "test_logs",
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
    size = "large",
    srcs = serve_tests_srcs,
    tags = ["exclusive", "team:serve", "post_wheel_build"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_runtime_env_2",
    size = "large",
    srcs = serve_tests_srcs,
    tags = ["exclusive", "team:serve", "post_wheel_build"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_cli",
    size = "large",
    srcs = serve_tests_srcs,
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_autoscaling_policy",
    size = "medium",
    srcs = serve_tests_srcs,
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_cross_language",
    size = "medium",
    srcs = serve_tests_srcs,
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_model_wrappers",
    size = "small",
    srcs = serve_tests_srcs,
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_http_adapters",
    size = "small",
    srcs = serve_tests_srcs,
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_pipeline_driver",
    size = "medium",
    srcs = serve_tests_srcs,
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_pipeline_dag",
    size = "medium",
    srcs = serve_tests_srcs,
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_pipeline_ingress_deployment",
    size = "medium",
    srcs = serve_tests_srcs,
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_pipeline_deployment_config",
    size = "small",
    srcs = serve_tests_srcs,
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_pipeline_handle_serde",
    size = "small",
    srcs = serve_tests_srcs,
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"],
)
# Runs test_api and test_failure with injected failures in the controller.
py_test(
   name = "test_controller_crashes",
   size = "large",
   srcs = glob(["tests/test_controller_crashes.py",
               "tests/test_api.py",
               "tests/test_failure.py",
               "**/conftest.py"]),
   tags = ["exclusive", "team:serve"],
   deps = [":serve_lib"],
)

py_test(
    name = "test_controller_recovery",
    size = "medium",
    srcs = serve_tests_srcs,
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
    name = "tutorial_sklearn_ray_overview",
    size = "small",
    main = "quick_start.py",
    srcs = ["//doc/source/serve/doc_code:serve_doc_code"],
    tags = ["exclusive", "team:serve"],
)

py_test(
    name = "tutorial_rllib",
    size = "small",
    srcs = serve_tests_srcs,
    main = "test_myst_doc.py",
    args = ["--path", "doc/source/serve/tutorials/rllib.md"],
    data = ["//doc/source/serve/tutorials:markdowns"],
    tags = ["exclusive", "team:serve"],
)

py_test(
    name = "snippet_model_composition",
    size = "small",
    srcs = glob(["examples/doc/*.py"]),
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"]
)

py_test(
    name = "conda_env",
    size = "medium",
    srcs = glob(["examples/doc/*.py"]),
    tags = ["exclusive", "post_wheel_build", "team:serve"],
    deps = [":serve_lib"]
)

pipeline_tests_srcs = glob(["pipeline/tests/**/*.py"])

py_test(
    name = "test_deployment_node",
    size = "medium",
    srcs = pipeline_tests_srcs,
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"],
)

py_test(
    name = "test_generate",
    size = "medium",
    srcs = pipeline_tests_srcs,
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"],
)

py_test(
    name = "snippet_model_ensemble",
    size = "small",
    srcs = glob(["examples/doc/*.py"]),
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"]
)

py_test(
    name = "test_json_serde",
    size = "medium",
    srcs = pipeline_tests_srcs,
    tags = ["exclusive", "team:serve"],
    deps = [":serve_lib"],
)
