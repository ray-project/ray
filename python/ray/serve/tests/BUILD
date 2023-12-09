load("//bazel:python.bzl", "py_test_module_list")

py_library(
    name = "conftest",
    srcs = ["conftest.py"],
)

py_library(
    name = "common",
    srcs = glob(["common/*.py"]),
    visibility = [
        "//python/ray/serve/tests:__subpackages__",
    ],
)

# Small tests.
py_test_module_list(
    size = "small",
    files = [
        "test_advanced.py",
        "test_autoscaling_metrics.py",
        "test_batching.py",
        "test_cluster_node_info_cache.py",
        "test_constructor_failure.py",
        "test_controller.py",
        "test_cross_language.py",
        "test_deployment_graph_handle_serde.py",
        "test_deployment_graph_ingress_deployment.py",
        "test_deployment_node.py",
        "test_deployment_scheduler.py",
        "test_deployment_version.py",
        "test_expected_versions.py",
        "test_http_util.py",
        "test_kv_store.py",
        "test_long_poll.py",
        "test_persistence.py",
        "test_proxy.py",
        "test_proxy_router.py",
        "test_util.py",
        "test_websockets.py",
    ],
    tags = [
        "exclusive",
        "team:serve",
    ],
    deps = [
        ":common",
        ":conftest",
        "//python/ray/serve:serve_lib",
    ],
)

# Medium tests.
py_test_module_list(
    size = "medium",
    files = [
        "test_autoscaling_policy.py",
        "test_callback.py",
        "test_cancellation.py",
        "test_cluster.py",
        "test_controller_recovery.py",
        "test_deployment_graph_build.py",
        "test_failure.py",
        "test_fastapi.py",
        "test_gcs_failure.py",
        "test_gradio.py",
        "test_grpc.py",
        "test_handle.py",
        "test_handle_streaming.py",
        "test_healthcheck.py",
        "test_http_headers.py",
        "test_http_routes.py",
        "test_logging.py",
        "test_max_replicas_per_node.py",
        "test_multiplex.py",
        "test_new_handle_api.py",
        "test_proxy_response_generator.py",
        "test_proxy_state.py",
        "test_ray_client.py",
        "test_regression.py",
        "test_replica_placement_group.py",
        "test_request_timeout.py",
        "test_streaming_response.py",
        "test_target_capacity.py",
    ],
    tags = [
        "exclusive",
        "team:serve",
    ],
    deps = [
        ":common",
        ":conftest",
        "//python/ray/serve:serve_lib",
    ],
)

# Large tests.
py_test_module_list(
    size = "large",
    files = [
        "test_deploy.py",
        "test_deploy_2.py",
        "test_deploy_app.py",
        "test_metrics.py",
        "test_standalone.py",
        "test_standalone_2.py",
        "test_standalone_3.py",
        "test_telemetry.py",
        "test_telemetry_2.py",
    ],
    tags = [
        "exclusive",
        "team:serve",
    ],
    deps = [
        ":common",
        ":conftest",
        "//python/ray/serve:serve_lib",
    ],
)

# Minimal tests
py_test_module_list(
    size = "large",
    files = [
        "test_api.py",
        "test_model_composition.py",
    ],
    tags = [
        "exclusive",
        "minimal",
        "team:serve",
    ],
    deps = [
        ":common",
        ":conftest",
        "//python/ray/serve:serve_lib",
    ],
)

# Post-wheel-build tests.
py_test_module_list(
    size = "large",
    files = [
        "test_runtime_env.py",
        "test_runtime_env_2.py",
    ],
    tags = [
        "exclusive",
        "post_wheel_build",
        "team:serve",
    ],
    deps = [
        ":common",
        ":conftest",
        "//python/ray/serve:serve_lib",
    ],
)

# Tests requiring `test_config_files/`.
py_test_module_list(
    size = "large",
    data = glob(["test_config_files/**/*"]),
    files = [
        "test_cli.py",
        "test_cli_2.py",
    ],
    tags = [
        "exclusive",
        "team:serve",
    ],
    deps = [
        ":common",
        ":conftest",
        "//python/ray/serve:serve_lib",
    ],
)

# Runs test_api and test_failure with injected failures in the controller.
py_test(
    name = "test_controller_crashes",
    size = "large",
    srcs = [
        "test_api.py",
        "test_controller_crashes.py",
        "test_failure.py",
    ],
    tags = [
        "exclusive",
        "team:serve",
    ],
    deps = [
        ":common",
        ":conftest",
        "//python/ray/serve:serve_lib",
    ],
)

py_test(
    name = "test_new_handle_api_set_via_env_var",
    size = "medium",
    srcs = ["test_new_handle_api_set_via_env_var.py"],
    env = {"RAY_SERVE_ENABLE_NEW_HANDLE_API": "1"},
    tags = [
        "exclusive",
        "team:serve",
    ],
    deps = [
        ":common",
        ":conftest",
        "//python/ray/serve:serve_lib",
    ],
)

py_test(
    name = "tutorial_rllib",
    size = "medium",
    srcs = ["test_myst_doc.py"],
    args = [
        "--path",
        "doc/source/serve/tutorials/rllib.md",
    ],
    data = ["//doc/source/serve/tutorials:markdowns"],
    main = "test_myst_doc.py",
    tags = [
        "exclusive",
        "no_main",
        "team:serve",
    ],
    deps = [
        ":common",
        ":conftest",
        "//python/ray/serve:serve_lib",
    ],
)

py_test(
    name = "test_serve_ha",
    size = "medium",
    srcs = ["test_serve_ha.py"],
    tags = [
        "exclusive",
        "ha_integration",
        "team:serve",
    ],
    deps = [
        ":common",
        ":conftest",
        "//python/ray/serve:serve_lib",
    ],
)
