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
  ]
)

# Small tests.
py_test_module_list(
  files = [
    "test_advanced.py",
    "test_batching.py",
    "test_constructor_failure.py",
    "test_controller.py",
    "test_proxy_router.py",
    "test_proxy_actor_wrapper.py",
    "test_deployment_scheduler.py",
    "test_deployment_version.py",
    "test_kv_store.py",
    "test_persistence.py",
    "test_cluster_node_info_cache.py",
    "test_long_poll.py",
    "test_util.py",
    "test_proxy.py",
    "test_deployment_graph_ingress.py",
    "test_deployment_graph_handle_serde.py",
    "test_websockets.py",
    "test_deployment_node.py",
    "test_expected_versions.py"
  ],
  size = "small",
  tags = ["exclusive", "team:serve"],
  deps = ["//python/ray/serve:serve_lib", ":conftest", ":common"]
)

py_test_module_list(
  files = [
    "test_cross_language.py",
  ],
  size = "small",
  tags = ["exclusive", "no_windows", "team:serve"],
  deps = ["//python/ray/serve:serve_lib", ":conftest", ":common"]
)

# Medium tests.
py_test_module_list(
  files = [
    "test_healthcheck.py",
    "test_http_routes.py",
    "test_logging.py",
    "test_proxy_state.py",
    "test_ray_client.py",
    "test_failure.py",
    "test_handle.py",
    "test_handle_streaming.py",
    "test_new_handle_api.py",
    "test_regression.py",
    "test_request_timeout.py",
    "test_cluster.py",
    "test_autoscaling_policy.py",
    "test_cancellation.py",
    "test_streaming_response.py",
    "test_controller_recovery.py",
    "test_replica_placement_group.py",
    "test_max_replicas_per_node.py",
    "test_deployment_graph_build.py",
    "test_grpc.py",
    "test_multiplex.py",
    "test_callback.py",
    "test_http_headers.py",
    "test_proxy_response_generator.py",
    "test_target_capacity.py",
  ],
  size = "medium",
  tags = ["exclusive", "team:serve"],
  deps = ["//python/ray/serve:serve_lib", ":conftest", ":common"],
)

py_test_module_list(
  files = [
    "test_gcs_failure.py",
    "test_gradio.py",
    "test_fastapi.py",
  ],
  size = "medium",
  tags = ["exclusive", "no_windows", "team:serve"],
  deps = ["//python/ray/serve:serve_lib", ":conftest", ":common"],
)

# Large tests.
py_test_module_list(
  files = [
    "test_deploy.py",
    "test_deploy_2.py",
    "test_metrics.py",
    "test_telemetry.py",
    "test_telemetry_2.py",
    "test_standalone.py",
    "test_standalone_3.py",
    "test_deploy_app.py",
  ],
  size = "large",
  tags = ["exclusive", "team:serve"],
  deps = ["//python/ray/serve:serve_lib", ":conftest", ":common"],
)

py_test_module_list(
  files = [
    "test_standalone_2.py",
  ],
  size = "large",
  tags = ["exclusive", "no_windows", "team:serve"],
  deps = ["//python/ray/serve:serve_lib", ":conftest", ":common"],
)

# Minimal tests
py_test_module_list(
  files = [
    "test_api.py",
    "test_model_composition.py",
  ],
  size = "large",
  tags = ["exclusive", "team:serve", "minimal"],
  deps = ["//python/ray/serve:serve_lib", ":conftest", ":common"],
)

# Post-wheel-build tests.
py_test_module_list(
  files = [
    "test_runtime_env.py",
    "test_runtime_env_2.py",
  ],
  size = "large",
  tags = ["exclusive", "team:serve", "post_wheel_build"],
  deps = ["//python/ray/serve:serve_lib", ":conftest", ":common"],
)

# Tests requiring `test_config_files/`.
py_test_module_list(
  files = [
    "test_cli.py",
    "test_cli_2.py",
  ],
  size = "large",
  tags = ["exclusive", "team:serve"],
  deps = ["//python/ray/serve:serve_lib", ":conftest", ":common"],
  data = glob(["test_config_files/**/*"]),
)

# Runs test_api and test_failure with injected failures in the controller.
py_test(
   name = "test_controller_crashes",
   size = "large",
   srcs = ["test_controller_crashes.py",
               "test_api.py",
               "test_failure.py"],
   tags = ["exclusive", "team:serve"],
   deps = ["//python/ray/serve:serve_lib", ":conftest", ":common"],
)

py_test(
    name = "test_new_handle_api_set_via_env_var",
    size = "medium",
    srcs = ["test_new_handle_api_set_via_env_var.py"],
    env = {"RAY_SERVE_ENABLE_NEW_HANDLE_API": "1"},
    tags = ["exclusive", "team:serve"],
    deps = ["//python/ray/serve:serve_lib", ":conftest", ":common"],
)

py_test(
    name = "test_serve_ha",
    srcs = ["test_serve_ha.py"],
    size = "medium",
    tags = ["exclusive", "team:serve", "ha_integration"],
    deps = ["//python/ray/serve:serve_lib", ":conftest", ":common"],
)
