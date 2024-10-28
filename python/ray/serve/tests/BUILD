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
        "test_actor_replica_wrapper.py",
        "test_advanced.py",
        "test_batching.py",
        "test_cluster_node_info_cache.py",
        "test_constructor_failure.py",
        "test_controller.py",
        "test_deployment_graph_handle_serde.py",
        "test_deployment_graph_ingress.py",
        "test_deployment_node.py",
        "test_deployment_scheduler.py",
        "test_deployment_version.py",
        "test_expected_versions.py",
        "test_max_queued_requests.py",
        "test_persistence.py",
        "test_proxy.py",
        "test_proxy_actor_wrapper.py",
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
        "test_callback.py",
        "test_cancellation.py",
        "test_cluster.py",
        "test_controller_recovery.py",
        "test_failure.py",
        "test_grpc.py",
        "test_handle.py",
        "test_handle_api.py",
        "test_handle_streaming.py",
        "test_healthcheck.py",
        "test_http_headers.py",
        "test_http_routes.py",
        "test_logging.py",
        "test_max_replicas_per_node.py",
        "test_multiplex.py",
        "test_proxy_response_generator.py",
        "test_proxy_state.py",
        "test_ray_client.py",
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

# Medium tests, don't run on windows.
py_test_module_list(
    size = "medium",
    files = [
        "test_fastapi.py",
        "test_gcs_failure.py",
        "test_gradio.py",
    ],
    tags = [
        "exclusive",
        "no_windows",
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
        "test_autoscaling_policy.py",
        "test_deploy.py",
        "test_deploy_2.py",
        "test_standalone.py",
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

# Large tests requiring `test_config_files/`.
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

# Large tests require `test_config_files/`, no windows.
py_test_module_list(
    size = "large",
    data = glob(["test_config_files/**/*"]),
    files = [
        "test_standalone_2.py",
    ],
    tags = [
        "exclusive",
        "no_windows",
        "team:serve",
    ],
    deps = [
        ":common",
        ":conftest",
        "//python/ray/serve:serve_lib",
    ],
)

# Enormous tests.
py_test_module_list(
    size = "enormous",
    files = [
        "test_deploy_app.py",
        "test_metrics.py",
        "test_enable_task_events.py",
        "test_kv_store.py",
        "test_long_poll.py",
        "test_regression.py",
    ],
    tags = [
        "exclusive",
        "team:serve",
        "use_all_core_windows",
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

# Serve HA.
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

# ----- TEST FEATURE FLAGS -----

# Test autoscaling with metrics collected from replica instead of handle.
py_test(
    name = "test_autoscaling_policy_with_metr_disab",
    size = "large",
    srcs = ["test_autoscaling_policy.py"],
    env = {"RAY_SERVE_COLLECT_AUTOSCALING_METRICS_ON_HANDLE": "0"},
    main = "test_autoscaling_policy.py",
    tags = [
        "autoscaling",
        "exclusive",
        "team:serve",
    ],
    deps = [
        ":common",
        ":conftest",
        "//python/ray/serve:serve_lib",
    ],
)

# Tests disabling queue len caching feature flag.
# TODO(edoakes): remove this after the FF is removed.
py_test_module_list(
    size = "medium",
    data = glob(["test_config_files/**/*"]),
    env = {"RAY_SERVE_ENABLE_QUEUE_LENGTH_CACHE": "0"},
    files = [
        "test_cancellation.py",
        "test_handle.py",
        "test_handle_api.py",
        "test_handle_streaming.py",
        "test_multiplex.py",
        "test_request_timeout.py",
    ],
    name_suffix = "_with_queue_len_cache_disabled",
    tags = [
        "exclusive",
        "no_windows",
        "team:serve",
    ],
    deps = [
        ":common",
        ":conftest",
        "//python/ray/serve:serve_lib",
    ],
)

# Tests autoscaling when queue len caching is disabled.
py_test_module_list(
    size = "large",
    env = {"RAY_SERVE_ENABLE_QUEUE_LENGTH_CACHE": "0"},
    files = [
        "test_autoscaling_policy.py",
    ],
    name_suffix = "_with_queue_len_cache_disabled",
    tags = [
        "exclusive",
        "no_windows",
        "team:serve",
    ],
    deps = [
        ":common",
        ":conftest",
        "//python/ray/serve:serve_lib",
    ],
)

# Test old stop-fully-then-start behavior.
# TODO(zcin): remove this after the old behavior is completely removed
py_test_module_list(
    size = "medium",
    data = glob(["test_config_files/**/*"]),
    env = {"RAY_SERVE_EAGERLY_START_REPLACEMENT_REPLICAS": "0"},
    files = [
        "test_controller_recovery.py",
        "test_deploy.py",
        "test_max_replicas_per_node.py",
    ],
    name_suffix = "_with_stop_fully_then_start_behavior",
    tags = [
        "exclusive",
        "no_windows",
        "team:serve",
    ],
    deps = [
        ":common",
        ":conftest",
        "//python/ray/serve:serve_lib",
    ],
)

# Test feature flag for task events.
py_test_module_list(
    size = "small",
    data = glob(["test_config_files/**/*"]),
    env = {"RAY_SERVE_ENABLE_TASK_EVENTS": "1"},
    files = [
        "test_enable_task_events.py",
    ],
    name_suffix = "_with_task_events_enabled",
    tags = [
        "exclusive",
        "no_windows",
        "team:serve",
    ],
    deps = [
        ":common",
        ":conftest",
        "//python/ray/serve:serve_lib",
    ],
)

# Medium tests with compact scheduling
py_test_module_list(
    size = "medium",
    data = glob(["test_config_files/**/*"]),
    env = {"RAY_SERVE_USE_COMPACT_SCHEDULING_STRATEGY": "1"},
    files = [
        "test_cluster.py",
        "test_controller_recovery.py",
        "test_deployment_scheduler.py",
        "test_gcs_failure.py",
        "test_max_replicas_per_node.py",
        "test_replica_placement_group.py",
    ],
    name_suffix = "_with_compact_scheduling",
    tags = [
        "exclusive",
        "no_windows",
        "team:serve",
    ],
    deps = [
        ":common",
        ":conftest",
        "//python/ray/serve:serve_lib",
    ],
)

# Large tests with compact scheduling
py_test_module_list(
    size = "large",
    env = {"RAY_SERVE_USE_COMPACT_SCHEDULING_STRATEGY": "1"},
    files = [
        "test_standalone.py",
        "test_standalone_3.py",
    ],
    name_suffix = "_with_comp_sche",
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

# Large tests with compact scheduling, no windows
py_test_module_list(
    size = "large",
    data = glob(["test_config_files/**/*"]),
    env = {"RAY_SERVE_USE_COMPACT_SCHEDULING_STRATEGY": "1"},
    files = [
        "test_standalone_2.py",
    ],
    name_suffix = "_with_compact_scheduling",
    tags = [
        "exclusive",
        "no_windows",
        "team:serve",
    ],
    deps = [
        ":common",
        ":conftest",
        "//python/ray/serve:serve_lib",
    ],
)

