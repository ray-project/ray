# @IMPORTANT: Two targets from this directory's BUILD.bazel are defined in
# `src/ray/meson.build` instead of here, due to dependency ordering constraints:
#
#   - `metric_interface` (`metric_interface_cpp_lib_dep`) is defined early in src/ray/meson.build (before subdir('stats/'))
#     because `stats/tag_defs` depends on it. Pulling it here would be too late.
#   - `open_telemetry_metric_recorder` is defined in src/ray/meson.build (after subdir('rpc/'))
#     because it depends on `rpc/` targets (rpc_common, authentication_mode,
#     authentication_token_loader) that are processed after observability/.
#
# All other targets in BUILD.bazel are defined here as normal, except:
#   - `fake_metric` (fake_metric.h) — test/fake helper, intentionally omitted
#   - `fake_ray_event_recorder` (fake_ray_event_recorder.h) — test/fake helper, intentionally omitted

# === @begin: open_telemetry_metric_recorder_cpp_lib (@original: open_telemetry_metric_recorder) ===
# @IMPORTANT: MOVED INLINE to src/ray/meson.build (after subdir('rpc/')) because it depends
# on rpc/ targets (rpc_common, authentication_mode, authentication_token_loader) that are
# processed after observability/. `open_telemetry_metric_recorder_cpp_lib_dep` is still
# available here — it's already defined by the time this file is processed.
# === @end: open_telemetry_metric_recorder_cpp_lib ===


# === @begin: metric_interface_cpp_lib (@original: metric_interface) ===
# @IMPORTANT: `metric_interface_cpp_lib_dep` is defined EARLY in src/ray/meson.build
# (before subdir('stats/')) because stats/tag_defs depends on it.
# Do NOT redefine it here — it would cause a duplicate variable error.
# === @end: metric_interface_cpp_lib ===


# === @begin: ray_event_interface_cpp_lib (@original: ray_event_interface) ===
# Header-only
ray_event_interface_cpp_lib_dependencies = [
    # Package-managed
    events_base_event_proto_cpp_lib_dep,
]

ray_event_interface_cpp_lib_dep = declare_dependency(
    sources: ['ray_event_interface.h'],
    dependencies: ray_event_interface_cpp_lib_dependencies
)
# === @end: ray_event_interface_cpp_lib ===


# === @begin: ray_event_cpp_lib (@original: ray_event) ===
# Header-only
ray_event_cpp_lib_dependencies = [
    # Package-managed
    ray_event_interface_cpp_lib_dep,
    grpc_util_cpp_lib_dep,
    id_cpp_lib_dep,
    gcs_proto_cpp_lib_dep,

    # Eugo-managed
    absl_time,
]

ray_event_cpp_lib_dep = declare_dependency(
    sources: ['ray_event.h'],
    dependencies: ray_event_cpp_lib_dependencies
)
# === @end: ray_event_cpp_lib ===


# === @begin: ray_driver_job_definition_event_cpp_lib (@original: ray_driver_job_definition_event) ===
ray_driver_job_definition_event_cpp_lib_dependencies = [
    # Package-managed
    ray_event_cpp_lib_dep,
    events_driver_job_definition_event_proto_cpp_lib_dep,
]

ray_driver_job_definition_event_cpp_lib = static_library(
    'ray_driver_job_definition_event_cpp_lib',
    ['ray_driver_job_definition_event.cc'],
    install: false,
    dependencies: ray_driver_job_definition_event_cpp_lib_dependencies
)

ray_driver_job_definition_event_cpp_lib_dep = declare_dependency(
    link_with: [ray_driver_job_definition_event_cpp_lib],
    dependencies: ray_driver_job_definition_event_cpp_lib_dependencies
)
# === @end: ray_driver_job_definition_event_cpp_lib ===


# === @begin: ray_driver_job_lifecycle_event_cpp_lib (@original: ray_driver_job_lifecycle_event) ===
ray_driver_job_lifecycle_event_cpp_lib_dependencies = [
    # Package-managed
    ray_event_cpp_lib_dep,
    events_driver_job_lifecycle_event_proto_cpp_lib_dep,
]

ray_driver_job_lifecycle_event_cpp_lib = static_library(
    'ray_driver_job_lifecycle_event_cpp_lib',
    ['ray_driver_job_lifecycle_event.cc'],
    install: false,
    dependencies: ray_driver_job_lifecycle_event_cpp_lib_dependencies
)

ray_driver_job_lifecycle_event_cpp_lib_dep = declare_dependency(
    link_with: [ray_driver_job_lifecycle_event_cpp_lib],
    dependencies: ray_driver_job_lifecycle_event_cpp_lib_dependencies
)
# === @end: ray_driver_job_lifecycle_event_cpp_lib ===


# === @begin: ray_actor_definition_event_cpp_lib (@original: ray_actor_definition_event) ===
ray_actor_definition_event_cpp_lib_dependencies = [
    # Package-managed
    ray_event_cpp_lib_dep,
    label_selector_cpp_lib_dep,
    events_actor_definition_event_proto_cpp_lib_dep,
]

ray_actor_definition_event_cpp_lib = static_library(
    'ray_actor_definition_event_cpp_lib',
    ['ray_actor_definition_event.cc'],
    install: false,
    dependencies: ray_actor_definition_event_cpp_lib_dependencies
)

ray_actor_definition_event_cpp_lib_dep = declare_dependency(
    link_with: [ray_actor_definition_event_cpp_lib],
    dependencies: ray_actor_definition_event_cpp_lib_dependencies
)
# === @end: ray_actor_definition_event_cpp_lib ===


# === @begin: ray_actor_lifecycle_event_cpp_lib (@original: ray_actor_lifecycle_event) ===
ray_actor_lifecycle_event_cpp_lib_dependencies = [
    # Package-managed
    ray_event_cpp_lib_dep,
    events_actor_lifecycle_event_proto_cpp_lib_dep,
]

ray_actor_lifecycle_event_cpp_lib = static_library(
    'ray_actor_lifecycle_event_cpp_lib',
    ['ray_actor_lifecycle_event.cc'],
    install: false,
    dependencies: ray_actor_lifecycle_event_cpp_lib_dependencies
)

ray_actor_lifecycle_event_cpp_lib_dep = declare_dependency(
    link_with: [ray_actor_lifecycle_event_cpp_lib],
    dependencies: ray_actor_lifecycle_event_cpp_lib_dependencies
)
# === @end: ray_actor_lifecycle_event_cpp_lib ===


# === @begin: ray_node_definition_event_cpp_lib (@original: ray_node_definition_event) ===
ray_node_definition_event_cpp_lib_dependencies = [
    # Package-managed
    ray_event_cpp_lib_dep,
    events_node_definition_event_proto_cpp_lib_dep,
]

ray_node_definition_event_cpp_lib = static_library(
    'ray_node_definition_event_cpp_lib',
    ['ray_node_definition_event.cc'],
    install: false,
    dependencies: ray_node_definition_event_cpp_lib_dependencies
)

ray_node_definition_event_cpp_lib_dep = declare_dependency(
    link_with: [ray_node_definition_event_cpp_lib],
    dependencies: ray_node_definition_event_cpp_lib_dependencies
)
# === @end: ray_node_definition_event_cpp_lib ===


# === @begin: ray_node_lifecycle_event_cpp_lib (@original: ray_node_lifecycle_event) ===
ray_node_lifecycle_event_cpp_lib_dependencies = [
    # Package-managed
    ray_event_cpp_lib_dep,
    events_node_lifecycle_event_proto_cpp_lib_dep,
]

ray_node_lifecycle_event_cpp_lib = static_library(
    'ray_node_lifecycle_event_cpp_lib',
    ['ray_node_lifecycle_event.cc'],
    install: false,
    dependencies: ray_node_lifecycle_event_cpp_lib_dependencies
)

ray_node_lifecycle_event_cpp_lib_dep = declare_dependency(
    link_with: [ray_node_lifecycle_event_cpp_lib],
    dependencies: ray_node_lifecycle_event_cpp_lib_dependencies
)
# === @end: ray_node_lifecycle_event_cpp_lib ===


# === @begin: ray_event_recorder_interface_cpp_lib (@original: ray_event_recorder_interface) ===
# Header-only
ray_event_recorder_interface_cpp_lib_dependencies = [
    # Package-managed
    ray_event_cpp_lib_dep,
]

ray_event_recorder_interface_cpp_lib_dep = declare_dependency(
    sources: ['ray_event_recorder_interface.h'],
    dependencies: ray_event_recorder_interface_cpp_lib_dependencies
)
# === @end: ray_event_recorder_interface_cpp_lib ===


# === @begin: ray_event_recorder_cpp_lib (@original: ray_event_recorder) ===
ray_event_recorder_cpp_lib_dependencies = [
    # Package-managed
    metric_interface_cpp_lib_dep,
    ray_event_cpp_lib_dep,
    ray_event_recorder_interface_cpp_lib_dep,
    asio_cpp_lib_dep,
    ray_config_cpp_lib_dep,
    events_event_aggregator_service_proto_cpp_lib_dep,
    event_aggregator_client_cpp_lib_dep,
    graceful_shutdown_cpp_lib_dep,
    logging_cpp_lib_dep,

    # Eugo-managed
    boost_circular_buffer,
]

ray_event_recorder_cpp_lib = static_library(
    'ray_event_recorder_cpp_lib',
    ['ray_event_recorder.cc'],
    install: false,
    dependencies: ray_event_recorder_cpp_lib_dependencies
)

ray_event_recorder_cpp_lib_dep = declare_dependency(
    link_with: [ray_event_recorder_cpp_lib],
    dependencies: ray_event_recorder_cpp_lib_dependencies
)
# === @end: ray_event_recorder_cpp_lib ===


# === @begin: observability_metrics_cpp_lib (@original: metrics) ===
# Header-only
observability_metrics_cpp_lib_dependencies = [
    # Package-managed
    stats_cpp_lib_dep,
]

observability_metrics_cpp_lib_dep = declare_dependency(
    sources: ['metrics.h'],
    dependencies: observability_metrics_cpp_lib_dependencies
)
# === @end: observability_metrics_cpp_lib ===


# === @begin: metric_constants_cpp_lib (@original: metric_constants) ===
metric_constants_cpp_lib_dep = declare_dependency(
    sources: ['metric_constants.h'],
)
# === @end: metric_constants_cpp_lib ===


# Umbrella dependency — backward compatible with consumers using the consolidated target
observability_cpp_lib_dep = declare_dependency(
    dependencies: [
        metric_interface_cpp_lib_dep,
        metric_constants_cpp_lib_dep,
        ray_event_interface_cpp_lib_dep,
        ray_event_cpp_lib_dep,
        ray_driver_job_definition_event_cpp_lib_dep,
        ray_driver_job_lifecycle_event_cpp_lib_dep,
        ray_actor_definition_event_cpp_lib_dep,
        ray_actor_lifecycle_event_cpp_lib_dep,
        ray_node_definition_event_cpp_lib_dep,
        ray_node_lifecycle_event_cpp_lib_dep,
        ray_event_recorder_interface_cpp_lib_dep,
        ray_event_recorder_cpp_lib_dep,
        open_telemetry_metric_recorder_cpp_lib_dep,
        observability_metrics_cpp_lib_dep,
    ]
)
