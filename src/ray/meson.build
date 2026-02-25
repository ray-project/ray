# @IMPORTANT: This file exists despite no corresponding BUILD.bazel because it contains shared variable definitions (e.g. `metric_interface_cpp_lib_dep`) and shared targets (e.g. `stats_cpp_lib`) that are used across multiple subdirectories, and it's cleaner to define them in one place rather than duplicate them in multiple meson.build files across stats/, observability/, gcs/, etc.

subdir('protobuf/')
subdir('thirdparty/')
subdir('util/')

# === @begin: metric_interface_cpp_lib (@original: metric_interface from observability/) ===
# Header-only
# # @IMPORTANT - Defined early (before `stats/`) because `stats/tag_defs` depends on it.
# The canonical Bazel target is `//src/ray/observability:metric_interface`,
# but it's header-only with only an external dep, so it can safely live here.
metric_interface_cpp_lib_dependencies = [
    # Eugo-managed
    opencensus_cpp_stats,
]

metric_interface_cpp_lib_dep = declare_dependency(
    sources: ['observability/metric_interface.h'],
    dependencies: metric_interface_cpp_lib_dependencies
)
# === @end: metric_interface_cpp_lib ===

subdir('stats/')

# FlatBuffers codegen (formerly raylet/format/)
subdir('flatbuffers/')

subdir('common/')

subdir('rpc/')

# `ray_syncer` depends on `rpc/authentication/` targets (`authentication_mode`, `authentication_token`,
# `authentication_token_loader`, `authentication_token_validator`), so it must come AFTER rpc/.
subdir('ray_syncer/')


# @IMPORTANT: defined inline here (after rpc/) because it depends on `rpc/` targets
# (`rpc_common`, `authentication_mode`, `authentication_token_loader`) but is needed by
# `stats_metric` (defined in `stats/` which is processed BEFORE `rpc/`).  For `stats_metric`
# the dep is still circular — headers found via project-wide -I, symbols resolved at
# final _raylet link time.
# === @begin: observability/open_telemetry_metric_recorder_cpp_lib (@original: open_telemetry_metric_recorder) ===
open_telemetry_metric_recorder_cpp_lib_dependencies = [
    # Package-managed
    constants_cpp_lib_dep,
    ray_config_cpp_lib_dep,
    rpc_common_cpp_lib_dep,
    authentication_mode_cpp_lib_dep,
    authentication_token_loader_cpp_lib_dep,
    logging_cpp_lib_dep,

    # Eugo-managed
    absl_container_flat_hash_map,
    opentelemetry_cpp_api,
    opentelemetry_cpp_otlp_grpc_metric_exporter,
    opentelemetry_cpp_metrics,
]

open_telemetry_metric_recorder_cpp_lib = static_library(
    'open_telemetry_metric_recorder_cpp_lib',
    ['observability/open_telemetry_metric_recorder.cc'],
    install: false,
    # IMPORTANT: intentionally ommitted for now as we dont want to enforce mTLS at the moment as it causes other potential complications. Return to this later!
    # cpp_args: ['-DENABLE_OTLP_GRPC_SSL_MTLS_PREVIEW'],  # @IMPORTANT: mTLS support, must match opentelemetry-cpp build
    dependencies: open_telemetry_metric_recorder_cpp_lib_dependencies
)

open_telemetry_metric_recorder_cpp_lib_dep = declare_dependency(
    link_with: [open_telemetry_metric_recorder_cpp_lib],
    dependencies: open_telemetry_metric_recorder_cpp_lib_dependencies
)
# === @end: observability/open_telemetry_metric_recorder_cpp_lib ===


# @Important: due to circular dependency between `stats` and `rpc` folders, this guy should go here
# === @begin: stats/stats_cpp_lib (@original: stats_lib) ===
stats_cpp_lib_dependencies = [
    # Package-managed
    stats_metric_cpp_lib_dep,
    tag_defs_cpp_lib_dep,
    metric_interface_cpp_lib_dep,
    reporter_rpc_cpp_lib_dep,      # alias for metrics_agent_client_cpp_lib_dep
    network_util_cpp_lib_dep,
    size_literals_cpp_lib_dep,

    # Eugo-managed
    grpc,                           # includes grpc_opencensus_plugin

    # System-managed
    threads, # `"-lpthread"` for Linux
]

stats_cpp_lib = static_library(
    'stats_cpp_lib',
    ['stats/metric_exporter.cc'],
    install: false,
    dependencies: stats_cpp_lib_dependencies
)

stats_cpp_lib_dep = declare_dependency(
    link_with: [stats_cpp_lib],
    dependencies: stats_cpp_lib_dependencies
)
# === @end: stats/stats_cpp_lib (@original: stats_lib) ===


subdir('object_manager/')

subdir('object_manager_rpc_client/')

subdir('pubsub/')

# raylet_ipc_client (formerly common/client_connection + new raylet_ipc_client)
subdir('raylet_ipc_client/')

# raylet_rpc_client (formerly raylet_client/)
subdir('raylet_rpc_client/')

subdir('gcs/')

# gcs_rpc_client (formerly gcs/gcs_client/ + rpc/gcs_server/)
subdir('gcs_rpc_client/')

# We can't traverse the entire `raylet` tree, as it includes parts which depend on targets from `gcs`.
subdir('raylet/scheduling/')


# @IMPORTANT: `observability` must come before `gcs_server` because gcs targets use observability types
subdir('observability/')

# @IMPORTANT: `core_worker_rpc_client` must come before `gcs_server` (gcs_server depends on worker_rpc_cpp_lib_dep)
# and before `gcs/actor` (gcs_actor depends on core_worker_client_pool)
subdir('core_worker_rpc_client/')

# @IMPORTANT: `gcs_server` targets are inline here (not in `gcs/meson.build`) because they depend on
# targets from `gcs_rpc_client/`, `raylet/scheduling/`, and others that are processed after `gcs/`.
# === @begin: gcs/gcs_table_storage_cpp_lib (@original: gcs_table_storage) ===
gcs_table_storage_cpp_lib_dependencies = [
    # Package-managed
    asio_cpp_lib_dep,
    id_cpp_lib_dep,
    status_cpp_lib_dep,
    store_client_cpp_lib_dep,
    gcs_proto_cpp_lib_dep,

    # Eugo-managed
    absl_container_flat_hash_map,
    absl_container_flat_hash_set,
]

gcs_table_storage_cpp_lib = static_library(
    'gcs_table_storage_cpp_lib',
    ['gcs/gcs_table_storage.cc'],
    install: false,
    dependencies: gcs_table_storage_cpp_lib_dependencies
)

gcs_table_storage_cpp_lib_dep = declare_dependency(
    link_with: [gcs_table_storage_cpp_lib],
    dependencies: gcs_table_storage_cpp_lib_dependencies
)
# === @end: gcs/gcs_table_storage_cpp_lib (original: gcs_table_storage_lib) ===


# @IMPORTANT: `gcs/actor` must be after `gcs_table_storage` (gcs_actor_scheduler needs it) and
# after `core_worker_rpc_client` (gcs_actor_manager needs worker_rpc_cpp_lib_dep),
# but BEFORE `gcs_server` (gcs_server depends on gcs_actor_cpp_lib_dep).
subdir('gcs/actor/')


# === @begin: gcs/grpc_service_interfaces_cpp_lib (@original: grpc_service_interfaces) ===
# Header-only
gcs_grpc_service_interfaces_cpp_lib_dependencies = [
    # Package-managed
    status_cpp_lib_dep,
    autoscaler_proto_cpp_grpc_lib_dep,
    gcs_service_proto_cpp_grpc_lib_dep,
    rpc_callback_types_cpp_lib_dep,
]

gcs_grpc_service_interfaces_cpp_lib_dep = declare_dependency(
    sources: ['gcs/grpc_service_interfaces.h'],
    dependencies: gcs_grpc_service_interfaces_cpp_lib_dependencies
)
# === @end: gcs/grpc_service_interfaces_cpp_lib ===


# === @begin: gcs/gcs_state_util_cpp_lib (@original: gcs_state_util) ===
gcs_state_util_cpp_lib_dependencies = [
    # Package-managed
    gcs_proto_cpp_lib_dep,

    # Eugo-managed
    absl_container_flat_hash_map,
]

gcs_state_util_cpp_lib = static_library(
    'gcs_state_util_cpp_lib',
    ['gcs/state_util.cc'],
    install: false,
    dependencies: gcs_state_util_cpp_lib_dependencies
)

gcs_state_util_cpp_lib_dep = declare_dependency(
    link_with: [gcs_state_util_cpp_lib],
    dependencies: gcs_state_util_cpp_lib_dependencies
)
# === @end: gcs/gcs_state_util_cpp_lib ===


# === @begin: gcs/gcs_metrics_cpp_lib (@original: gcs:metrics) ===
# Header-only
gcs_metrics_cpp_lib_dependencies = [
    # Package-managed
    observability_metrics_cpp_lib_dep,
    stats_cpp_lib_dep,
]

gcs_metrics_cpp_lib_dep = declare_dependency(
    sources: ['gcs/metrics.h'],
    dependencies: gcs_metrics_cpp_lib_dependencies
)
# === @end: gcs/gcs_metrics_cpp_lib ===


# === @begin: gcs/gcs_kv_manager_cpp_lib (@original: gcs_kv_manager) ===
gcs_kv_manager_cpp_lib_dependencies = [
    # Package-managed
    asio_cpp_lib_dep,
    status_cpp_lib_dep,
    gcs_grpc_service_interfaces_cpp_lib_dep,
    postable_cpp_lib_dep,
    gcs_proto_cpp_lib_dep,
]

gcs_kv_manager_cpp_lib = static_library(
    'gcs_kv_manager_cpp_lib',
    ['gcs/gcs_kv_manager.cc'],
    install: false,
    dependencies: gcs_kv_manager_cpp_lib_dependencies
)

gcs_kv_manager_cpp_lib_dep = declare_dependency(
    link_with: [gcs_kv_manager_cpp_lib],
    dependencies: gcs_kv_manager_cpp_lib_dependencies
)
# === @end: gcs/gcs_kv_manager_cpp_lib ===


# === @begin: gcs/gcs_function_manager_cpp_lib (@original: gcs_function_manager) ===
# Header-only
gcs_function_manager_cpp_lib_dependencies = [
    # Package-managed
    gcs_kv_manager_cpp_lib_dep,
    asio_cpp_lib_dep,
    constants_cpp_lib_dep,
    id_cpp_lib_dep,

    # Eugo-managed
    absl_container_flat_hash_map,
]

gcs_function_manager_cpp_lib_dep = declare_dependency(
    sources: ['gcs/gcs_function_manager.h'],
    dependencies: gcs_function_manager_cpp_lib_dependencies
)
# === @end: gcs/gcs_function_manager_cpp_lib ===


# === @begin: gcs/gcs_usage_stats_client_cpp_lib (@original: gcs_usage_stats_client) ===
gcs_usage_stats_client_cpp_lib_dependencies = [
    # Package-managed
    gcs_kv_manager_cpp_lib_dep,
    asio_cpp_lib_dep,
    usage_proto_cpp_lib_dep,
]

gcs_usage_stats_client_cpp_lib = static_library(
    'gcs_usage_stats_client_cpp_lib',
    ['gcs/usage_stats_client.cc'],
    install: false,
    dependencies: gcs_usage_stats_client_cpp_lib_dependencies
)

gcs_usage_stats_client_cpp_lib_dep = declare_dependency(
    link_with: [gcs_usage_stats_client_cpp_lib],
    dependencies: gcs_usage_stats_client_cpp_lib_dependencies
)
# === @end: gcs/gcs_usage_stats_client_cpp_lib ===


# === @begin: gcs/gcs_store_client_kv_cpp_lib (@original: gcs_store_client_kv) ===
gcs_store_client_kv_cpp_lib_dependencies = [
    # Package-managed
    gcs_kv_manager_cpp_lib_dep,
    postable_cpp_lib_dep,
    store_client_cpp_lib_dep,
]

gcs_store_client_kv_cpp_lib = static_library(
    'gcs_store_client_kv_cpp_lib',
    ['gcs/store_client_kv.cc'],
    install: false,
    dependencies: gcs_store_client_kv_cpp_lib_dependencies
)

gcs_store_client_kv_cpp_lib_dep = declare_dependency(
    link_with: [gcs_store_client_kv_cpp_lib],
    dependencies: gcs_store_client_kv_cpp_lib_dependencies
)
# === @end: gcs/gcs_store_client_kv_cpp_lib ===


# === @begin: gcs/gcs_init_data_cpp_lib (@original: gcs_init_data) ===
gcs_init_data_cpp_lib_dependencies = [
    # Package-managed
    gcs_table_storage_cpp_lib_dep,
    asio_cpp_lib_dep,
    id_cpp_lib_dep,
    postable_cpp_lib_dep,
    gcs_proto_cpp_lib_dep,

    # Eugo-managed
    absl_container_flat_hash_map,
]

gcs_init_data_cpp_lib = static_library(
    'gcs_init_data_cpp_lib',
    ['gcs/gcs_init_data.cc'],
    install: false,
    dependencies: gcs_init_data_cpp_lib_dependencies
)

gcs_init_data_cpp_lib_dep = declare_dependency(
    link_with: [gcs_init_data_cpp_lib],
    dependencies: gcs_init_data_cpp_lib_dependencies
)
# === @end: gcs/gcs_init_data_cpp_lib ===


# === @begin: gcs/gcs_node_manager_cpp_lib (@original: gcs_node_manager) ===
gcs_node_manager_cpp_lib_dependencies = [
    # Package-managed
    gcs_init_data_cpp_lib_dep,
    gcs_table_storage_cpp_lib_dep,
    gcs_grpc_service_interfaces_cpp_lib_dep,
    asio_cpp_lib_dep,
    id_cpp_lib_dep,
    protobuf_utils_cpp_lib_dep,
    ray_config_cpp_lib_dep,
    ray_event_recorder_interface_cpp_lib_dep,
    ray_node_definition_event_cpp_lib_dep,
    ray_node_lifecycle_event_cpp_lib_dep,
    autoscaler_proto_cpp_lib_dep,
    gcs_service_proto_cpp_lib_dep,
    ray_syncer_proto_cpp_lib_dep,
    gcs_publisher_cpp_lib_dep,
    raylet_client_pool_cpp_lib_dep,
    stats_metric_cpp_lib_dep,
    event_cpp_lib_dep,
    logging_cpp_lib_dep,
    time_cpp_lib_dep,

    # Eugo-managed
    absl_container_flat_hash_map,
    absl_container_flat_hash_set,
]

gcs_node_manager_cpp_lib = static_library(
    'gcs_node_manager_cpp_lib',
    ['gcs/gcs_node_manager.cc'],
    install: false,
    dependencies: gcs_node_manager_cpp_lib_dependencies
)

gcs_node_manager_cpp_lib_dep = declare_dependency(
    link_with: [gcs_node_manager_cpp_lib],
    dependencies: gcs_node_manager_cpp_lib_dependencies
)
# === @end: gcs/gcs_node_manager_cpp_lib ===


# === @begin: gcs/gcs_resource_manager_cpp_lib (@original: gcs_resource_manager) ===
gcs_resource_manager_cpp_lib_dependencies = [
    # Package-managed
    gcs_init_data_cpp_lib_dep,
    gcs_node_manager_cpp_lib_dep,
    gcs_state_util_cpp_lib_dep,
    gcs_grpc_service_interfaces_cpp_lib_dep,
    asio_cpp_lib_dep,
    id_cpp_lib_dep,
    ray_config_cpp_lib_dep,
    gcs_service_proto_cpp_lib_dep,
    ray_syncer_proto_cpp_lib_dep,
    ray_syncer_cpp_lib_dep,
    cluster_resource_manager_cpp_lib_dep,
    logging_cpp_lib_dep,

    # Eugo-managed
    absl_container_flat_hash_map,
]

gcs_resource_manager_cpp_lib = static_library(
    'gcs_resource_manager_cpp_lib',
    ['gcs/gcs_resource_manager.cc'],
    install: false,
    dependencies: gcs_resource_manager_cpp_lib_dependencies
)

gcs_resource_manager_cpp_lib_dep = declare_dependency(
    link_with: [gcs_resource_manager_cpp_lib],
    dependencies: gcs_resource_manager_cpp_lib_dependencies
)
# === @end: gcs/gcs_resource_manager_cpp_lib ===


# === @begin: gcs/gcs_pubsub_handler_cpp_lib (@original: gcs_pubsub_handler) ===
gcs_pubsub_handler_cpp_lib_dependencies = [
    # Package-managed
    asio_cpp_lib_dep,
    gcs_grpc_service_interfaces_cpp_lib_dep,
    postable_cpp_lib_dep,
    gcs_service_proto_cpp_lib_dep,
    gcs_publisher_cpp_lib_dep,

    # Eugo-managed
    absl_container_flat_hash_map,
    absl_container_flat_hash_set,
]

gcs_pubsub_handler_cpp_lib = static_library(
    'gcs_pubsub_handler_cpp_lib',
    ['gcs/pubsub_handler.cc'],
    install: false,
    dependencies: gcs_pubsub_handler_cpp_lib_dependencies
)

gcs_pubsub_handler_cpp_lib_dep = declare_dependency(
    link_with: [gcs_pubsub_handler_cpp_lib],
    dependencies: gcs_pubsub_handler_cpp_lib_dependencies
)
# === @end: gcs/gcs_pubsub_handler_cpp_lib ===


# === @begin: gcs/gcs_runtime_env_handler_cpp_lib (@original: gcs_runtime_env_handler) ===
gcs_runtime_env_handler_cpp_lib_dependencies = [
    # Package-managed
    gcs_grpc_service_interfaces_cpp_lib_dep,
    asio_cpp_lib_dep,
    runtime_env_cpp_lib_dep,
    gcs_proto_cpp_lib_dep,
    thread_checker_cpp_lib_dep,

    # Eugo-managed
    boost_asio,
]

gcs_runtime_env_handler_cpp_lib = static_library(
    'gcs_runtime_env_handler_cpp_lib',
    ['gcs/runtime_env_handler.cc'],
    install: false,
    dependencies: gcs_runtime_env_handler_cpp_lib_dependencies
)

gcs_runtime_env_handler_cpp_lib_dep = declare_dependency(
    link_with: [gcs_runtime_env_handler_cpp_lib],
    dependencies: gcs_runtime_env_handler_cpp_lib_dependencies
)
# === @end: gcs/gcs_runtime_env_handler_cpp_lib ===


# === @begin: gcs/gcs_worker_manager_cpp_lib (@original: gcs_worker_manager) ===
gcs_worker_manager_cpp_lib_dependencies = [
    # Package-managed
    gcs_kv_manager_cpp_lib_dep,
    gcs_table_storage_cpp_lib_dep,
    gcs_usage_stats_client_cpp_lib_dep,
    gcs_grpc_service_interfaces_cpp_lib_dep,
    gcs_publisher_cpp_lib_dep,
    stats_metric_cpp_lib_dep,
]

gcs_worker_manager_cpp_lib = static_library(
    'gcs_worker_manager_cpp_lib',
    ['gcs/gcs_worker_manager.cc'],
    install: false,
    dependencies: gcs_worker_manager_cpp_lib_dependencies
)

gcs_worker_manager_cpp_lib_dep = declare_dependency(
    link_with: [gcs_worker_manager_cpp_lib],
    dependencies: gcs_worker_manager_cpp_lib_dependencies
)
# === @end: gcs/gcs_worker_manager_cpp_lib ===


# === @begin: gcs/gcs_health_check_manager_cpp_lib (@original: gcs_health_check_manager) ===
gcs_health_check_manager_cpp_lib_dependencies = [
    # Package-managed
    asio_cpp_lib_dep,
    id_cpp_lib_dep,
    ray_config_cpp_lib_dep,
    stats_metric_cpp_lib_dep,
    thread_checker_cpp_lib_dep,

    # Eugo-managed
    grpc,                           # covers grpc++ and health_proto (bundled in grpc meta-dep)
    absl_container_flat_hash_map,
]

gcs_health_check_manager_cpp_lib = static_library(
    'gcs_health_check_manager_cpp_lib',
    ['gcs/gcs_health_check_manager.cc'],
    install: false,
    dependencies: gcs_health_check_manager_cpp_lib_dependencies
)

gcs_health_check_manager_cpp_lib_dep = declare_dependency(
    link_with: [gcs_health_check_manager_cpp_lib],
    dependencies: gcs_health_check_manager_cpp_lib_dependencies
)
# === @end: gcs/gcs_health_check_manager_cpp_lib ===


# === @begin: gcs/gcs_ray_event_converter_cpp_lib (@original: gcs_ray_event_converter) ===
gcs_ray_event_converter_cpp_lib_dependencies = [
    # Package-managed
    function_descriptor_cpp_lib_dep,
    grpc_util_cpp_lib_dep,
    id_cpp_lib_dep,
    events_event_aggregator_service_proto_cpp_lib_dep,
    gcs_service_proto_cpp_lib_dep,
    logging_cpp_lib_dep,
]

gcs_ray_event_converter_cpp_lib = static_library(
    'gcs_ray_event_converter_cpp_lib',
    ['gcs/gcs_ray_event_converter.cc'],
    install: false,
    dependencies: gcs_ray_event_converter_cpp_lib_dependencies
)

gcs_ray_event_converter_cpp_lib_dep = declare_dependency(
    link_with: [gcs_ray_event_converter_cpp_lib],
    dependencies: gcs_ray_event_converter_cpp_lib_dependencies
)
# === @end: gcs/gcs_ray_event_converter_cpp_lib ===


# === @begin: gcs/gcs_task_manager_cpp_lib (@original: gcs_task_manager) ===
gcs_task_manager_cpp_lib_dependencies = [
    # Package-managed
    gcs_ray_event_converter_cpp_lib_dep,
    gcs_usage_stats_client_cpp_lib_dep,
    gcs_grpc_service_interfaces_cpp_lib_dep,
    asio_cpp_lib_dep,
    id_cpp_lib_dep,
    protobuf_utils_cpp_lib_dep,
    ray_config_cpp_lib_dep,
    status_cpp_lib_dep,
    events_event_aggregator_service_proto_cpp_lib_dep,
    gcs_proto_cpp_lib_dep,
    stats_metric_cpp_lib_dep,
    counter_map_cpp_lib_dep,

    # Eugo-managed
    absl_container_flat_hash_map,
    absl_container_flat_hash_set,
    absl_strings,
    absl_synchronization,
]

gcs_task_manager_cpp_lib = static_library(
    'gcs_task_manager_cpp_lib',
    ['gcs/gcs_task_manager.cc'],
    install: false,
    dependencies: gcs_task_manager_cpp_lib_dependencies
)

gcs_task_manager_cpp_lib_dep = declare_dependency(
    link_with: [gcs_task_manager_cpp_lib],
    dependencies: gcs_task_manager_cpp_lib_dependencies
)
# === @end: gcs/gcs_task_manager_cpp_lib ===


# === @begin: gcs/gcs_server_io_context_policy_cpp_lib (@original: gcs_server_io_context_policy) ===
# Header-only
gcs_server_io_context_policy_cpp_lib_dependencies = [
    # Package-managed
    gcs_task_manager_cpp_lib_dep,
    ray_event_recorder_cpp_lib_dep,
    gcs_publisher_cpp_lib_dep,
    ray_syncer_cpp_lib_dep,
    array_cpp_lib_dep,
    type_traits_cpp_lib_dep,
]

gcs_server_io_context_policy_cpp_lib_dep = declare_dependency(
    sources: ['gcs/gcs_server_io_context_policy.h'],
    dependencies: gcs_server_io_context_policy_cpp_lib_dependencies
)
# === @end: gcs/gcs_server_io_context_policy_cpp_lib ===


# === @begin: gcs/gcs_job_manager_cpp_lib (@original: gcs_job_manager) ===
gcs_job_manager_cpp_lib_dependencies = [
    # Package-managed
    gcs_function_manager_cpp_lib_dep,
    gcs_init_data_cpp_lib_dep,
    gcs_table_storage_cpp_lib_dep,
    gcs_grpc_service_interfaces_cpp_lib_dep,
    protobuf_utils_cpp_lib_dep,
    runtime_env_cpp_lib_dep,
    core_worker_client_pool_cpp_lib_dep,
    metric_interface_cpp_lib_dep,
    ray_driver_job_definition_event_cpp_lib_dep,
    ray_driver_job_lifecycle_event_cpp_lib_dep,
    ray_event_recorder_interface_cpp_lib_dep,
    gcs_publisher_cpp_lib_dep,
    stats_metric_cpp_lib_dep,
    event_cpp_lib_dep,
    thread_checker_cpp_lib_dep,

    # Eugo-managed
    absl_container_flat_hash_map,
]

gcs_job_manager_cpp_lib = static_library(
    'gcs_job_manager_cpp_lib',
    ['gcs/gcs_job_manager.cc'],
    install: false,
    dependencies: gcs_job_manager_cpp_lib_dependencies
)

gcs_job_manager_cpp_lib_dep = declare_dependency(
    link_with: [gcs_job_manager_cpp_lib],
    dependencies: gcs_job_manager_cpp_lib_dependencies
)
# === @end: gcs/gcs_job_manager_cpp_lib ===


# === @begin: gcs/gcs_placement_group_cpp_lib (@original: gcs_placement_group) ===
gcs_placement_group_cpp_lib_dependencies = [
    # Package-managed
    bundle_spec_cpp_lib_dep,
    id_cpp_lib_dep,
    metrics_cpp_lib_dep,
    gcs_service_proto_cpp_lib_dep,
    stats_cpp_lib_dep,
    counter_map_cpp_lib_dep,
    time_cpp_lib_dep,
]

gcs_placement_group_cpp_lib = static_library(
    'gcs_placement_group_cpp_lib',
    ['gcs/gcs_placement_group.cc'],
    install: false,
    dependencies: gcs_placement_group_cpp_lib_dependencies
)

gcs_placement_group_cpp_lib_dep = declare_dependency(
    link_with: [gcs_placement_group_cpp_lib],
    dependencies: gcs_placement_group_cpp_lib_dependencies
)
# === @end: gcs/gcs_placement_group_cpp_lib ===


# === @begin: gcs/gcs_placement_group_scheduler_cpp_lib (@original: gcs_placement_group_scheduler) ===
gcs_placement_group_scheduler_cpp_lib_dependencies = [
    # Package-managed
    gcs_node_manager_cpp_lib_dep,
    gcs_placement_group_cpp_lib_dep,
    gcs_table_storage_cpp_lib_dep,
    asio_cpp_lib_dep,
    id_cpp_lib_dep,
    cluster_resource_scheduler_cpp_lib_dep,
    scheduling_context_cpp_lib_dep,
    raylet_client_interface_cpp_lib_dep,

    # Eugo-managed
    absl_container_flat_hash_map,
    absl_container_flat_hash_set,
]

gcs_placement_group_scheduler_cpp_lib = static_library(
    'gcs_placement_group_scheduler_cpp_lib',
    ['gcs/gcs_placement_group_scheduler.cc'],
    install: false,
    dependencies: gcs_placement_group_scheduler_cpp_lib_dependencies
)

gcs_placement_group_scheduler_cpp_lib_dep = declare_dependency(
    link_with: [gcs_placement_group_scheduler_cpp_lib],
    dependencies: gcs_placement_group_scheduler_cpp_lib_dependencies
)
# === @end: gcs/gcs_placement_group_scheduler_cpp_lib ===


# === @begin: gcs/gcs_placement_group_manager_cpp_lib (@original: gcs_placement_group_manager) ===
gcs_placement_group_manager_cpp_lib_dependencies = [
    # Package-managed
    gcs_init_data_cpp_lib_dep,
    gcs_placement_group_cpp_lib_dep,
    gcs_placement_group_scheduler_cpp_lib_dep,
    gcs_resource_manager_cpp_lib_dep,
    gcs_table_storage_cpp_lib_dep,
    gcs_usage_stats_client_cpp_lib_dep,
    gcs_grpc_service_interfaces_cpp_lib_dep,
    asio_cpp_lib_dep,
    bundle_spec_cpp_lib_dep,
    id_cpp_lib_dep,
    ray_config_cpp_lib_dep,
    gcs_proto_cpp_lib_dep,
    stats_cpp_lib_dep,
    counter_map_cpp_lib_dep,
    exponential_backoff_cpp_lib_dep,

    # Eugo-managed
    absl_container_flat_hash_map,
]

gcs_placement_group_manager_cpp_lib = static_library(
    'gcs_placement_group_manager_cpp_lib',
    ['gcs/gcs_placement_group_manager.cc'],
    install: false,
    dependencies: gcs_placement_group_manager_cpp_lib_dependencies
)

gcs_placement_group_manager_cpp_lib_dep = declare_dependency(
    link_with: [gcs_placement_group_manager_cpp_lib],
    dependencies: gcs_placement_group_manager_cpp_lib_dependencies
)
# === @end: gcs/gcs_placement_group_manager_cpp_lib ===


# === @begin: gcs/gcs_autoscaler_state_manager_cpp_lib (@original: gcs_autoscaler_state_manager) ===
# NOTE: Bazel dep on gtest — per AGENTS.md §10, gtest is NOT added.
# Vendored empty gtest.h stub handles accidental includes.
gcs_autoscaler_state_manager_cpp_lib_dependencies = [
    # Package-managed
    gcs_init_data_cpp_lib_dep,
    gcs_kv_manager_cpp_lib_dep,
    gcs_node_manager_cpp_lib_dep,
    gcs_placement_group_manager_cpp_lib_dep,
    gcs_state_util_cpp_lib_dep,
    gcs_grpc_service_interfaces_cpp_lib_dep,
    asio_cpp_lib_dep,
    id_cpp_lib_dep,
    protobuf_utils_cpp_lib_dep,
    ray_config_cpp_lib_dep,
    gcs_actor_manager_cpp_lib_dep,
    gcs_proto_cpp_lib_dep,
    gcs_publisher_cpp_lib_dep,
    logging_cpp_lib_dep,
    string_utils_cpp_lib_dep,
    thread_checker_cpp_lib_dep,
    time_cpp_lib_dep,

    # Eugo-managed
    absl_container_flat_hash_map,
    # "@com_google_googletest//:gtest",
]

gcs_autoscaler_state_manager_cpp_lib = static_library(
    'gcs_autoscaler_state_manager_cpp_lib',
    ['gcs/gcs_autoscaler_state_manager.cc'],
    install: false,
    dependencies: gcs_autoscaler_state_manager_cpp_lib_dependencies
)

gcs_autoscaler_state_manager_cpp_lib_dep = declare_dependency(
    link_with: [gcs_autoscaler_state_manager_cpp_lib],
    dependencies: gcs_autoscaler_state_manager_cpp_lib_dependencies
)
# === @end: gcs/gcs_autoscaler_state_manager_cpp_lib ===


# === @begin: gcs/grpc_services_cpp_lib (@original: grpc_services) ===
gcs_grpc_services_cpp_lib_dependencies = [
    # Package-managed
    gcs_grpc_service_interfaces_cpp_lib_dep,
    asio_cpp_lib_dep,
    id_cpp_lib_dep,
    autoscaler_proto_cpp_grpc_lib_dep,
    gcs_service_proto_cpp_grpc_lib_dep,
    grpc_server_cpp_lib_dep,
    rpc_callback_types_cpp_lib_dep,
    authentication_token_cpp_lib_dep,

    # Eugo-managed
    grpc,
]

gcs_grpc_services_cpp_lib = static_library(
    'gcs_grpc_services_cpp_lib',
    ['gcs/grpc_services.cc'],
    install: false,
    dependencies: gcs_grpc_services_cpp_lib_dependencies
)

gcs_grpc_services_cpp_lib_dep = declare_dependency(
    link_with: [gcs_grpc_services_cpp_lib],
    dependencies: gcs_grpc_services_cpp_lib_dependencies
)
# === @end: gcs/grpc_services_cpp_lib ===


# === @begin: gcs/gcs_server_cpp_lib (@original: gcs_server_lib) ===
gcs_server_cpp_lib_dependencies = [
    # Package-managed
    gcs_actor_cpp_lib_dep,                          # //src/ray/gcs/actor:gcs_actor
    gcs_actor_manager_cpp_lib_dep,                  # //src/ray/gcs/actor:gcs_actor_manager
    gcs_actor_scheduler_cpp_lib_dep,                # //src/ray/gcs/actor:gcs_actor_scheduler
    gcs_autoscaler_state_manager_cpp_lib_dep,
    gcs_function_manager_cpp_lib_dep,
    gcs_health_check_manager_cpp_lib_dep,
    gcs_init_data_cpp_lib_dep,
    gcs_job_manager_cpp_lib_dep,
    gcs_kv_manager_cpp_lib_dep,
    gcs_node_manager_cpp_lib_dep,
    gcs_placement_group_cpp_lib_dep,
    gcs_placement_group_manager_cpp_lib_dep,
    gcs_placement_group_scheduler_cpp_lib_dep,
    gcs_pubsub_handler_cpp_lib_dep,
    gcs_resource_manager_cpp_lib_dep,
    gcs_runtime_env_handler_cpp_lib_dep,
    gcs_server_io_context_policy_cpp_lib_dep,
    gcs_state_util_cpp_lib_dep,
    gcs_store_client_kv_cpp_lib_dep,
    gcs_table_storage_cpp_lib_dep,
    gcs_task_manager_cpp_lib_dep,
    gcs_usage_stats_client_cpp_lib_dep,
    gcs_worker_manager_cpp_lib_dep,
    gcs_grpc_service_interfaces_cpp_lib_dep,        # :grpc_service_interfaces
    gcs_grpc_services_cpp_lib_dep,                  # :grpc_services
    gcs_metrics_cpp_lib_dep,                        # :metrics
    core_worker_client_cpp_lib_dep,
    core_worker_client_pool_cpp_lib_dep,
    store_client_cpp_lib_dep,
    gcs_in_memory_store_client_cpp_lib_dep,
    observable_store_client_cpp_lib_dep,
    redis_store_client_cpp_lib_dep,
    metric_constants_cpp_lib_dep,
    autoscaler_proto_cpp_grpc_lib_dep,
    gcs_service_proto_cpp_grpc_lib_dep,
    gcs_publisher_cpp_lib_dep,
    publisher_cpp_lib_dep,
    scheduler_cpp_lib_dep,
    raylet_client_lib_cpp_lib_dep,
    raylet_client_pool_cpp_lib_dep,
    grpc_server_cpp_lib_dep,
    metrics_agent_client_cpp_lib_dep,
    authentication_token_loader_cpp_lib_dep,
    counter_map_cpp_lib_dep,
    exponential_backoff_cpp_lib_dep,
    network_util_cpp_lib_dep,
    thread_checker_cpp_lib_dep,
    time_cpp_lib_dep,
    type_traits_cpp_lib_dep,

    # Eugo-managed
    boost_bimap,
    absl_container_btree,
]

gcs_server_cpp_lib = static_library(
    'gcs_server_cpp_lib',
    ['gcs/gcs_server.cc'],
    install: false,
    dependencies: gcs_server_cpp_lib_dependencies
)

gcs_server_cpp_lib_dep = declare_dependency(
    link_with: [gcs_server_cpp_lib],
    dependencies: gcs_server_cpp_lib_dependencies
)
# === @end: gcs/gcs_server_cpp_lib ===


# === @begin: gcs/gcs_server ===
gcs_server_exe_dependencies = [
    # Package-managed
    gcs_server_cpp_lib_dep,
    metrics_cpp_lib_dep,                    # //src/ray/common:metrics
    observability_metrics_cpp_lib_dep,      # //src/ray/observability:metrics
    # @IMPORTANT: Cannot add `raylet_metrics_cpp_lib_dep` here (//src/ray/raylet:metrics) — it maps to
    # `//src/ray/raylet:metrics` but `subdir('raylet/')` is processed AFTER this
    # executable. `gcs_server_main.cc` includes "ray/raylet/metrics.h", which
    # is found via the project-wide `-I` path. The concrete deps of that header-only
    # target (`observability_metrics_cpp_lib_dep`, `stats_cpp_lib_dep`) are present
    # in this list, so link resolution is complete.
    stats_cpp_lib_dep,              # `//src/ray/stats:stats_lib`
    event_cpp_lib_dep,
    port_persistence_cpp_lib_dep,
    raii_cpp_lib_dep,
    stream_redirection_cpp_lib_dep,
    stream_redirection_options_cpp_lib_dep,

    # Eugo-managed
    gflags,
]

gcs_server = executable(
    'gcs_server',
    ['gcs/gcs_server_main.cc'],
    dependencies: gcs_server_exe_dependencies,
    install: true,
    install_dir: py.get_install_dir() / 'ray/core/src/ray/gcs/'
)
# === @end: gcs/gcs_server ===

subdir('core_worker/')

# @IMPORTANT: due to circular dependency between `object_manager` and `gcs` folders, this guy should go here
# NOTE: ownership_object_directory, pull_manager, and object_manager compile here because
# they depend on core_worker_rpc_client, gcs_rpc_client, and pubsub targets that are processed
# after object_manager/. Sub-targets without ordering issues are in object_manager/meson.build.

# === @begin: object_manager/ownership_object_directory_cpp_lib (@original: ownership_object_directory) ===
ownership_object_directory_cpp_lib_dependencies = [
    # Package-managed
    object_directory_cpp_lib_dep,
    asio_cpp_lib_dep,
    id_cpp_lib_dep,
    core_worker_client_pool_cpp_lib_dep,
    gcs_client_cpp_lib_dep,
    subscriber_interface_cpp_lib_dep,

    # Eugo-managed
    absl_container_flat_hash_map,
]

ownership_object_directory_cpp_lib = static_library(
    'ownership_object_directory_cpp_lib',
    ['object_manager/ownership_object_directory.cc'],
    install: false,
    dependencies: ownership_object_directory_cpp_lib_dependencies
)

ownership_object_directory_cpp_lib_dep = declare_dependency(
    link_with: [ownership_object_directory_cpp_lib],
    dependencies: ownership_object_directory_cpp_lib_dependencies
)
# === @end: object_manager/ownership_object_directory_cpp_lib (@original: ownership_object_directory) ===


# === @begin: object_manager/pull_manager_cpp_lib (@original: pull_manager) ===
pull_manager_cpp_lib_dependencies = [
    # Package-managed
    object_manager_metrics_cpp_lib_dep,
    object_manager_common_cpp_lib_dep,
    ownership_object_directory_cpp_lib_dep,
    id_cpp_lib_dep,
    ray_config_cpp_lib_dep,
    ray_object_cpp_lib_dep,
    status_cpp_lib_dep,
    stats_metric_cpp_lib_dep,
    container_util_cpp_lib_dep,
    counter_map_cpp_lib_dep,

    # Eugo-managed
    boost_asio,
    boost_bind,
    absl_container_flat_hash_map,
    absl_container_flat_hash_set,
    absl_time,
]

pull_manager_cpp_lib = static_library(
    'pull_manager_cpp_lib',
    ['object_manager/pull_manager.cc'],
    install: false,
    dependencies: pull_manager_cpp_lib_dependencies
)

pull_manager_cpp_lib_dep = declare_dependency(
    link_with: [pull_manager_cpp_lib],
    dependencies: pull_manager_cpp_lib_dependencies
)
# === @end: object_manager/pull_manager_cpp_lib (@original: pull_manager) ===


# === @begin: object_manager/object_manager_cpp_lib (@original: object_manager) ===
object_manager_cpp_lib_dependencies = [
    # Package-managed
    chunk_object_reader_cpp_lib_dep,
    object_manager_metrics_cpp_lib_dep,
    object_buffer_pool_cpp_lib_dep,
    object_directory_cpp_lib_dep,
    object_manager_common_cpp_lib_dep,
    pull_manager_cpp_lib_dep,
    push_manager_cpp_lib_dep,
    spilled_object_reader_cpp_lib_dep,
    asio_cpp_lib_dep,
    id_cpp_lib_dep,
    ray_config_cpp_lib_dep,
    status_cpp_lib_dep,
    plasma_store_server_cpp_lib_dep,
    object_manager_client_interface_cpp_lib_dep,
    common_proto_cpp_lib_dep,
    node_manager_proto_cpp_lib_dep,
    object_manager_rpc_cpp_lib_dep, # Alias for `object_manager_server_cpp_lib_dep`

    # Eugo-managed
    absl_container_flat_hash_map,
]

object_manager_cpp_lib = static_library(
    'object_manager_cpp_lib',
    ['object_manager/object_manager.cc'],
    install: false,
    dependencies: object_manager_cpp_lib_dependencies
)

object_manager_cpp_lib_dep = declare_dependency(
    link_with: [object_manager_cpp_lib],
    dependencies: object_manager_cpp_lib_dependencies
)
# === @end: object_manager/object_manager_cpp_lib (@original: object_manager) ===

subdir('raylet/')

subdir('internal/')