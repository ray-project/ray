# === @begin: gcs_actor_cpp_lib (@original: gcs_actor) ===
gcs_actor_cpp_lib_dependencies = [
    id_cpp_lib_dep,
    lease_cpp_lib_dep,
    task_common_cpp_lib_dep,
    cluster_resource_data_cpp_lib_dep,
    label_selector_cpp_lib_dep,
    ray_actor_definition_event_cpp_lib_dep,
    ray_actor_lifecycle_event_cpp_lib_dep,
    ray_event_recorder_interface_cpp_lib_dep,
    core_worker_proto_cpp_lib_dep,
    export_event_proto_cpp_lib_dep,
    gcs_service_proto_cpp_lib_dep,
    counter_map_cpp_lib_dep,
    event_cpp_lib_dep,
    logging_cpp_lib_dep,
]

gcs_actor_cpp_lib = static_library(
    'gcs_actor_cpp_lib',
    ['gcs_actor.cc'],
    install: false,
    dependencies: gcs_actor_cpp_lib_dependencies
)

gcs_actor_cpp_lib_dep = declare_dependency(
    link_with: [gcs_actor_cpp_lib],
    dependencies: gcs_actor_cpp_lib_dependencies
)
# === @end: gcs_actor_cpp_lib ===


# === @begin: gcs_actor_scheduler_cpp_lib (@original: gcs_actor_scheduler) ===
# NOTE: Bazel dep on gtest — per AGENTS.md §10, gtest is NOT added.
# Vendored empty gtest.h stub handles FRIEND_TEST.
gcs_actor_scheduler_cpp_lib_dependencies = [
    gcs_actor_cpp_lib_dep,
    asio_cpp_lib_dep,
    id_cpp_lib_dep,
    ray_config_cpp_lib_dep,
    core_worker_client_pool_cpp_lib_dep,
    gcs_node_manager_cpp_lib_dep,
    gcs_table_storage_cpp_lib_dep,
    raylet_client_interface_cpp_lib_dep,
    raylet_client_pool_cpp_lib_dep,
    logging_cpp_lib_dep,
    time_cpp_lib_dep,
    absl_container_flat_hash_map,
    absl_container_flat_hash_set,
]

gcs_actor_scheduler_cpp_lib = static_library(
    'gcs_actor_scheduler_cpp_lib',
    ['gcs_actor_scheduler.cc'],
    install: false,
    dependencies: gcs_actor_scheduler_cpp_lib_dependencies
)

gcs_actor_scheduler_cpp_lib_dep = declare_dependency(
    link_with: [gcs_actor_scheduler_cpp_lib],
    dependencies: gcs_actor_scheduler_cpp_lib_dependencies
)
# === @end: gcs_actor_scheduler_cpp_lib ===


# === @begin: gcs_actor_manager_cpp_lib (@original: gcs_actor_manager) ===
# NOTE: Bazel dep on gtest — per AGENTS.md §10, gtest is NOT added.
# Vendored empty gtest.h stub handles FRIEND_TEST.
gcs_actor_manager_cpp_lib_dependencies = [
    gcs_actor_cpp_lib_dep,
    gcs_actor_scheduler_cpp_lib_dep,
    asio_cpp_lib_dep,
    id_cpp_lib_dep,
    protobuf_utils_cpp_lib_dep,
    ray_config_cpp_lib_dep,
    task_common_cpp_lib_dep,
    core_worker_client_interface_cpp_lib_dep,
    core_worker_client_pool_cpp_lib_dep,
    gcs_function_manager_cpp_lib_dep,
    gcs_init_data_cpp_lib_dep,
    gcs_table_storage_cpp_lib_dep,
    gcs_usage_stats_client_cpp_lib_dep,
    gcs_grpc_service_interfaces_cpp_lib_dep,
    ray_event_recorder_interface_cpp_lib_dep,
    gcs_service_proto_cpp_lib_dep,
    gcs_publisher_cpp_lib_dep,
    stats_cpp_lib_dep,
    container_util_cpp_lib_dep,
    counter_map_cpp_lib_dep,
    logging_cpp_lib_dep,
    thread_checker_cpp_lib_dep,
    time_cpp_lib_dep,
    absl_container_flat_hash_map,
    absl_container_flat_hash_set,
]

gcs_actor_manager_cpp_lib = static_library(
    'gcs_actor_manager_cpp_lib',
    ['gcs_actor_manager.cc'],
    install: false,
    dependencies: gcs_actor_manager_cpp_lib_dependencies
)

gcs_actor_manager_cpp_lib_dep = declare_dependency(
    link_with: [gcs_actor_manager_cpp_lib],
    dependencies: gcs_actor_manager_cpp_lib_dependencies
)
# === @end: gcs_actor_manager_cpp_lib ===
