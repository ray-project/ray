# =====================================================================
# core_worker/
#
# IMPORTANT: 1. Ordering within this file respects the internal dependency graph.
# You will also see things "out of order" from a BUILD.bazel perspective,
# but this is intentional: i.e. `cw_metrics_cpp_lib_dep` is defined at the top
# of the file because it's a leaf node dependency used by many other targets,
# not because it's defined first in BUILD.bazel (in bazel its definition is
# after all the targets that depend on it).
# IMPORTANT: 2. `actor_manager` is defined inline here (not in `actor_management/`)
# because it depends on `task_submission:actor_task_submitter`, which
# in turn depends on `actor_management:actor_creator` — a circular dep.
# =====================================================================


# === @begin: cw_metrics_cpp_lib (@original: metrics) ===
# Header-only
cw_metrics_cpp_lib_dependencies = [
    stats_metric_cpp_lib_dep
]

cw_metrics_cpp_lib_dep = declare_dependency(
    sources: ['metrics.h'],
    dependencies: cw_metrics_cpp_lib_dependencies
)
# === @end: cw_metrics_cpp_lib ===


# === @begin: cw_common_cpp_lib (@original: core_worker:common) ===
cw_common_cpp_lib_dependencies = [
    id_cpp_lib_dep,
    ray_object_cpp_lib_dep,
    task_common_cpp_lib_dep,
    process_cpp_lib_dep,
    process_utils_cpp_lib_dep,
]

cw_common_cpp_lib = static_library(
    'cw_common_cpp_lib',
    ['common.cc'],
    install: false,
    dependencies: cw_common_cpp_lib_dependencies
)

cw_common_cpp_lib_dep = declare_dependency(
    link_with: [cw_common_cpp_lib],
    dependencies: cw_common_cpp_lib_dependencies
)
# === @end: cw_common_cpp_lib ===


# === @begin: cw_grpc_service_cpp_lib (@original: grpc_service) ===
cw_grpc_service_cpp_lib_dependencies = [
    asio_cpp_lib_dep,
    core_worker_proto_cpp_grpc_lib_dep,
    core_worker_proto_cpp_lib_dep,
    grpc_server_cpp_lib_dep,
    rpc_callback_types_cpp_lib_dep,
    authentication_token_cpp_lib_dep,
]

cw_grpc_service_cpp_lib = static_library(
    'cw_grpc_service_cpp_lib',
    ['grpc_service.cc'],
    install: false,
    dependencies: cw_grpc_service_cpp_lib_dependencies
)

cw_grpc_service_cpp_lib_dep = declare_dependency(
    link_with: [cw_grpc_service_cpp_lib],
    dependencies: cw_grpc_service_cpp_lib_dependencies
)
# === @end: cw_grpc_service_cpp_lib ===


# === @begin: cw_shutdown_coordinator_cpp_lib (@original: shutdown_coordinator) ===
cw_shutdown_coordinator_cpp_lib_dependencies = [
    buffer_cpp_lib_dep,     # buffer (consolidated)
    common_proto_cpp_lib_dep,
]

cw_shutdown_coordinator_cpp_lib = static_library(
    'cw_shutdown_coordinator_cpp_lib',
    ['shutdown_coordinator.cc'],
    install: false,
    dependencies: cw_shutdown_coordinator_cpp_lib_dependencies
)

cw_shutdown_coordinator_cpp_lib_dep = declare_dependency(
    link_with: [cw_shutdown_coordinator_cpp_lib],
    dependencies: cw_shutdown_coordinator_cpp_lib_dependencies
)
# === @end: cw_shutdown_coordinator_cpp_lib ===


# === @begin: core_worker_context_cpp_lib (@original: core_worker_context) ===
core_worker_context_cpp_lib_dependencies = [
    cw_common_cpp_lib_dep,
    runtime_env_cpp_lib_dep,
    task_common_cpp_lib_dep,
    boost_thread,
    absl_base_core_headers,
    absl_synchronization,
    nlohmann_json,
]

core_worker_context_cpp_lib = static_library(
    'core_worker_context_cpp_lib',
    ['context.cc'],
    install: false,
    dependencies: core_worker_context_cpp_lib_dependencies
)

core_worker_context_cpp_lib_dep = declare_dependency(
    link_with: [core_worker_context_cpp_lib],
    dependencies: core_worker_context_cpp_lib_dependencies
)
# === @end: core_worker_context_cpp_lib ===


# === @begin: cw_lease_policy_cpp_lib (@original: lease_policy) ===
cw_lease_policy_cpp_lib_dependencies = [
    id_cpp_lib_dep,
    lease_cpp_lib_dep,
    absl_base_core_headers,
    absl_container_flat_hash_map,
    absl_container_flat_hash_set,
]

cw_lease_policy_cpp_lib = static_library(
    'cw_lease_policy_cpp_lib',
    ['lease_policy.cc'],
    install: false,
    dependencies: cw_lease_policy_cpp_lib_dependencies
)

cw_lease_policy_cpp_lib_dep = declare_dependency(
    link_with: [cw_lease_policy_cpp_lib],
    dependencies: cw_lease_policy_cpp_lib_dependencies
)
# === @end: cw_lease_policy_cpp_lib ===


# === @begin: cw_options_cpp_lib (@original: core_worker_options) ===
# Header-only
cw_options_cpp_lib_dependencies = [
        cw_common_cpp_lib_dep,
        id_cpp_lib_dep,
        ray_object_cpp_lib_dep,
        status_cpp_lib_dep,
        gcs_client_cpp_lib_dep,
        process_cpp_lib_dep,
]
cw_options_cpp_lib_dep = declare_dependency(
    sources: ['core_worker_options.h'],
    dependencies: cw_options_cpp_lib_dependencies
)
# === @end: cw_options_cpp_lib ===


# === @begin: cw_reference_counter_interface_cpp_lib (@original: reference_counter_interface) ===
# Header-only
cw_reference_counter_interface_cpp_lib_dependencies = [
        id_cpp_lib_dep,
        cw_lease_policy_cpp_lib_dep,
        publisher_interface_cpp_lib_dep,
        subscriber_interface_cpp_lib_dep,
        rpc_utils_cpp_lib_dep,
        absl_base_core_headers,
        absl_synchronization,
]

cw_reference_counter_interface_cpp_lib_dep = declare_dependency(
    sources: ['reference_counter_interface.h'],
    dependencies: cw_reference_counter_interface_cpp_lib_dependencies
)
# === @end: cw_reference_counter_interface_cpp_lib ===


# === @begin: cw_generator_waiter_cpp_lib (@original: generator_waiter) ===
cw_generator_waiter_cpp_lib_dependencies = [
    # Package-managed
    cw_common_cpp_lib_dep,

    # Eugo-managed
    absl_synchronization,
]

cw_generator_waiter_cpp_lib = static_library(
    'cw_generator_waiter_cpp_lib',
    ['generator_waiter.cc'],
    install: false,
    dependencies: cw_generator_waiter_cpp_lib_dependencies
)

cw_generator_waiter_cpp_lib_dep = declare_dependency(
    link_with: [cw_generator_waiter_cpp_lib],
    dependencies: cw_generator_waiter_cpp_lib_dependencies
)
# === @end: cw_generator_waiter_cpp_lib ===


# === @begin: cw_exp_mutable_object_manager_cpp_lib (@original: experimental_mutable_object_manager) ===
cw_exp_mutable_object_manager_cpp_lib_dependencies = [
    # Package-managed
    ray_config_cpp_lib_dep,
    ray_object_cpp_lib_dep,
    status_cpp_lib_dep,
    object_manager_common_cpp_lib_dep,
    plasma_client_cpp_lib_dep,
    time_cpp_lib_dep,

    # Eugo-managed
    absl_container_node_hash_map,
    absl_strings,
    # "@com_google_googletest//:gtest_prod",
]

cw_exp_mutable_object_manager_cpp_lib = static_library(
    'cw_exp_mutable_object_manager_cpp_lib',
    ['experimental_mutable_object_manager.cc'],
    install: false,
    dependencies: cw_exp_mutable_object_manager_cpp_lib_dependencies
)

cw_exp_mutable_object_manager_cpp_lib_dep = declare_dependency(
    link_with: [cw_exp_mutable_object_manager_cpp_lib],
    dependencies: cw_exp_mutable_object_manager_cpp_lib_dependencies
)
# === @end: cw_exp_mutable_object_manager_cpp_lib ===


# === @begin: cw_task_event_buffer_cpp_lib (@original: task_event_buffer) ===
cw_task_event_buffer_cpp_lib_dependencies = [
    asio_cpp_lib_dep,
    id_cpp_lib_dep,
    protobuf_utils_cpp_lib_dep,
    task_common_cpp_lib_dep,
    gcs_client_cpp_lib_dep,
    export_task_event_proto_cpp_lib_dep,
    gcs_proto_cpp_lib_dep,
    event_aggregator_client_cpp_lib_dep,
    counter_map_cpp_lib_dep,
    event_cpp_lib_dep,
    graceful_shutdown_cpp_lib_dep,
    boost_circular_buffer,
    absl_base_core_headers,
    absl_synchronization,
    absl_types_optional,
]

cw_task_event_buffer_cpp_lib = static_library(
    'cw_task_event_buffer_cpp_lib',
    ['task_event_buffer.cc'],
    install: false,
    dependencies: cw_task_event_buffer_cpp_lib_dependencies
)

cw_task_event_buffer_cpp_lib_dep = declare_dependency(
    link_with: [cw_task_event_buffer_cpp_lib],
    dependencies: cw_task_event_buffer_cpp_lib_dependencies
)
# === @end: cw_task_event_buffer_cpp_lib ===


# === @begin: cw_memory_store_cpp_lib (@original: memory_store) ===
cw_memory_store_cpp_lib_dependencies = [
    core_worker_context_cpp_lib_dep,
    asio_cpp_lib_dep,
    id_cpp_lib_dep,
    metrics_cpp_lib_dep,
    ray_config_cpp_lib_dep,
    status_cpp_lib_dep,
    raylet_ipc_client_interface_cpp_lib_dep,
    rpc_utils_cpp_lib_dep,
    stats_metric_cpp_lib_dep,
    absl_container_flat_hash_map,
    absl_container_flat_hash_set,
    absl_synchronization,
]

cw_memory_store_cpp_lib = static_library(
    'cw_memory_store_cpp_lib',
    ['store_provider/memory_store/memory_store.cc'],
    install: false,
    dependencies: cw_memory_store_cpp_lib_dependencies
)

cw_memory_store_cpp_lib_dep = declare_dependency(
    link_with: [cw_memory_store_cpp_lib],
    dependencies: cw_memory_store_cpp_lib_dependencies
)
# === @end: cw_memory_store_cpp_lib ===


# === @begin: cw_exp_mutable_object_provider_cpp_lib (@original: experimental_mutable_object_provider) ===
cw_exp_mutable_object_provider_cpp_lib_dependencies = [
    cw_exp_mutable_object_manager_cpp_lib_dep,
    asio_cpp_lib_dep,
    raylet_client_interface_cpp_lib_dep,
    client_call_cpp_lib_dep,
]

cw_exp_mutable_object_provider_cpp_lib = static_library(
    'cw_exp_mutable_object_provider_cpp_lib',
    ['experimental_mutable_object_provider.cc'],
    install: false,
    dependencies: cw_exp_mutable_object_provider_cpp_lib_dependencies
)

cw_exp_mutable_object_provider_cpp_lib_dep = declare_dependency(
    link_with: [cw_exp_mutable_object_provider_cpp_lib],
    dependencies: cw_exp_mutable_object_provider_cpp_lib_dependencies
)
# === @end: cw_exp_mutable_object_provider_cpp_lib ===


# === @begin: cw_profile_event_cpp_lib (@original: profile_event) ===
cw_profile_event_cpp_lib_dependencies = [
    # Package-managed
    core_worker_context_cpp_lib_dep,
    cw_task_event_buffer_cpp_lib_dep,

    # Eugo-managed
    absl_time,
]

cw_profile_event_cpp_lib = static_library(
    'cw_profile_event_cpp_lib',
    ['profile_event.cc'],
    install: false,
    dependencies: cw_profile_event_cpp_lib_dependencies
)

cw_profile_event_cpp_lib_dep = declare_dependency(
    link_with: [cw_profile_event_cpp_lib],
    dependencies: cw_profile_event_cpp_lib_dependencies
)
# === @end: cw_profile_event_cpp_lib ===


# === @begin: cw_plasma_store_provider_cpp_lib (@original: plasma_store_provider) ===
cw_plasma_store_provider_cpp_lib_dependencies = [
    # Package-managed
    cw_common_cpp_lib_dep,
    core_worker_context_cpp_lib_dep,
    buffer_cpp_lib_dep,
    id_cpp_lib_dep,
    ray_config_cpp_lib_dep,
    status_cpp_lib_dep,
    plasma_client_cpp_lib_dep,
    common_proto_cpp_lib_dep,
    raylet_ipc_client_interface_cpp_lib_dep,
    time_cpp_lib_dep,

    # Eugo-managed
    absl_container_flat_hash_map,
    absl_container_flat_hash_set,
]

cw_plasma_store_provider_cpp_lib = static_library(
    'cw_plasma_store_provider_cpp_lib',
    ['store_provider/plasma_store_provider.cc'],
    install: false,
    dependencies: cw_plasma_store_provider_cpp_lib_dependencies
)

cw_plasma_store_provider_cpp_lib_dep = declare_dependency(
    link_with: [cw_plasma_store_provider_cpp_lib],
    dependencies: cw_plasma_store_provider_cpp_lib_dependencies
)
# === @end: cw_plasma_store_provider_cpp_lib ===


# === @begin: cw_reference_counter_cpp_lib (@original: reference_counter) ===
cw_reference_counter_cpp_lib_dependencies = [
    cw_metrics_cpp_lib_dep,
    cw_reference_counter_interface_cpp_lib_dep,
    id_cpp_lib_dep,
    cw_lease_policy_cpp_lib_dep,
    common_proto_cpp_lib_dep,
    publisher_interface_cpp_lib_dep,
    subscriber_interface_cpp_lib_dep,
    rpc_utils_cpp_lib_dep,
    logging_cpp_lib_dep,
    network_util_cpp_lib_dep,
    absl_base_core_headers,
    absl_synchronization,
]

cw_reference_counter_cpp_lib = static_library(
    'cw_reference_counter_cpp_lib',
    ['reference_counter.cc'],
    install: false,
    dependencies: cw_reference_counter_cpp_lib_dependencies
)

cw_reference_counter_cpp_lib_dep = declare_dependency(
    link_with: [cw_reference_counter_cpp_lib],
    dependencies: cw_reference_counter_cpp_lib_dependencies
)
# === @end: cw_reference_counter_cpp_lib ===


# === @begin: cw_task_manager_interface_cpp_lib (@original: task_manager_interface) ===
# Header-only
cw_task_manager_interface_cpp_lib_dependencies = [
    cw_reference_counter_interface_cpp_lib_dep,
    id_cpp_lib_dep,
    status_cpp_lib_dep,
    task_common_cpp_lib_dep,
    common_proto_cpp_lib_dep,
    core_worker_proto_cpp_lib_dep,
]

cw_task_manager_interface_cpp_lib_dep = declare_dependency(
    sources: ['task_manager_interface.h'],
    dependencies: cw_task_manager_interface_cpp_lib_dependencies
)
# === @end: cw_task_manager_interface_cpp_lib ===


# === @begin: cw_future_resolver_cpp_lib (@original: future_resolver) ===
cw_future_resolver_cpp_lib_dependencies = [
    cw_memory_store_cpp_lib_dep,
    cw_reference_counter_interface_cpp_lib_dep,
    id_cpp_lib_dep,
    core_worker_client_pool_cpp_lib_dep,
]

cw_future_resolver_cpp_lib = static_library(
    'cw_future_resolver_cpp_lib',
    ['future_resolver.cc'],
    install: false,
    dependencies: cw_future_resolver_cpp_lib_dependencies
)

cw_future_resolver_cpp_lib_dep = declare_dependency(
    link_with: [cw_future_resolver_cpp_lib],
    dependencies: cw_future_resolver_cpp_lib_dependencies
)
# === @end: cw_future_resolver_cpp_lib ===


# =====================================================================
# Subdirectories — ordering matters for cross-deps
# =====================================================================
# task_execution needs: cw_common, cw_task_event_buffer, cw_context
# actor_management needs: cw_context (actor_manager is defined below)
# task_submission needs: cw_lease_policy, cw_memory_store,
#     cw_task_manager_interface, cw_reference_counter_interface,
#     am_actor_creator
# =====================================================================
subdir('task_execution/')
subdir('actor_management/')
subdir('task_submission/')


# === @begin: am_actor_manager_cpp_lib (@original: actor_management:actor_manager) ===
# IMPORTANT: Defined here (not in actor_management/) because it depends on
# task_submission:actor_task_submitter, which depends on
# actor_management:actor_creator — circular dep at subdir level.
# NOTE: Named with the `am_` prefix (not `cw_`) because it logically belongs to the
# actor_management/ subdirectory. The prefix encodes the Bazel package, not the
# physical file where it's defined: `cw_` = core_worker/, `am_` = actor_management/,
# `ts_` = task_submission/, `task_exec_` = task_execution/.
am_actor_manager_cpp_lib_dependencies = [
    am_actor_creator_cpp_lib_dep,
    am_actor_handle_cpp_lib_dep,
    id_cpp_lib_dep,
    protobuf_utils_cpp_lib_dep,
    task_common_cpp_lib_dep,
    cw_common_cpp_lib_dep,
    core_worker_context_cpp_lib_dep,
    cw_reference_counter_interface_cpp_lib_dep,
    ts_actor_task_submitter_cpp_lib_dep,
    gcs_client_cpp_lib_dep,
    core_worker_proto_cpp_lib_dep,
    absl_container_flat_hash_map,
]

am_actor_manager_cpp_lib = static_library(
    'am_actor_manager_cpp_lib',
    ['actor_management/actor_manager.cc'],
    install: false,
    dependencies: am_actor_manager_cpp_lib_dependencies
)

am_actor_manager_cpp_lib_dep = declare_dependency(
    link_with: [am_actor_manager_cpp_lib],
    dependencies: am_actor_manager_cpp_lib_dependencies
)
# === @end: am_actor_manager_cpp_lib ===


# === @begin: cw_task_manager_cpp_lib (@original: task_manager) ===
cw_task_manager_cpp_lib_dependencies = [
    # Package-managed
    cw_options_cpp_lib_dep,
    cw_memory_store_cpp_lib_dep,
    cw_task_event_buffer_cpp_lib_dep,
    cw_task_manager_interface_cpp_lib_dep,
    buffer_cpp_lib_dep,
    id_cpp_lib_dep,
    protobuf_utils_cpp_lib_dep,
    am_actor_manager_cpp_lib_dep,
    core_worker_client_interface_cpp_lib_dep,
    common_proto_cpp_lib_dep,
    core_worker_proto_cpp_lib_dep,
    stats_metric_cpp_lib_dep,
    counter_map_cpp_lib_dep,
    exponential_backoff_cpp_lib_dep,
    time_cpp_lib_dep,

    # Eugo-managed
    absl_base_core_headers,
    absl_container_flat_hash_map,
    absl_strings,
    absl_synchronization,
]

cw_task_manager_cpp_lib = static_library(
    'cw_task_manager_cpp_lib',
    ['task_manager.cc'],
    install: false,
    dependencies: cw_task_manager_cpp_lib_dependencies
)

cw_task_manager_cpp_lib_dep = declare_dependency(
    link_with: [cw_task_manager_cpp_lib],
    dependencies: cw_task_manager_cpp_lib_dependencies
)
# === @end: cw_task_manager_cpp_lib ===


# === @begin: cw_object_recovery_manager_cpp_lib (@original: object_recovery_manager) ===
cw_object_recovery_manager_cpp_lib_dependencies = [
    # Package-managed
    cw_memory_store_cpp_lib_dep,
    cw_reference_counter_interface_cpp_lib_dep,
    cw_task_manager_cpp_lib_dep,
    id_cpp_lib_dep,
    raylet_client_pool_cpp_lib_dep,

    # Eugo-managed
    absl_base_core_headers,
    absl_synchronization,
]

cw_object_recovery_manager_cpp_lib = static_library(
    'cw_object_recovery_manager_cpp_lib',
    ['object_recovery_manager.cc'],
    install: false,
    dependencies: cw_object_recovery_manager_cpp_lib_dependencies
)

cw_object_recovery_manager_cpp_lib_dep = declare_dependency(
    link_with: [cw_object_recovery_manager_cpp_lib],
    dependencies: cw_object_recovery_manager_cpp_lib_dependencies
)
# === @end: cw_object_recovery_manager_cpp_lib ===


# === @begin: core_worker_cpp_lib (@original: core_worker_lib) ===
core_worker_cpp_lib_dependencies = [
    # Package-managed - this file
    cw_common_cpp_lib_dep,
    core_worker_context_cpp_lib_dep,
    cw_options_cpp_lib_dep,
    cw_exp_mutable_object_manager_cpp_lib_dep,
    cw_exp_mutable_object_provider_cpp_lib_dep,
    cw_future_resolver_cpp_lib_dep,
    cw_generator_waiter_cpp_lib_dep,
    cw_grpc_service_cpp_lib_dep,
    cw_memory_store_cpp_lib_dep,
    cw_metrics_cpp_lib_dep,
    cw_object_recovery_manager_cpp_lib_dep,
    cw_plasma_store_provider_cpp_lib_dep,
    cw_profile_event_cpp_lib_dep,
    cw_reference_counter_cpp_lib_dep,
    cw_shutdown_coordinator_cpp_lib_dep,
    cw_task_event_buffer_cpp_lib_dep,

    # Package-managed - Subdirectory targets
    metrics_cpp_lib_dep,
    protobuf_utils_cpp_lib_dep,
    am_actor_handle_cpp_lib_dep,
    am_actor_manager_cpp_lib_dep,
    task_exec_task_receiver_cpp_lib_dep,
    ts_normal_task_submitter_cpp_lib_dep,
    core_worker_client_cpp_lib_dep,
    core_worker_client_pool_cpp_lib_dep,
    gcs_client_cpp_lib_dep,
    pubsub_proto_cpp_lib_dep,
    publisher_cpp_lib_dep,
    subscriber_cpp_lib_dep,
    raylet_ipc_client_cpp_lib_dep,
    raylet_client_lib_cpp_lib_dep,
    metrics_agent_client_cpp_lib_dep,
    stats_cpp_lib_dep,
    container_util_cpp_lib_dep,
    env_cpp_lib_dep,
    event_cpp_lib_dep,
    mutex_protected_cpp_lib_dep,
    network_util_cpp_lib_dep,
    path_utils_cpp_lib_dep,
    process_cpp_lib_dep,
    process_utils_cpp_lib_dep,
    shared_lru_cpp_lib_dep,
    stream_redirection_cpp_lib_dep,
    stream_redirection_options_cpp_lib_dep,
    time_cpp_lib_dep,

    # Eugo-managed
    absl_cleanup,
    absl_strings,
    # "@com_google_googletest//:gtest_prod",
]

core_worker_cpp_lib = static_library(
    'core_worker_cpp_lib',
    [
        'core_worker.cc',
        'core_worker_process.cc',
        'core_worker_shutdown_executor.cc',
    ],
    install: false,
    dependencies: core_worker_cpp_lib_dependencies
)

core_worker_cpp_lib_dep = declare_dependency(
    link_with: [core_worker_cpp_lib],
    dependencies: core_worker_cpp_lib_dependencies
)
# === @end: core_worker_cpp_lib ===
