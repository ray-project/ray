# === @begin: ts_dependency_resolver_cpp_lib (@original: dependency_resolver) ===
ts_dependency_resolver_cpp_lib_dependencies = [
    # Package-managed
    id_cpp_lib_dep,
    task_common_cpp_lib_dep,
    cw_lease_policy_cpp_lib_dep,            # from parent core_worker/meson.build
    cw_memory_store_cpp_lib_dep,            # from parent core_worker/meson.build
    cw_task_manager_interface_cpp_lib_dep,  # from parent core_worker/meson.build
    am_actor_creator_cpp_lib_dep,           # from actor_management/meson.build

    # Eugo-managed
    absl_container_flat_hash_map,
    absl_container_flat_hash_set,
]

ts_dependency_resolver_cpp_lib = static_library(
    'ts_dependency_resolver_cpp_lib',
    ['dependency_resolver.cc'],
    install: false,
    dependencies: ts_dependency_resolver_cpp_lib_dependencies
)

ts_dependency_resolver_cpp_lib_dep = declare_dependency(
    link_with: [ts_dependency_resolver_cpp_lib],
    dependencies: ts_dependency_resolver_cpp_lib_dependencies
)
# === @end: ts_dependency_resolver_cpp_lib ===


# === @begin: ts_actor_submit_queue_cpp_lib (@original: actor_submit_queue) ===
# Header-only
ts_actor_submit_queue_cpp_lib_dependencies = [
        # Package-managed
        id_cpp_lib_dep,
        task_common_cpp_lib_dep,

        # Eugo-managed
        absl_types_optional,
]

ts_actor_submit_queue_cpp_lib_dep = declare_dependency(
    sources: ['actor_submit_queue.h'],
    dependencies: ts_actor_submit_queue_cpp_lib_dependencies
)
# === @end: ts_actor_submit_queue_cpp_lib ===


# === @begin: ts_out_of_order_actor_submit_queue_cpp_lib (@original: out_of_order_actor_submit_queue) ===
ts_out_of_order_actor_submit_queue_cpp_lib_dependencies = [
    # Package-managed
    ts_actor_submit_queue_cpp_lib_dep,
    id_cpp_lib_dep,

    # Eugo-managed
    absl_container_btree,
    absl_types_optional,
]

ts_out_of_order_actor_submit_queue_cpp_lib = static_library(
    'ts_out_of_order_actor_submit_queue_cpp_lib',
    ['out_of_order_actor_submit_queue.cc'],
    install: false,
    dependencies: ts_out_of_order_actor_submit_queue_cpp_lib_dependencies
)

ts_out_of_order_actor_submit_queue_cpp_lib_dep = declare_dependency(
    link_with: [ts_out_of_order_actor_submit_queue_cpp_lib],
    dependencies: ts_out_of_order_actor_submit_queue_cpp_lib_dependencies
)
# === @end: ts_out_of_order_actor_submit_queue_cpp_lib ===


# === @begin: ts_sequential_actor_submit_queue_cpp_lib (@original: sequential_actor_submit_queue) ===
ts_sequential_actor_submit_queue_cpp_lib_dependencies = [
    # Package-managed
    ts_actor_submit_queue_cpp_lib_dep,
    id_cpp_lib_dep,

    # Eugo-managed
    absl_types_optional,
]

ts_sequential_actor_submit_queue_cpp_lib = static_library(
    'ts_sequential_actor_submit_queue_cpp_lib',
    ['sequential_actor_submit_queue.cc'],
    install: false,
    dependencies: ts_sequential_actor_submit_queue_cpp_lib_dependencies
)

ts_sequential_actor_submit_queue_cpp_lib_dep = declare_dependency(
    link_with: [ts_sequential_actor_submit_queue_cpp_lib],
    dependencies: ts_sequential_actor_submit_queue_cpp_lib_dependencies
)
# === @end: ts_sequential_actor_submit_queue_cpp_lib ===


# === @begin: ts_task_submission_util_cpp_lib (@original: task_submission_util) ===
# Header-only
ts_task_submission_util_cpp_lib_dependencies = [
    # Package-managed
    asio_cpp_lib_dep,
    id_cpp_lib_dep,
    gcs_client_cpp_lib_dep,
]

ts_task_submission_util_cpp_lib_dep = declare_dependency(
    sources: ['task_submission_util.h'],
    dependencies: ts_task_submission_util_cpp_lib_dependencies
)
# === @end: ts_task_submission_util_cpp_lib ===


# === @begin: ts_actor_task_submitter_cpp_lib (@original: actor_task_submitter) ===
ts_actor_task_submitter_cpp_lib_dependencies = [
    # Package-managed
    ts_actor_submit_queue_cpp_lib_dep,
    ts_dependency_resolver_cpp_lib_dep,
    ts_out_of_order_actor_submit_queue_cpp_lib_dep,
    ts_sequential_actor_submit_queue_cpp_lib_dep,
    ts_task_submission_util_cpp_lib_dep,
    asio_cpp_lib_dep,
    id_cpp_lib_dep,
    protobuf_utils_cpp_lib_dep,
    cw_reference_counter_interface_cpp_lib_dep,  # from parent core_worker/meson.build
    am_actor_creator_cpp_lib_dep,           # from actor_management/meson.build
    core_worker_client_pool_cpp_lib_dep,
    gcs_client_cpp_lib_dep,
    raylet_client_interface_cpp_lib_dep,
    raylet_client_pool_cpp_lib_dep,
    rpc_callback_types_cpp_lib_dep,
    time_cpp_lib_dep,

    # Eugo-managed
    absl_base_core_headers,
    absl_container_flat_hash_map,
    absl_container_flat_hash_set,
]

ts_actor_task_submitter_cpp_lib = static_library(
    'ts_actor_task_submitter_cpp_lib',
    ['actor_task_submitter.cc'],
    install: false,
    dependencies: ts_actor_task_submitter_cpp_lib_dependencies
)

ts_actor_task_submitter_cpp_lib_dep = declare_dependency(
    link_with: [ts_actor_task_submitter_cpp_lib],
    dependencies: ts_actor_task_submitter_cpp_lib_dependencies
)
# === @end: ts_actor_task_submitter_cpp_lib ===


# === @begin: ts_normal_task_submitter_cpp_lib (@original: normal_task_submitter) ===
ts_normal_task_submitter_cpp_lib_dependencies = [
    # Package-managed
    ts_dependency_resolver_cpp_lib_dep,
    ts_task_submission_util_cpp_lib_dep,
    id_cpp_lib_dep,
    lease_cpp_lib_dep,
    protobuf_utils_cpp_lib_dep,
    cw_lease_policy_cpp_lib_dep,            # from parent core_worker/meson.build
    cw_memory_store_cpp_lib_dep,            # from parent core_worker/meson.build
    cw_task_manager_interface_cpp_lib_dep,  # from parent core_worker/meson.build
    core_worker_client_pool_cpp_lib_dep,
    gcs_client_cpp_lib_dep,
    raylet_client_interface_cpp_lib_dep,
    raylet_client_pool_cpp_lib_dep,
    process_utils_cpp_lib_dep,
    time_cpp_lib_dep,

    # Eugo-managed
    absl_base_core_headers,
]

ts_normal_task_submitter_cpp_lib = static_library(
    'ts_normal_task_submitter_cpp_lib',
    ['normal_task_submitter.cc'],
    install: false,
    dependencies: ts_normal_task_submitter_cpp_lib_dependencies
)

ts_normal_task_submitter_cpp_lib_dep = declare_dependency(
    link_with: [ts_normal_task_submitter_cpp_lib],
    dependencies: ts_normal_task_submitter_cpp_lib_dependencies
)
# === @end: ts_normal_task_submitter_cpp_lib ===
