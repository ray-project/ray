# === @begin: core_worker_cpp_lib (original: core_worker_lib) ===
core_worker_cpp_lib_dependencies = [
    # Package-managed
    gcs_cpp_lib_dep,
    gcs_client_cpp_lib_dep,
    plasma_client_cpp_lib_dep,
    ray_common_cpp_lib_dep,
    raylet_client_cpp_lib_dep,
    stats_cpp_lib_dep,
    worker_rpc_cpp_lib_dep,
    core_worker_proto_cpp_lib_dep,
    util_cpp_lib_dep,

    # Eugo-managed
    boost_circular_buffer,
    boost_fiber,
    absl_cleanup,
    absl_container_btree,
    absl_container_flat_hash_map,
    absl_container_flat_hash_set,
    absl_container_node_hash_map,
    nlohmann_json
]

core_worker_cpp_lib = static_library(
    'core_worker_cpp_lib',
    [
        'actor_handle.cc',
        'actor_manager.cc',
        'common.cc',
        'context.cc',
        'core_worker.cc',
        'core_worker_process.cc',
        'experimental_mutable_object_manager.cc',
        'experimental_mutable_object_provider.cc',
        'future_resolver.cc',
        'generator_waiter.cc',
        'lease_policy.cc',
        'object_recovery_manager.cc',
        'profile_event.cc',
        'reference_count.cc',
        'task_event_buffer.cc',
        'task_manager.cc',

        'store_provider/plasma_store_provider.cc',
        'store_provider/memory_store/memory_store.cc',

        'transport/actor_scheduling_queue.cc',
        'transport/actor_scheduling_util.cc',
        'transport/actor_task_submitter.cc',
        'transport/concurrency_group_manager.cc',
        'transport/dependency_resolver.cc',
        'transport/normal_scheduling_queue.cc',
        'transport/normal_task_submitter.cc',
        'transport/out_of_order_actor_scheduling_queue.cc',
        'transport/out_of_order_actor_submit_queue.cc',
        'transport/sequential_actor_submit_queue.cc',
        'transport/task_receiver.cc',
        'transport/thread_pool.cc'
    ],
    install: false,
    dependencies: core_worker_cpp_lib_dependencies
)

core_worker_cpp_lib_dep = declare_dependency(
    link_with: [core_worker_cpp_lib],
    dependencies: core_worker_cpp_lib_dependencies
)
# === @end: core_worker_cpp_lib (original: core_worker_lib) ===
