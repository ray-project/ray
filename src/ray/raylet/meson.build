# === @begin: raylet_cpp_lib (original: raylet_lib) ===
raylet_cpp_lib_dependencies = [
    # Package-managed
    gcs_cpp_lib_dep,
    gcs_client_cpp_lib_dep,
    node_manager_cpp_fbs_lib_dep,
    node_manager_rpc_cpp_lib_dep,
    object_manager_cpp_lib_dep,
    plasma_client_cpp_lib_dep,
    pubsub_cpp_lib_dep,
    ray_common_cpp_lib_dep,
    scheduler_cpp_lib_dep,
    stats_cpp_lib_dep,
    worker_rpc_cpp_lib_dep,
    agent_manager_proto_cpp_lib_dep,
    common_proto_cpp_lib_dep,
    runtime_env_agent_proto_cpp_lib_dep,
    util_cpp_lib_dep,

    # Eugo-managed
    boost_asio,
    boost_beast,
    boost_system,
    prometheus_cpp_pull,
    absl_base_core_headers,
    absl_container_flat_hash_set,
    absl_memory,
    absl_strings,
    #"@com_google_googletest//:gtest",
    opencensus_cpp_prometheus_exporter ,
    opencensus_cpp_stats,
    opencensus_cpp_tags,

    # System-managed
    threads
]

raylet_cpp_lib = static_library(
    'raylet_cpp_lib',
    [
        'worker_killing_policy.cc',
        'node_manager.cc',
        'raylet.cc',
        'local_object_manager.cc',
        'agent_manager.cc',
        'placement_group_resource_manager.cc',
        'worker_killing_policy_retriable_fifo.cc',
        'runtime_env_agent_client.cc',
        'worker_killing_policy_group_by_owner.cc',
        'local_task_manager.cc',
        'wait_manager.cc',
        'worker.cc',
        'dependency_manager.cc',
        'worker_pool.cc'
    ],
    install: false,
    dependencies: raylet_cpp_lib_dependencies
)

raylet_cpp_lib_dep = declare_dependency(
    link_with: [raylet_cpp_lib],
    dependencies: raylet_cpp_lib_dependencies
)
# === @end: raylet_cpp_lib (original: raylet_lib) ===


# === @begin: raylet_executable (original: raylet) ===
raylet_executable = executable(
    'raylet',
    ['main.cc'],
    install: false,
    dependencies: [
        # Package-managed
        raylet_cpp_lib_dep,
        util_cpp_lib_dep,

        # Eugo-managed
        gflags
    ]
)
# === @end: raylet_executable (original: raylet) ===
