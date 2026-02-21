# === @begin: raylet_metrics_cpp_lib (@original: metrics) ===
# Defined early because worker_pool depends on it
raylet_metrics_cpp_lib_dep = declare_dependency(
    sources: ['metrics.h'],
    dependencies: [
        observability_metrics_cpp_lib_dep,
        stats_cpp_lib_dep,
    ]
)
# === @end: raylet_metrics_cpp_lib ===


# === @begin: agent_manager_cpp_lib (@original: agent_manager) ===
agent_manager_cpp_lib_dependencies = [
    id_cpp_lib_dep,
    ray_config_cpp_lib_dep,
    gcs_proto_cpp_lib_dep,
    event_cpp_lib_dep,
    logging_cpp_lib_dep,
    process_cpp_lib_dep,
    process_interface_cpp_lib_dep,
    process_utils_cpp_lib_dep,
    thread_utils_cpp_lib_dep,
    boost_asio,
]

agent_manager_cpp_lib = static_library(
    'agent_manager_cpp_lib',
    ['agent_manager.cc'],
    install: false,
    dependencies: agent_manager_cpp_lib_dependencies
)

agent_manager_cpp_lib_dep = declare_dependency(
    link_with: [agent_manager_cpp_lib],
    dependencies: agent_manager_cpp_lib_dependencies
)
# === @end: agent_manager_cpp_lib ===


# === @begin: wait_manager_cpp_lib (@original: wait_manager) ===
wait_manager_cpp_lib_dependencies = [
    id_cpp_lib_dep,
    container_util_cpp_lib_dep,
]

wait_manager_cpp_lib = static_library(
    'wait_manager_cpp_lib',
    ['wait_manager.cc'],
    install: false,
    dependencies: wait_manager_cpp_lib_dependencies
)

wait_manager_cpp_lib_dep = declare_dependency(
    link_with: [wait_manager_cpp_lib],
    dependencies: wait_manager_cpp_lib_dependencies
)
# === @end: wait_manager_cpp_lib ===


# === @begin: lease_dependency_manager_cpp_lib (@original: lease_dependency_manager) ===
lease_dependency_manager_cpp_lib_dependencies = [
    id_cpp_lib_dep,
    object_manager_cpp_lib_dep,
    counter_map_cpp_lib_dep,
    absl_container_flat_hash_map,
    absl_container_flat_hash_set,
]

lease_dependency_manager_cpp_lib = static_library(
    'lease_dependency_manager_cpp_lib',
    ['lease_dependency_manager.cc'],
    install: false,
    dependencies: lease_dependency_manager_cpp_lib_dependencies
)

lease_dependency_manager_cpp_lib_dep = declare_dependency(
    link_with: [lease_dependency_manager_cpp_lib],
    dependencies: lease_dependency_manager_cpp_lib_dependencies
)
# === @end: lease_dependency_manager_cpp_lib ===


# === @begin: placement_group_resource_manager_cpp_lib (@original: placement_group_resource_manager) ===
placement_group_resource_manager_cpp_lib_dependencies = [
    id_cpp_lib_dep,
    gcs_client_cpp_lib_dep,
    cluster_resource_scheduler_cpp_lib_dep,
    container_util_cpp_lib_dep,
    absl_container_flat_hash_map,
]

placement_group_resource_manager_cpp_lib = static_library(
    'placement_group_resource_manager_cpp_lib',
    ['placement_group_resource_manager.cc'],
    install: false,
    dependencies: placement_group_resource_manager_cpp_lib_dependencies
)

placement_group_resource_manager_cpp_lib_dep = declare_dependency(
    link_with: [placement_group_resource_manager_cpp_lib],
    dependencies: placement_group_resource_manager_cpp_lib_dependencies
)
# === @end: placement_group_resource_manager_cpp_lib ===


# === @begin: worker_interface_cpp_lib (@original: worker_interface) ===
worker_interface_cpp_lib_dep = declare_dependency(
    sources: ['worker_interface.h'],
    dependencies: [
        id_cpp_lib_dep,
        process_interface_cpp_lib_dep,
        absl_time,
    ]
)
# === @end: worker_interface_cpp_lib ===


# === @begin: runtime_env_agent_client_cpp_lib (@original: runtime_env_agent_client) ===
runtime_env_agent_client_cpp_lib_dependencies = [
    asio_cpp_lib_dep,
    id_cpp_lib_dep,
    ray_config_cpp_lib_dep,
    status_cpp_lib_dep,
    gcs_proto_cpp_lib_dep,
    runtime_env_agent_proto_cpp_lib_dep,
    authentication_token_loader_cpp_lib_dep,
    logging_cpp_lib_dep,
    process_utils_cpp_lib_dep,
    time_cpp_lib_dep,
    boost_beast,
    absl_container_flat_hash_set,
    absl_strings,
]

runtime_env_agent_client_cpp_lib = static_library(
    'runtime_env_agent_client_cpp_lib',
    ['runtime_env_agent_client.cc'],
    install: false,
    dependencies: runtime_env_agent_client_cpp_lib_dependencies
)

runtime_env_agent_client_cpp_lib_dep = declare_dependency(
    link_with: [runtime_env_agent_client_cpp_lib],
    dependencies: runtime_env_agent_client_cpp_lib_dependencies
)
# === @end: runtime_env_agent_client_cpp_lib ===


# === @begin: worker_cpp_lib (@original: worker) ===
worker_cpp_lib_dependencies = [
    worker_interface_cpp_lib_dep,
    id_cpp_lib_dep,
    ray_common_cpp_lib_dep,     # lease (consolidated)
    core_worker_client_cpp_lib_dep,
    node_manager_cpp_fbs_lib_dep,
    core_worker_proto_cpp_grpc_lib_dep,
    core_worker_proto_cpp_lib_dep,
    cluster_resource_scheduler_cpp_lib_dep,
    client_connection_cpp_lib_dep,
    process_cpp_lib_dep,
    process_interface_cpp_lib_dep,
    absl_time,
]

worker_cpp_lib = static_library(
    'worker_cpp_lib',
    ['worker.cc'],
    install: false,
    dependencies: worker_cpp_lib_dependencies
)

worker_cpp_lib_dep = declare_dependency(
    link_with: [worker_cpp_lib],
    dependencies: worker_cpp_lib_dependencies
)
# === @end: worker_cpp_lib ===


# === @begin: worker_pool_cpp_lib (@original: worker_pool) ===
worker_pool_cpp_lib_dependencies = [
    raylet_metrics_cpp_lib_dep,
    runtime_env_agent_client_cpp_lib_dep,
    worker_interface_cpp_lib_dep,
    constants_cpp_lib_dep,
    ray_common_cpp_lib_dep,     # lease, protobuf_utils, runtime_env, status (consolidated)
    ray_config_cpp_lib_dep,
    core_worker_client_interface_cpp_lib_dep,
    gcs_client_cpp_lib_dep,
    core_worker_proto_cpp_lib_dep,
    client_connection_cpp_lib_dep,
    container_util_cpp_lib_dep,
    network_util_cpp_lib_dep,
    process_cpp_lib_dep,
    process_utils_cpp_lib_dep,
    time_cpp_lib_dep,
    boost_system,
    absl_container_flat_hash_map,
    absl_strings,
]

worker_pool_cpp_lib = static_library(
    'worker_pool_cpp_lib',
    ['worker_pool.cc'],
    install: false,
    dependencies: worker_pool_cpp_lib_dependencies
)

worker_pool_cpp_lib_dep = declare_dependency(
    link_with: [worker_pool_cpp_lib],
    dependencies: worker_pool_cpp_lib_dependencies
)
# === @end: worker_pool_cpp_lib ===


# === @begin: worker_killing_policy_cpp_lib (@original: worker_killing_policy) ===
worker_killing_policy_cpp_lib_dependencies = [
    worker_interface_cpp_lib_dep,
    worker_pool_cpp_lib_dep,
    ray_common_cpp_lib_dep,     # lease, memory_monitor_interface (consolidated)
    absl_time,
]

worker_killing_policy_cpp_lib = static_library(
    'worker_killing_policy_cpp_lib',
    [
        'worker_killing_policy.cc',
        'worker_killing_policy_group_by_owner.cc',
    ],
    install: false,
    dependencies: worker_killing_policy_cpp_lib_dependencies
)

worker_killing_policy_cpp_lib_dep = declare_dependency(
    link_with: [worker_killing_policy_cpp_lib],
    dependencies: worker_killing_policy_cpp_lib_dependencies
)
# === @end: worker_killing_policy_cpp_lib ===


# === @begin: throttler_cpp_lib (@original: throttler) ===
throttler_cpp_lib_dep = declare_dependency(
    sources: ['throttler.h'],
    dependencies: [absl_time]
)
# === @end: throttler_cpp_lib ===


# === @begin: local_object_manager_interface_cpp_lib (@original: local_object_manager_interface) ===
local_object_manager_interface_cpp_lib_dep = declare_dependency(
    sources: ['local_object_manager_interface.h'],
    dependencies: [
        id_cpp_lib_dep,
        ray_object_cpp_lib_dep,
        node_manager_proto_cpp_lib_dep,
    ]
)
# === @end: local_object_manager_interface_cpp_lib ===


# === @begin: local_object_manager_cpp_lib (@original: local_object_manager) ===
local_object_manager_cpp_lib_dependencies = [
    local_object_manager_interface_cpp_lib_dep,
    worker_pool_cpp_lib_dep,
    asio_cpp_lib_dep,
    id_cpp_lib_dep,
    ray_object_cpp_lib_dep,
    core_worker_client_pool_cpp_lib_dep,
    metric_interface_cpp_lib_dep,
    node_manager_proto_cpp_lib_dep,
    subscriber_interface_cpp_lib_dep,
    time_cpp_lib_dep,
    absl_strings,
]

local_object_manager_cpp_lib = static_library(
    'local_object_manager_cpp_lib',
    ['local_object_manager.cc'],
    install: false,
    dependencies: local_object_manager_cpp_lib_dependencies
)

local_object_manager_cpp_lib_dep = declare_dependency(
    link_with: [local_object_manager_cpp_lib],
    dependencies: local_object_manager_cpp_lib_dependencies
)
# === @end: local_object_manager_cpp_lib ===


# === @begin: raylet_cpp_lib (@original: raylet_lib) ===
raylet_cpp_lib_dependencies = [
    agent_manager_cpp_lib_dep,
    lease_dependency_manager_cpp_lib_dep,
    local_object_manager_interface_cpp_lib_dep,
    placement_group_resource_manager_cpp_lib_dep,
    runtime_env_agent_client_cpp_lib_dep,
    throttler_cpp_lib_dep,
    wait_manager_cpp_lib_dep,
    worker_cpp_lib_dep,
    worker_killing_policy_cpp_lib_dep,
    worker_pool_cpp_lib_dep,
    ray_common_cpp_lib_dep,         # buffer, flatbuf_utils, lease, memory_monitor (consolidated)
    cgroup_manager_interface_cpp_lib_dep,
    core_worker_client_pool_cpp_lib_dep,
    node_manager_cpp_fbs_lib_dep,
    gcs_client_cpp_lib_dep,
    object_manager_cpp_lib_dep,
    plasma_client_cpp_lib_dep,
    metric_interface_cpp_lib_dep,
    subscriber_cpp_lib_dep,
    scheduler_cpp_lib_dep,
    node_manager_server_cpp_lib_dep,
    rpc_callback_types_cpp_lib_dep,
    authentication_token_loader_cpp_lib_dep,
    stats_cpp_lib_dep,
    cmd_line_utils_cpp_lib_dep,
    container_util_cpp_lib_dep,
    network_util_cpp_lib_dep,
    port_persistence_cpp_lib_dep,
    process_cpp_lib_dep,
    process_utils_cpp_lib_dep,
    time_cpp_lib_dep,
    util_cpp_lib_dep,
    boost_system,
    absl_base_core_headers,
    absl_container_flat_hash_map,
    absl_container_flat_hash_set,
    absl_memory,
    absl_strings,
    absl_time,
    threads,
]

raylet_cpp_lib = static_library(
    'raylet_cpp_lib',
    ['node_manager.cc'],
    install: false,
    dependencies: raylet_cpp_lib_dependencies
)

raylet_cpp_lib_dep = declare_dependency(
    link_with: [raylet_cpp_lib],
    dependencies: raylet_cpp_lib_dependencies
)
# === @end: raylet_cpp_lib ===


# === @begin: raylet_executable (original: raylet) ===
raylet_executable = executable(
    'raylet',
    ['main.cc'],
    dependencies: [
        # Package-managed
        local_object_manager_cpp_lib_dep,
        local_object_manager_interface_cpp_lib_dep,
        raylet_metrics_cpp_lib_dep,
        raylet_cpp_lib_dep,
        asio_cpp_lib_dep,
        ray_common_cpp_lib_dep,     # lease, metrics, ray_config, status (consolidated)
        cgroup_manager_factory_cpp_lib_dep,
        core_worker_cpp_lib_dep,    # core_worker:metrics
        core_worker_client_cpp_lib_dep,
        core_worker_client_pool_cpp_lib_dep,
        gcs_client_cpp_lib_dep,
        object_manager_cpp_lib_dep, # includes ownership_object_directory
        cluster_lease_manager_cpp_lib_dep,
        raylet_client_lib_cpp_lib_dep,
        raylet_client_pool_cpp_lib_dep,
        metrics_agent_client_cpp_lib_dep,
        stats_cpp_lib_dep,
        cmd_line_utils_cpp_lib_dep,
        event_cpp_lib_dep,
        process_cpp_lib_dep,
        raii_cpp_lib_dep,
        stream_redirection_cpp_lib_dep,
        stream_redirection_options_cpp_lib_dep,
        time_cpp_lib_dep,
        util_cpp_lib_dep,

        # Eugo managed
        gflags,
        absl_strings,
        nlohmann_json,
    ],
    install: true,
    install_dir: py.get_install_dir() / 'ray/core/src/ray/raylet/'
)
# === @end: raylet_executable ===
