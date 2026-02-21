# === @begin: raylet_ipc_client_interface_cpp_lib (@original: raylet_ipc_client_interface) ===
raylet_ipc_client_interface_cpp_lib_dep = declare_dependency(
    sources: ['raylet_ipc_client_interface.h'],
    dependencies: [
        ray_common_cpp_lib_dep,  # buffer, id, status, flatbuf_utils (consolidated)
        node_manager_cpp_fbs_lib_dep,
        common_proto_cpp_lib_dep,
        process_interface_cpp_lib_dep,
        absl_container_flat_hash_set,
    ]
)
# === @end: raylet_ipc_client_interface_cpp_lib ===


# === @begin: client_connection_cpp_lib (@original: client_connection) ===
client_connection_cpp_lib_dependencies = [
    asio_cpp_lib_dep,
    event_stats_cpp_lib_dep,
    id_cpp_lib_dep,
    status_cpp_lib_dep,
    node_manager_cpp_fbs_lib_dep,
    network_util_cpp_lib_dep,
    process_utils_cpp_lib_dep,
    time_cpp_lib_dep,
]

client_connection_cpp_lib = static_library(
    'client_connection_cpp_lib',
    ['client_connection.cc'],
    install: false,
    dependencies: client_connection_cpp_lib_dependencies
)

client_connection_cpp_lib_dep = declare_dependency(
    link_with: [client_connection_cpp_lib],
    dependencies: client_connection_cpp_lib_dependencies
)
# === @end: client_connection_cpp_lib ===


# === @begin: raylet_ipc_client_target_cpp_lib (@original: raylet_ipc_client) ===
raylet_ipc_client_target_cpp_lib_dependencies = [
    client_connection_cpp_lib_dep,
    asio_cpp_lib_dep,
    ray_common_cpp_lib_dep,  # buffer, flatbuf_utils, id, status (consolidated)
    node_manager_cpp_fbs_lib_dep,
    common_proto_cpp_lib_dep,
    raylet_ipc_client_interface_cpp_lib_dep,
    logging_cpp_lib_dep,
    process_utils_cpp_lib_dep,
    absl_container_flat_hash_set,
]

raylet_ipc_client_target_cpp_lib = static_library(
    'raylet_ipc_client_target_cpp_lib',
    ['raylet_ipc_client.cc'],
    install: false,
    dependencies: raylet_ipc_client_target_cpp_lib_dependencies
)

raylet_ipc_client_target_cpp_lib_dep = declare_dependency(
    link_with: [raylet_ipc_client_target_cpp_lib],
    dependencies: raylet_ipc_client_target_cpp_lib_dependencies
)
# === @end: raylet_ipc_client_target_cpp_lib ===


# Umbrella dependency — backward compatible with consumers using the consolidated target
raylet_ipc_client_cpp_lib_dep = declare_dependency(
    dependencies: [
        raylet_ipc_client_interface_cpp_lib_dep,
        client_connection_cpp_lib_dep,
        raylet_ipc_client_target_cpp_lib_dep,
    ]
)
