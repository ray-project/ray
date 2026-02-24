# IMPORTANT: Ordering within this file respects the internal dependency graph.
# You will see things "out of order" from a BUILD.bazel perspective because
# Meson requires define-before-use: if target A depends on target B and both
# are in this file, B must appear first regardless of Bazel ordering.
# Specifically:
#   - client_connection moved before raylet_ipc_client (raylet_ipc_client depends on it)
#
# @skip: fake_raylet_ipc_client (@original: fake_raylet_ipc_client) — test-only header; not compiled in production Meson build.


# === @begin: raylet_ipc_client_interface_cpp_lib (@original: raylet_ipc_client_interface) ===
# Header-only
raylet_ipc_client_interface_cpp_lib_dependencies = [
    # Package-managed
    buffer_cpp_lib_dep,
    id_cpp_lib_dep,
    status_cpp_lib_dep,
    node_manager_cpp_fbs_lib_dep,
    common_proto_cpp_lib_dep,
    process_interface_cpp_lib_dep,

    # Eugo-managed
    absl_container_flat_hash_set,
]

raylet_ipc_client_interface_cpp_lib_dep = declare_dependency(
    sources: ['raylet_ipc_client_interface.h'],
    dependencies: raylet_ipc_client_interface_cpp_lib_dependencies
)
# === @end: raylet_ipc_client_interface_cpp_lib ===


# === @begin: client_connection_cpp_lib (@original: client_connection) ===
client_connection_cpp_lib_dependencies = [
    # Package-managed
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
    # Package-managed
    client_connection_cpp_lib_dep,
    asio_cpp_lib_dep,
    buffer_cpp_lib_dep,
    flatbuf_utils_cpp_lib_dep,
    id_cpp_lib_dep,
    status_cpp_lib_dep,
    node_manager_cpp_fbs_lib_dep,
    common_proto_cpp_lib_dep,
    raylet_ipc_client_interface_cpp_lib_dep,
    logging_cpp_lib_dep,
    process_utils_cpp_lib_dep,

    # Eugo-managed
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
raylet_ipc_client_cpp_lib_dependencies = [
    # Package-managed
    raylet_ipc_client_interface_cpp_lib_dep,
    client_connection_cpp_lib_dep,
    raylet_ipc_client_target_cpp_lib_dep,
]

raylet_ipc_client_cpp_lib_dep = declare_dependency(
    dependencies: raylet_ipc_client_cpp_lib_dependencies
)
