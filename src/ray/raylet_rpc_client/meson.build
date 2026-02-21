# === @begin: raylet_client_interface_cpp_lib (@original: raylet_client_interface) ===
raylet_client_interface_cpp_lib_dep = declare_dependency(
    sources: ['raylet_client_interface.h'],
    dependencies: [
        autoscaler_proto_cpp_lib_dep,
        common_proto_cpp_lib_dep,
        node_manager_proto_cpp_lib_dep,
        rpc_callback_types_cpp_lib_dep,
    ]
)
# === @end: raylet_client_interface_cpp_lib ===


# === @begin: raylet_client_lib_cpp_lib (@original: raylet_client_lib) ===
raylet_client_lib_cpp_lib_dependencies = [
    raylet_client_interface_cpp_lib_dep,
    ray_common_cpp_lib_dep,  # bundle_spec, ray_config (consolidated)
    node_manager_proto_cpp_grpc_lib_dep,
    retryable_grpc_client_cpp_lib_dep,
    rpc_callback_types_cpp_lib_dep,
    logging_cpp_lib_dep,
]

raylet_client_lib_cpp_lib = static_library(
    'raylet_client_lib_cpp_lib',
    ['raylet_client.cc'],
    install: false,
    dependencies: raylet_client_lib_cpp_lib_dependencies
)

raylet_client_lib_cpp_lib_dep = declare_dependency(
    link_with: [raylet_client_lib_cpp_lib],
    dependencies: raylet_client_lib_cpp_lib_dependencies
)
# === @end: raylet_client_lib_cpp_lib ===


# === @begin: raylet_client_pool_cpp_lib (@original: raylet_client_pool) ===
raylet_client_pool_cpp_lib_dependencies = [
    raylet_client_interface_cpp_lib_dep,
    gcs_client_cpp_lib_dep,
]

raylet_client_pool_cpp_lib = static_library(
    'raylet_client_pool_cpp_lib',
    ['raylet_client_pool.cc'],
    install: false,
    dependencies: raylet_client_pool_cpp_lib_dependencies
)

raylet_client_pool_cpp_lib_dep = declare_dependency(
    link_with: [raylet_client_pool_cpp_lib],
    dependencies: raylet_client_pool_cpp_lib_dependencies
)
# === @end: raylet_client_pool_cpp_lib ===


# === @begin: raylet_client_with_io_context_cpp_lib (@original: raylet_client_with_io_context_lib) ===
raylet_client_with_io_context_cpp_lib_dependencies = [
    raylet_client_interface_cpp_lib_dep,
    raylet_client_lib_cpp_lib_dep,
    ray_common_cpp_lib_dep,  # bundle_spec, ray_config (consolidated)
    node_manager_proto_cpp_grpc_lib_dep,
    retryable_grpc_client_cpp_lib_dep,
    rpc_callback_types_cpp_lib_dep,
    logging_cpp_lib_dep,
]

raylet_client_with_io_context_cpp_lib = static_library(
    'raylet_client_with_io_context_cpp_lib',
    ['raylet_client_with_io_context.cc'],
    install: false,
    dependencies: raylet_client_with_io_context_cpp_lib_dependencies
)

raylet_client_with_io_context_cpp_lib_dep = declare_dependency(
    link_with: [raylet_client_with_io_context_cpp_lib],
    dependencies: raylet_client_with_io_context_cpp_lib_dependencies
)
# === @end: raylet_client_with_io_context_cpp_lib ===


# Umbrella dependency — backward compatible with consumers using the consolidated target
raylet_client_cpp_lib_dep = declare_dependency(
    dependencies: [
        raylet_client_interface_cpp_lib_dep,
        raylet_client_lib_cpp_lib_dep,
        raylet_client_pool_cpp_lib_dep,
        raylet_client_with_io_context_cpp_lib_dep,
    ]
)
