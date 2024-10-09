# === @begin: node_manager_rpc_cpp_lib (original: node_manager_rpc) ===
node_manager_rpc_cpp_lib_dependencies = [
    # Package-managed
    grpc_common_cpp_lib_dep,
    node_manager_proto_cpp_grpc_lib_dep,
    ray_common_cpp_lib_dep,

    # Eugo-managed
    boost_asio,
    grpc
]

node_manager_rpc_cpp_lib = static_library(
    'node_manager_rpc_cpp_lib',
    [
        'node_manager_client_pool.cc'
    ],
    dependencies: node_manager_rpc_cpp_lib_dependencies,
    install: false
)

node_manager_rpc_cpp_lib_dep = declare_dependency(
    link_with: [node_manager_rpc_cpp_lib],
    dependencies: node_manager_rpc_cpp_lib_dependencies
)
# === @end: node_manager_rpc_cpp_lib (original: node_manager_rpc) ===
