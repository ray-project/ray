# === @begin: object_manager_rpc_cpp_lib (original: object_manager_rpc) ===
object_manager_rpc_cpp_lib_dependencies = [
    # Package-managed
    grpc_common_cpp_lib_dep,
    object_manager_proto_cpp_grpc_lib_dep,
    ray_common_cpp_lib_dep,

    # Eugo-managed
    boost_asio,
    grpc
]

object_manager_rpc_cpp_lib_dep = declare_dependency(
    sources: ['object_manager_client.h', 'object_manager_server.h'],
    dependencies: object_manager_rpc_cpp_lib_dependencies
)
# === @end: object_manager_rpc_cpp_lib (original: object_manager_rpc) ===
