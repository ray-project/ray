# === @begin: object_manager_client_interface_cpp_lib (@original: object_manager_client_interface) ===
# Header-only
object_manager_client_interface_cpp_lib_dependencies = [
    # Package-managed
    object_manager_proto_cpp_lib_dep,
    rpc_callback_types_cpp_lib_dep,
]

object_manager_client_interface_cpp_lib_dep = declare_dependency(
    sources: ['object_manager_client_interface.h'],
    dependencies: object_manager_client_interface_cpp_lib_dependencies
)
# === @end: object_manager_client_interface_cpp_lib (@original: object_manager_client_interface) ===


# === @begin: object_manager_client_cpp_lib (@original: object_manager_client) ===
# Header-only
object_manager_client_cpp_lib_dependencies = [
    # Package-managed
    object_manager_client_interface_cpp_lib_dep,
    object_manager_grpc_client_manager_cpp_lib_dep,
    object_manager_proto_cpp_grpc_lib_dep,
    logging_cpp_lib_dep,

    # Eugo-managed
    grpc,
]

object_manager_client_cpp_lib_dep = declare_dependency(
    sources: ['object_manager_client.h'],
    dependencies: object_manager_client_cpp_lib_dependencies
)
# === @end: object_manager_client_cpp_lib (@original: object_manager_client) ===
