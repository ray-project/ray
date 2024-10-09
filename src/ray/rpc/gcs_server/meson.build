# === @begin: gcs_service_rpc_cpp_lib (original: gcs_service_rpc) ===
gcs_service_rpc_cpp_lib_dependencies = [
    # Package-managed
    autoscaler_proto_cpp_grpc_lib_dep,
    gcs_service_proto_cpp_grpc_lib_dep,
    grpc_common_cpp_lib_dep,
    pubsub_proto_cpp_lib_dep,
    ray_common_cpp_lib_dep,

    # Eugo-managed
    boost_asio,
    grpc,
    absl_container_btree
]

gcs_service_rpc_cpp_lib_dep = declare_dependency(
    sources: ['gcs_rpc_client.h', 'gcs_rpc_server.h'],
    dependencies: gcs_service_rpc_cpp_lib_dependencies
)
# === @end: gcs_service_rpc_cpp_lib (original: gcs_service_rpc) ===
