# === @begin: gcs_client_cpp_lib (original: gcs_client_lib) ===
gcs_client_cpp_lib_dependencies = [
    # Package-managed
    gcs_cpp_lib_dep,
    gcs_pub_sub_cpp_lib_dep,
    gcs_service_rpc_cpp_lib_dep,
    pubsub_cpp_lib_dep,
    ray_common_cpp_lib_dep,
    redis_store_client_cpp_lib_dep,
    usage_proto_cpp_lib_dep
]

gcs_client_cpp_lib = static_library(
    'gcs_client_cpp_lib',
    ['accessor.cc', 'gcs_client.cc'],
    install: false,
    dependencies: gcs_client_cpp_lib_dependencies
)

gcs_client_cpp_lib_dep = declare_dependency(
    link_with: [gcs_client_cpp_lib],
    dependencies: gcs_client_cpp_lib_dependencies
)
# === @end: gcs_client_cpp_lib (original: gcs_client_lib) ===
