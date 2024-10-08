# === @begin: gcs_pub_sub_cpp_lib (original: gcs_pub_sub_lib) ===
gcs_pub_sub_cpp_lib = static_library(
    'gcs_pub_sub_cpp_lib',
    ['gcs_pub_sub.cc'],
    install: false,
    dependencies: [
        # Package-managed
        gcs_cpp_lib_dep,
        gcs_service_rpc_cpp_lib_dep,
        pubsub_cpp_lib_dep,
        ray_common_cpp_lib_dep,
        redis_client_cpp_lib_dep
    ]
)

gcs_pub_sub_cpp_lib_dep = declare_dependency(
    link_with: [gcs_pub_sub_cpp_lib]
)
# === @end: gcs_pub_sub_cpp_lib (original: gcs_pub_sub_lib) ===
