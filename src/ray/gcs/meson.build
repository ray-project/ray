# === @begin: gcs_cpp_lib (original: gcs) ===
# NOTE: Old redis files (asio.cc, redis_async_context.cc, redis_client.cc, redis_context.cc)
# moved to store_client/ during upstream restructure. gcs_cpp_lib is now a virtual dep.
gcs_cpp_lib_dependencies = [
    # Package-managed
    node_manager_cpp_fbs_lib_dep,
    node_manager_rpc_cpp_lib_dep,
    ray_common_cpp_lib_dep,
    stats_cpp_lib_dep,
    gcs_proto_cpp_lib_dep,
    gcs_service_proto_cpp_lib_dep,
    util_cpp_lib_dep,

    # Eugo-managed
    hiredis, hiredis_ssl,
    boost_asio
]

gcs_cpp_lib_dep = declare_dependency(
    dependencies: gcs_cpp_lib_dependencies
)
# === @end: gcs_cpp_lib (original: gcs) ===


# === @begin: postable/ ===
subdir('postable/')
# === @end: postable/ ===


# === @begin: redis_client_cpp_lib (original: redis_client) ===
# NOTE: Redis client files moved to store_client/. redis_client_cpp_lib is now a virtual dep.
redis_client_cpp_lib_dependencies = [
    # Package-managed
    ray_common_cpp_lib_dep,
    stats_cpp_lib_dep,
    util_cpp_lib_dep,

    # Eugo-managed
    hiredis, hiredis_ssl,
    boost_asio
]

redis_client_cpp_lib_dep = declare_dependency(
    dependencies: redis_client_cpp_lib_dependencies
)
# === @end: redis_client_cpp_lib (original: redis_client) ===


# === @begin: store_client/ ===
subdir('store_client/')
# === @end: store_client/ ===


# === @begin: actor/ ===
# NOTE: actor/ moved to src/ray/meson.build (after core_worker_rpc_client/)
# because gcs_actor depends on worker_rpc_cpp_lib_dep which is defined in
# core_worker_rpc_client/, processed after gcs/.
# === @end: actor/ ===
