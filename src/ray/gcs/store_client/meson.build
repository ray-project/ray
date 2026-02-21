# === @begin: store_client_cpp_lib (@original: store_client) ===
store_client_cpp_lib_dep = declare_dependency(
    sources: ['store_client.h'],
    dependencies: [
        asio_cpp_lib_dep,
        id_cpp_lib_dep,
        status_cpp_lib_dep,
        postable_cpp_lib_dep,
        rpc_callback_types_cpp_lib_dep,
    ]
)
# === @end: store_client_cpp_lib ===


# === @begin: observable_store_client_cpp_lib (@original: observable_store_client) ===
observable_store_client_cpp_lib_dependencies = [
    store_client_cpp_lib_dep,
]

observable_store_client_cpp_lib = static_library(
    'observable_store_client_cpp_lib',
    ['observable_store_client.cc'],
    install: false,
    dependencies: observable_store_client_cpp_lib_dependencies
)

observable_store_client_cpp_lib_dep = declare_dependency(
    link_with: [observable_store_client_cpp_lib],
    dependencies: observable_store_client_cpp_lib_dependencies
)
# === @end: observable_store_client_cpp_lib ===


# === @begin: in_memory_store_client_cpp_lib (@original: in_memory_store_client) ===
in_memory_store_client_cpp_lib_dependencies = [
    store_client_cpp_lib_dep,
    asio_cpp_lib_dep,
    concurrent_flat_map_cpp_lib_dep,
    absl_container_node_hash_map,
]

in_memory_store_client_cpp_lib = static_library(
    'in_memory_store_client_cpp_lib',
    ['in_memory_store_client.cc'],
    install: false,
    dependencies: in_memory_store_client_cpp_lib_dependencies
)

in_memory_store_client_cpp_lib_dep = declare_dependency(
    link_with: [in_memory_store_client_cpp_lib],
    dependencies: in_memory_store_client_cpp_lib_dependencies
)
# backward compat alias
gcs_in_memory_store_client_cpp_lib_dep = in_memory_store_client_cpp_lib_dep
# === @end: in_memory_store_client_cpp_lib ===


# === @begin: redis_store_client_cpp_lib (@original: redis_store_client) ===
redis_store_client_cpp_lib_dependencies = [
    store_client_cpp_lib_dep,
    hiredis,
    hiredis_ssl,
    asio_cpp_lib_dep,
    ray_config_cpp_lib_dep,
    status_cpp_lib_dep,
    postable_cpp_lib_dep,
    stats_cpp_lib_dep,
    container_util_cpp_lib_dep,
    exponential_backoff_cpp_lib_dep,
    network_util_cpp_lib_dep,
    boost_asio,
    absl_strings,
    absl_synchronization,
]

redis_store_client_cpp_lib = static_library(
    'redis_store_client_cpp_lib',
    [
        'redis_async_context.cc',
        'redis_context.cc',
        'redis_store_client.cc',
    ],
    install: false,
    dependencies: redis_store_client_cpp_lib_dependencies
)

redis_store_client_cpp_lib_dep = declare_dependency(
    link_with: [redis_store_client_cpp_lib],
    dependencies: redis_store_client_cpp_lib_dependencies
)
# === @end: redis_store_client_cpp_lib ===
