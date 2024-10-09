# === @begin: __common__ ===
object_manager_common_dep = declare_dependency(
    compile_args: ['-DARROW_USE_GLOG']
)
# === @end: __common__ ===


# === @begin: plasma/ ===
subdir('plasma/')
# === @end: plasma/ ===


# === @begin: plasma_client_cpp_lib (@original: plasma_client) ===
plasma_client_cpp_lib_dependencies = [
    # Project-managed
    object_manager_common_dep,

    plasma_cpp_fbs_lib_dep,
    ray_common_cpp_lib_dep,
    common_proto_cpp_lib_dep,
    util_cpp_lib_dep,

    # Eugo-managed
    msgpack_cxx,
    absl_strings
]

plasma_client_cpp_lib = static_library(
    'plasma_client_cpp_lib',
    [
        'common.cc',
        'plasma/client.cc',
        'plasma/connection.cc',
        'plasma/malloc.cc',
        'plasma/plasma.cc',
        'plasma/protocol.cc',
        'plasma/shared_memory.cc',
        'plasma/fling.cc'
    ],
    install: false,
    dependencies: plasma_client_cpp_lib_dependencies
)

plasma_client_cpp_lib_dep = declare_dependency(
    link_with: [plasma_client_cpp_lib],
    dependencies: plasma_client_cpp_lib_dependencies
)
# === @end: plasma_client_cpp_lib (@original: plasma_client) ===


# === @begin: plasma_store_server_cpp_lib (@original: plasma_store_server_lib) ===
plasma_store_server_cpp_lib_dependencies = [
    # Project-managed
    object_manager_common_dep,

    plasma_client_cpp_lib_dep,
    stats_cpp_lib_dep,
    network_cpp_lib_dep,

    # Eugo-managed
    spdlog
]

plasma_store_server_cpp_lib = static_library(
    'plasma_store_server_cpp_lib',
    [
        'plasma/create_request_queue.cc',
        'plasma/dlmalloc.cc',
        'plasma/eviction_policy.cc',
        'plasma/get_request_queue.cc',
        'plasma/object_lifecycle_manager.cc',
        'plasma/object_store.cc',
        'plasma/plasma_allocator.cc',
        'plasma/stats_collector.cc',
        'plasma/store.cc',
        'plasma/store_runner.cc'
    ],
    install: false,
    dependencies: plasma_store_server_cpp_lib_dependencies
)

plasma_store_server_cpp_lib_dep = declare_dependency(
    link_with: [plasma_store_server_cpp_lib],
    dependencies: plasma_store_server_cpp_lib_dependencies
)
# === @end: plasma_store_server_cpp_lib (@original: plasma_store_server_lib) ===
