# @IMPORTANT: ordering of targets in this file is dependent on the build graph.
# You will also see things "out of order" from a BUILD.bazel perspective,

# === @begin: plasma_cpp_fbs_lib (@original: plasma_fbs) ===
# Header-only
# NOTE: this is the same as `hdrs = ["plasma_generated.h"]` aka @original: plasma_generated
plasma_cpp_fbs = custom_target(
    input: ['plasma.fbs'],
    kwargs: cpp_fbs_default_kwargs
)

plasma_cpp_fbs_lib_dep = declare_dependency(
    sources: [plasma_cpp_fbs],
    dependencies: [flatbuffers]
)
# === @end: plasma_cpp_fbs_lib (@original: plasma_fbs) ===


# === @begin: object_manager_plasma_common_cpp_lib (@original: object_manager_plasma_common) ===
# Header-only
object_manager_plasma_common_cpp_lib_dependencies = [
    # Package-managed
    plasma_generated_cpp_lib_dep,
    id_cpp_lib_dep,
    object_manager_common_cpp_lib_dep,
    compat_cpp_lib_dep,

    # Eugo-managed
    # @com_google_googletest//:gtest_prod
]
object_manager_plasma_common_cpp_lib_dep = declare_dependency(
    sources: [
        'common.h',
        'plasma.h'
    ],
    dependencies: object_manager_plasma_common_cpp_lib_dependencies
)
# === @end: object_manager_plasma_common_cpp_lib (@original: object_manager_plasma_common) ===


# === @begin: fling_cpp_lib (@original: fling) ===
# NOTE: In Bazel, fling is excluded on Windows via select(). We compile on all
# non-Windows platforms since our Meson build targets Linux/macOS.
fling_cpp_lib_dependencies = [
    # Package-managed
    logging_cpp_lib_dep,
]

fling_cpp_lib = static_library(
    'fling_cpp_lib',
    ['fling.cc'],
    install: false,
    dependencies: fling_cpp_lib_dependencies
)

fling_cpp_lib_dep = declare_dependency(
    link_with: [fling_cpp_lib],
    dependencies: fling_cpp_lib_dependencies
)
# === @end: fling_cpp_lib (@original: fling) ===


# === @begin: plasma_malloc_cpp_lib (@original: plasma_malloc) ===
plasma_malloc_cpp_lib_dependencies = [
    # Package-managed
    object_manager_plasma_common_cpp_lib_dep,
    ray_config_cpp_lib_dep,
    dlmalloc_cpp_lib_dep,
    compat_cpp_lib_dep,

    # Eugo-managed
    absl_container_flat_hash_map,
]

plasma_malloc_cpp_lib = static_library(
    'plasma_malloc_cpp_lib',
    [
        'dlmalloc.cc',
        'malloc.cc'
    ],
    install: false,
    dependencies: plasma_malloc_cpp_lib_dependencies
)

plasma_malloc_cpp_lib_dep = declare_dependency(
    link_with: [plasma_malloc_cpp_lib],
    dependencies: plasma_malloc_cpp_lib_dependencies
)
# === @end: plasma_malloc_cpp_lib (@original: plasma_malloc) ===


# === @begin: plasma_shared_memory_cpp_lib (@original: plasma_shared_memory) ===
plasma_shared_memory_cpp_lib_dependencies = [
    # Package-managed
    plasma_malloc_cpp_lib_dep,
    ray_config_cpp_lib_dep,
    compat_cpp_lib_dep,
    logging_cpp_lib_dep,
    macros_cpp_lib_dep,
]

plasma_shared_memory_cpp_lib = static_library(
    'plasma_shared_memory_cpp_lib',
    ['shared_memory.cc'],
    install: false,
    # NOTE: this is not our `native/arrow` but plasma's sources vendored by ray. They originate from older Arrow versions, it's why this flag is prefixed w/ `ARROW_`.
    cpp_args: ['-DARROW_USE_GLOG'],
    dependencies: plasma_shared_memory_cpp_lib_dependencies
)

plasma_shared_memory_cpp_lib_dep = declare_dependency(
    link_with: [plasma_shared_memory_cpp_lib],
    dependencies: plasma_shared_memory_cpp_lib_dependencies
)
# === @end: plasma_shared_memory_cpp_lib (@original: plasma_shared_memory) ===


# === @begin: plasma_connection_protocol_cpp_lib (@original: plasma_connection_protocol) ===
plasma_connection_protocol_cpp_lib_dependencies = [
    # Package-managed
    fling_cpp_lib_dep,
    object_manager_plasma_common_cpp_lib_dep,
    plasma_generated_cpp_lib_dep,
    id_cpp_lib_dep,
    status_cpp_lib_dep,
    object_manager_common_cpp_lib_dep,
    common_proto_cpp_lib_dep,
    client_connection_cpp_lib_dep,
    compat_cpp_lib_dep,
    logging_cpp_lib_dep,
    process_utils_cpp_lib_dep,

    # Eugo-managed
    flatbuffers,
    absl_container_flat_hash_map,
    absl_container_flat_hash_set,
]

plasma_connection_protocol_cpp_lib = static_library(
    'plasma_connection_protocol_cpp_lib',
    [
        'connection.cc',
        'protocol.cc'
    ],
    install: false,
    dependencies: plasma_connection_protocol_cpp_lib_dependencies
)

plasma_connection_protocol_cpp_lib_dep = declare_dependency(
    link_with: [plasma_connection_protocol_cpp_lib],
    dependencies: plasma_connection_protocol_cpp_lib_dependencies
)
# === @end: plasma_connection_protocol_cpp_lib (@original: plasma_connection_protocol) ===


# === @begin: plasma_allocator_interface_cpp_lib (@original: plasma_allocator_interface) ===
# Header-only
plasma_allocator_interface_cpp_lib_dependencies = [
    # Package-managed
    object_manager_plasma_common_cpp_lib_dep,
    plasma_generated_cpp_lib_dep,
    status_cpp_lib_dep,
    object_manager_common_cpp_lib_dep,
    common_proto_cpp_lib_dep,

    # Eugo-managed
    absl_container_flat_hash_map,
]

plasma_allocator_interface_cpp_lib_dep = declare_dependency(
    sources: ['allocator.h'],
    dependencies: plasma_allocator_interface_cpp_lib_dependencies
)
# === @end: plasma_allocator_interface_cpp_lib (@original: plasma_allocator_interface) ===


# === @begin: plasma_allocator_cpp_lib (@original: plasma_allocator) ===
plasma_allocator_cpp_lib_dependencies = [
    # Package-managed
    plasma_allocator_interface_cpp_lib_dep,
    plasma_malloc_cpp_lib_dep,
    ray_config_cpp_lib_dep,
    object_manager_common_cpp_lib_dep,
    logging_cpp_lib_dep,

    # Eugo-managed
    absl_types_optional,
]

plasma_allocator_cpp_lib = static_library(
    'plasma_allocator_cpp_lib',
    ['plasma_allocator.cc'],
    install: false,
    dependencies: plasma_allocator_cpp_lib_dependencies
)

plasma_allocator_cpp_lib_dep = declare_dependency(
    link_with: [plasma_allocator_cpp_lib],
    dependencies: plasma_allocator_cpp_lib_dependencies
)
# === @end: plasma_allocator_cpp_lib (@original: plasma_allocator) ===


# === @begin: plasma_object_store_cpp_lib (@original: plasma_object_store) ===
plasma_object_store_cpp_lib_dependencies = [
    # Package-managed
    object_manager_plasma_common_cpp_lib_dep,
    plasma_allocator_interface_cpp_lib_dep,

    # Eugo-managed
    absl_container_flat_hash_map,
]

plasma_object_store_cpp_lib = static_library(
    'plasma_object_store_cpp_lib',
    ['object_store.cc'],
    install: false,
    dependencies: plasma_object_store_cpp_lib_dependencies
)

plasma_object_store_cpp_lib_dep = declare_dependency(
    link_with: [plasma_object_store_cpp_lib],
    dependencies: plasma_object_store_cpp_lib_dependencies
)
# === @end: plasma_object_store_cpp_lib (@original: plasma_object_store) ===


# === @begin: plasma_eviction_policy_cpp_lib (@original: plasma_eviction_policy) ===
plasma_eviction_policy_cpp_lib_dependencies = [
    # Package-managed
    object_manager_plasma_common_cpp_lib_dep,
    plasma_allocator_cpp_lib_dep,
    plasma_object_store_cpp_lib_dep,
    object_manager_common_cpp_lib_dep,
]

plasma_eviction_policy_cpp_lib = static_library(
    'plasma_eviction_policy_cpp_lib',
    ['eviction_policy.cc'],
    install: false,
    dependencies: plasma_eviction_policy_cpp_lib_dependencies
)

plasma_eviction_policy_cpp_lib_dep = declare_dependency(
    link_with: [plasma_eviction_policy_cpp_lib],
    dependencies: plasma_eviction_policy_cpp_lib_dependencies
)
# === @end: plasma_eviction_policy_cpp_lib (@original: plasma_eviction_policy) ===


# === @begin: stats_collector_cpp_lib (@original: stats_collector) ===
stats_collector_cpp_lib_dependencies = [
    # Package-managed
    object_manager_plasma_common_cpp_lib_dep,
    metrics_cpp_lib_dep,
    object_manager_metrics_cpp_lib_dep,
    stats_metric_cpp_lib_dep,
    counter_map_cpp_lib_dep,
]

stats_collector_cpp_lib = static_library(
    'stats_collector_cpp_lib',
    ['stats_collector.cc'],
    install: false,
    dependencies: stats_collector_cpp_lib_dependencies
)

stats_collector_cpp_lib_dep = declare_dependency(
    link_with: [stats_collector_cpp_lib],
    dependencies: stats_collector_cpp_lib_dependencies
)
# === @end: stats_collector_cpp_lib (@original: stats_collector) ===


# === @begin: plasma_create_request_queue_cpp_lib (@original: plasma_create_request_queue) ===
plasma_create_request_queue_cpp_lib_dependencies = [
    # Package-managed
    object_manager_plasma_common_cpp_lib_dep,
    plasma_connection_protocol_cpp_lib_dep,
    file_system_monitor_cpp_lib_dep,
    status_cpp_lib_dep,
    object_manager_common_cpp_lib_dep,

    # Eugo-managed
    absl_container_flat_hash_map,
]

plasma_create_request_queue_cpp_lib = static_library(
    'plasma_create_request_queue_cpp_lib',
    ['create_request_queue.cc'],
    install: false,
    dependencies: plasma_create_request_queue_cpp_lib_dependencies
)

plasma_create_request_queue_cpp_lib_dep = declare_dependency(
    link_with: [plasma_create_request_queue_cpp_lib],
    dependencies: plasma_create_request_queue_cpp_lib_dependencies
)
# === @end: plasma_create_request_queue_cpp_lib (@original: plasma_create_request_queue) ===


# === @begin: obj_lifecycle_mgr_cpp_lib (@original: obj_lifecycle_mgr) ===
obj_lifecycle_mgr_cpp_lib_dependencies = [
    # Package-managed
    plasma_allocator_cpp_lib_dep,
    plasma_connection_protocol_cpp_lib_dep,
    plasma_eviction_policy_cpp_lib_dep,
    plasma_object_store_cpp_lib_dep,
    stats_collector_cpp_lib_dep,
    asio_cpp_lib_dep,
    id_cpp_lib_dep,
    ray_config_cpp_lib_dep,
    object_manager_common_cpp_lib_dep,

    # Eugo-managed
    absl_container_flat_hash_set,
    absl_time,
    # @com_google_googletest//:gtest_prod
]

obj_lifecycle_mgr_cpp_lib = static_library(
    'obj_lifecycle_mgr_cpp_lib',
    ['obj_lifecycle_mgr.cc'],
    install: false,
    dependencies: obj_lifecycle_mgr_cpp_lib_dependencies
)

obj_lifecycle_mgr_cpp_lib_dep = declare_dependency(
    link_with: [obj_lifecycle_mgr_cpp_lib],
    dependencies: obj_lifecycle_mgr_cpp_lib_dependencies
)
# === @end: obj_lifecycle_mgr_cpp_lib (@original: obj_lifecycle_mgr) ===


# === @begin: plasma_get_request_queue_cpp_lib (@original: plasma_get_request_queue) ===
plasma_get_request_queue_cpp_lib_dependencies = [
    # Package-managed
    obj_lifecycle_mgr_cpp_lib_dep,
    plasma_connection_protocol_cpp_lib_dep,
    asio_cpp_lib_dep,
    id_cpp_lib_dep,
]

plasma_get_request_queue_cpp_lib = static_library(
    'plasma_get_request_queue_cpp_lib',
    ['get_request_queue.cc'],
    install: false,
    dependencies: plasma_get_request_queue_cpp_lib_dependencies
)

plasma_get_request_queue_cpp_lib_dep = declare_dependency(
    link_with: [plasma_get_request_queue_cpp_lib],
    dependencies: plasma_get_request_queue_cpp_lib_dependencies
)
# === @end: plasma_get_request_queue_cpp_lib (@original: plasma_get_request_queue) ===


# === @begin: plasma_store_cpp_lib (@original: plasma_store) ===
plasma_store_cpp_lib_dependencies = [
    # Package-managed
    obj_lifecycle_mgr_cpp_lib_dep,
    object_manager_plasma_common_cpp_lib_dep,
    plasma_allocator_cpp_lib_dep,
    plasma_connection_protocol_cpp_lib_dep,
    plasma_create_request_queue_cpp_lib_dep,
    plasma_eviction_policy_cpp_lib_dep,
    plasma_get_request_queue_cpp_lib_dep,
    plasma_malloc_cpp_lib_dep,
    plasma_object_store_cpp_lib_dep,
    asio_cpp_lib_dep,
    file_system_monitor_cpp_lib_dep,
    id_cpp_lib_dep,
    ray_config_cpp_lib_dep,
    status_cpp_lib_dep,
    object_manager_common_cpp_lib_dep,
    client_connection_cpp_lib_dep,
    stats_metric_cpp_lib_dep,
    network_util_cpp_lib_dep,

    # Eugo-managed
    boost_bind,
    absl_base_core_headers,
    absl_container_flat_hash_set,
    absl_synchronization,
]

plasma_store_cpp_lib = static_library(
    'plasma_store_cpp_lib',
    ['store.cc'],
    install: false,
    dependencies: plasma_store_cpp_lib_dependencies
)

plasma_store_cpp_lib_dep = declare_dependency(
    link_with: [plasma_store_cpp_lib],
    dependencies: plasma_store_cpp_lib_dependencies
)
# === @end: plasma_store_cpp_lib (@original: plasma_store) ===


# === @begin: plasma_store_server_cpp_lib (@original: plasma_store_server_lib) ===
plasma_store_server_cpp_lib_dependencies = [
    # Package-managed
    obj_lifecycle_mgr_cpp_lib_dep,
    plasma_allocator_cpp_lib_dep,
    plasma_object_store_cpp_lib_dep,
    plasma_store_cpp_lib_dep,
    asio_cpp_lib_dep,
    file_system_monitor_cpp_lib_dep,
    ray_config_cpp_lib_dep,
    thread_utils_cpp_lib_dep,

    # Eugo-managed
    absl_synchronization,
]

plasma_store_server_cpp_lib = static_library(
    'plasma_store_server_cpp_lib',
    ['store_runner.cc'],
    install: false,
    # NOTE: this is not our `native/arrow` but plasma's sources vendored by ray. They originate from older Arrow versions, it's why this flag is prefixed w/ `ARROW_`.
    cpp_args: ['-DARROW_USE_GLOG'],
    dependencies: plasma_store_server_cpp_lib_dependencies
)

plasma_store_server_cpp_lib_dep = declare_dependency(
    link_with: [plasma_store_server_cpp_lib],
    dependencies: plasma_store_server_cpp_lib_dependencies
)
# === @end: plasma_store_server_cpp_lib (@original: plasma_store_server_lib) ===


# === @begin: plasma_client_interface_cpp_lib (@original: plasma_client_interface) ===
# Header-only
plasma_client_interface_cpp_lib_dependencies = [
    # Package-managed
    buffer_cpp_lib_dep,
    id_cpp_lib_dep,
    status_cpp_lib_dep,
    object_manager_common_cpp_lib_dep,
    common_proto_cpp_lib_dep,

    # Eugo-managed
    absl_container_flat_hash_map,
]

plasma_client_interface_cpp_lib_dep = declare_dependency(
    sources: ['client.h'],
    dependencies: plasma_client_interface_cpp_lib_dependencies
)
# === @end: plasma_client_interface_cpp_lib (@original: plasma_client_interface) ===

# NOTE: @omit: fake_plasma_client.h — test-only target (used only in unit tests via
# FakePlasmaClient). Not built in Meson.


# === @begin: plasma_client_cpp_lib (@original: plasma_client) ===
plasma_client_cpp_lib_dependencies = [
    # Package-managed
    fling_cpp_lib_dep,
    object_manager_plasma_common_cpp_lib_dep,
    plasma_connection_protocol_cpp_lib_dep,
    plasma_generated_cpp_lib_dep,
    plasma_malloc_cpp_lib_dep,
    plasma_shared_memory_cpp_lib_dep,
    asio_cpp_lib_dep,
    buffer_cpp_lib_dep,
    ray_config_cpp_lib_dep,
    status_cpp_lib_dep,
    status_or_cpp_lib_dep,
    object_manager_common_cpp_lib_dep,
    common_proto_cpp_lib_dep,
    compat_cpp_lib_dep,
    visibility_cpp_lib_dep,

    # Eugo-managed
    absl_container_flat_hash_map,
    msgpack_cxx,
]

plasma_client_cpp_lib = static_library(
    'plasma_client_cpp_lib',
    ['client.cc'],
    install: false,
    # NOTE: this is not our `native/arrow` but plasma's sources vendored by ray. They originate from older Arrow versions, it's why this flag is prefixed w/ `ARROW_`.
    cpp_args: ['-DARROW_USE_GLOG'],
    dependencies: plasma_client_cpp_lib_dependencies
)

plasma_client_cpp_lib_dep = declare_dependency(
    link_with: [plasma_client_cpp_lib],
    dependencies: plasma_client_cpp_lib_dependencies
)
# === @end: plasma_client_cpp_lib (@original: plasma_client) ===
