subdir('protobuf/')
subdir('thirdparty/')
subdir('util/')
subdir('stats/')

# We can't traverse the entire `raylet` tree, as it includes parts which depend on targets from `common`.
subdir('raylet/format/')

subdir('common/')

subdir('rpc/')


# @Important: due to circular dependency between `stats` and `rpc` folders, this guy should go here
# === @begin: stats/stats_cpp_lib (original: stats_lib) ===
stats_cpp_lib_dependencies = [
    # Package-managed
    reporter_rpc_cpp_lib_dep,
    stats_metric_cpp_lib_dep,

    # Eugo-managed
    grpc,

    # System-managed
    threads
]

stats_cpp_lib = static_library(
    'stats_cpp_lib',
    ['stats/metric_exporter.cc'],
    install: false,
    dependencies: stats_cpp_lib_dependencies
)

stats_cpp_lib_dep = declare_dependency(
    link_with: [stats_cpp_lib],
    dependencies: stats_cpp_lib_dependencies
)
# === @end: stats/stats_cpp_lib (original: stats_lib) ===


subdir('object_manager/')

subdir('pubsub/')

subdir('raylet_client/')

subdir('gcs/')

# We can't traverse the entire `raylet` tree, as it includes parts which depend on targets from `gcs`.
subdir('raylet/scheduling/')

# Such division is required to avoid circular raptors
subdir('gcs/gcs_server/')

subdir('core_worker/')

# @Important: due to circular dependency between `object_manager` and `gcs` folders, this guy should go here
# === @begin: object_manager/object_manager_cpp_lib (original: object_manager) ===
object_manager_cpp_lib_dependencies = [
    # Package-managed
    core_worker_cpp_lib_dep,
    gcs_cpp_lib_dep,
    object_manager_rpc_cpp_lib_dep,
    plasma_store_server_cpp_lib_dep,
    ray_common_cpp_lib_dep,
    util_cpp_lib_dep,

    # Eugo-managed
    boost_asio
]

object_manager_cpp_lib = static_library(
    'object_manager_cpp_lib',
    [
        'object_manager/chunk_object_reader.cc',
        'object_manager/pull_manager.cc',
        'object_manager/spilled_object_reader.cc',
        'object_manager/memory_object_reader.cc',
        'object_manager/object_manager.cc',
        'object_manager/object_buffer_pool.cc',
        'object_manager/ownership_based_object_directory.cc',
        'object_manager/push_manager.cc',
    ],
    install: false,
    dependencies: object_manager_cpp_lib_dependencies
)

object_manager_cpp_lib_dep = declare_dependency(
    link_with: [object_manager_cpp_lib],
    dependencies: object_manager_cpp_lib_dependencies
)
# === @end: object_manager/object_manager_cpp_lib (original: object_manager) ===

subdir('raylet/')

subdir('internal/')