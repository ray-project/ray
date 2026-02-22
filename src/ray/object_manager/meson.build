# === @begin: object_manager_common_cpp_lib (@original: object_manager_common) ===
object_manager_common_cpp_lib_dependencies = [
    # Package-managed
    id_cpp_lib_dep,
    ray_config_cpp_lib_dep,
    status_cpp_lib_dep,

    # Eugo-managed
    absl_strings,
    absl_time,
]

object_manager_common_cpp_lib = static_library(
    'object_manager_common_cpp_lib',
    ['common.cc'],
    install: false,
    dependencies: object_manager_common_cpp_lib_dependencies
)

object_manager_common_cpp_lib_dep = declare_dependency(
    link_with: [object_manager_common_cpp_lib],
    dependencies: object_manager_common_cpp_lib_dependencies
)
# === @end: object_manager_common_cpp_lib ===


# === @begin: object_reader_cpp_lib (@original: object_reader) ===
# Header-only
object_reader_cpp_lib_dependencies = [
    # Package-managed
    common_proto_cpp_lib_dep,
]

object_reader_cpp_lib_dep = declare_dependency(
    sources: ['object_reader.h'],
    dependencies: object_reader_cpp_lib_dependencies
)
# === @end: object_reader_cpp_lib ===


# === @begin: object_manager_metrics_cpp_lib (@original: metrics) ===
# Header-only
object_manager_metrics_cpp_lib_dependencies = [
    # Package-managed
    stats_metric_cpp_lib_dep,
    size_literals_cpp_lib_dep,
]

object_manager_metrics_cpp_lib_dep = declare_dependency(
    sources: ['metrics.h'],
    dependencies: object_manager_metrics_cpp_lib_dependencies
)
# === @end: object_manager_metrics_cpp_lib ===


# === @begin: object_manager_grpc_client_manager_cpp_lib (@original: object_manager_grpc_client_manager) ===
# Header-only
object_manager_grpc_client_manager_cpp_lib_dependencies = [
    # Package-managed
    ray_config_cpp_lib_dep,
    grpc_client_cpp_lib_dep,

    # Eugo-managed
    grpc,
    absl_synchronization,
]

object_manager_grpc_client_manager_cpp_lib_dep = declare_dependency(
    sources: ['grpc_client_manager.h'],
    dependencies: object_manager_grpc_client_manager_cpp_lib_dependencies
)
# === @end: object_manager_grpc_client_manager_cpp_lib ===


# === @begin: object_directory_cpp_lib (@original: object_directory) ===
# Header-only
# NOTE: object_directory deps include gcs_client (from gcs_rpc_client/) which is
# processed after object_manager/. The header-only declare_dependency omits that
# dep — it's resolved at final link time via _raylet shared library.
object_directory_cpp_lib_dependencies = [
    # Package-managed
    object_manager_common_cpp_lib_dep,
    asio_cpp_lib_dep,
    id_cpp_lib_dep,
    status_cpp_lib_dep,
]

object_directory_cpp_lib_dep = declare_dependency(
    sources: ['object_directory.h'],
    dependencies: object_directory_cpp_lib_dependencies
)
# === @end: object_directory_cpp_lib (@original: object_directory) ===


# === @begin: plasma/ ===
subdir('plasma/')
# === @end: plasma/ ===


# === @begin: memory_object_reader_cpp_lib (@original: memory_object_reader) ===
memory_object_reader_cpp_lib_dependencies = [
    # Package-managed
    object_reader_cpp_lib_dep,
    plasma_client_cpp_lib_dep,
]

memory_object_reader_cpp_lib = static_library(
    'memory_object_reader_cpp_lib',
    ['memory_object_reader.cc'],
    install: false,
    dependencies: memory_object_reader_cpp_lib_dependencies
)

memory_object_reader_cpp_lib_dep = declare_dependency(
    link_with: [memory_object_reader_cpp_lib],
    dependencies: memory_object_reader_cpp_lib_dependencies
)
# === @end: memory_object_reader_cpp_lib ===


# === @begin: chunk_object_reader_cpp_lib (@original: chunk_object_reader) ===
chunk_object_reader_cpp_lib_dependencies = [
    # Package-managed
    object_reader_cpp_lib_dep,
    logging_cpp_lib_dep,
]

chunk_object_reader_cpp_lib = static_library(
    'chunk_object_reader_cpp_lib',
    ['chunk_object_reader.cc'],
    install: false,
    dependencies: chunk_object_reader_cpp_lib_dependencies
)

chunk_object_reader_cpp_lib_dep = declare_dependency(
    link_with: [chunk_object_reader_cpp_lib],
    dependencies: chunk_object_reader_cpp_lib_dependencies
)
# === @end: chunk_object_reader_cpp_lib ===


# === @begin: spilled_object_reader_cpp_lib (@original: spilled_object_reader) ===
spilled_object_reader_cpp_lib_dependencies = [
    # Package-managed
    object_reader_cpp_lib_dep,
    logging_cpp_lib_dep,

    # Eugo-managed
    absl_types_optional,
]

spilled_object_reader_cpp_lib = static_library(
    'spilled_object_reader_cpp_lib',
    ['spilled_object_reader.cc'],
    install: false,
    dependencies: spilled_object_reader_cpp_lib_dependencies
)

spilled_object_reader_cpp_lib_dep = declare_dependency(
    link_with: [spilled_object_reader_cpp_lib],
    dependencies: spilled_object_reader_cpp_lib_dependencies
)
# === @end: spilled_object_reader_cpp_lib ===


# === @begin: object_buffer_pool_cpp_lib (@original: object_buffer_pool) ===
object_buffer_pool_cpp_lib_dependencies = [
    # Package-managed
    memory_object_reader_cpp_lib_dep,
    id_cpp_lib_dep,
    status_cpp_lib_dep,
    logging_cpp_lib_dep,

    # Eugo-managed
    absl_base_core_headers,
    absl_container_flat_hash_map,
    absl_synchronization,
    absl_time,
]

object_buffer_pool_cpp_lib = static_library(
    'object_buffer_pool_cpp_lib',
    ['object_buffer_pool.cc'],
    install: false,
    dependencies: object_buffer_pool_cpp_lib_dependencies
)

object_buffer_pool_cpp_lib_dep = declare_dependency(
    link_with: [object_buffer_pool_cpp_lib],
    dependencies: object_buffer_pool_cpp_lib_dependencies
)
# === @end: object_buffer_pool_cpp_lib ===


# === @begin: push_manager_cpp_lib (@original: push_manager) ===
push_manager_cpp_lib_dependencies = [
    # Package-managed
    object_manager_metrics_cpp_lib_dep,
    id_cpp_lib_dep,
    stats_metric_cpp_lib_dep,

    # Eugo-managed
    absl_container_flat_hash_map,
]

push_manager_cpp_lib = static_library(
    'push_manager_cpp_lib',
    ['push_manager.cc'],
    install: false,
    dependencies: push_manager_cpp_lib_dependencies
)

push_manager_cpp_lib_dep = declare_dependency(
    link_with: [push_manager_cpp_lib],
    dependencies: push_manager_cpp_lib_dependencies
)
# === @end: push_manager_cpp_lib ===
