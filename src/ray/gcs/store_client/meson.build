# === @begin: observable_store_client_cpp_lib (original: observable_store_client) ===
observable_store_client_cpp_lib_dependencies = [
    # Package-managed
    ray_common_cpp_lib_dep,
    util_cpp_lib_dep
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
# === @end: observable_store_client_cpp_lib (original: observable_store_client) ===


# === @begin: redis_store_client_cpp_lib (original: redis_store_client) ===
redis_store_client_cpp_lib_dependencies = [
    # Package-managed
    redis_client_cpp_lib_dep,

    # Eugo-managed
    absl_strings,
    absl_synchronization
]

redis_store_client_cpp_lib = static_library(
    'redis_store_client_cpp_lib',
    ['redis_store_client.cc'],
    install: false,
    dependencies: redis_store_client_cpp_lib_dependencies
)

redis_store_client_cpp_lib_dep = declare_dependency(
    link_with: [redis_store_client_cpp_lib],
    dependencies: redis_store_client_cpp_lib_dependencies
)
# === @end: redis_store_client_cpp_lib (original: redis_store_client) ===


# === @begin: gcs_in_memory_store_client_cpp_lib (original: gcs_in_memory_store_client) ===
gcs_in_memory_store_client_cpp_lib_dependencies = [
    # Package-managed
    ray_common_cpp_lib_dep,
    util_cpp_lib_dep
]

gcs_in_memory_store_client_cpp_lib = static_library(
    'gcs_in_memory_store_client_cpp_lib',
    ['in_memory_store_client.cc'],
    install: false,
    dependencies: gcs_in_memory_store_client_cpp_lib_dependencies
)

gcs_in_memory_store_client_cpp_lib_dep = declare_dependency(
    link_with: [gcs_in_memory_store_client_cpp_lib],
    dependencies: gcs_in_memory_store_client_cpp_lib_dependencies
)
# === @end: gcs_in_memory_store_client_cpp_lib (original: gcs_in_memory_store_client) ===
