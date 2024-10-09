# === @begin: pubsub_cpp_lib (original: pubsub_lib) ===
pubsub_cpp_lib_dependencies = [
    # Package-managed
    pubsub_rpc_cpp_lib_dep,

    # Eugo-managed
    boost_any,
    absl_container_flat_hash_map,
    absl_container_flat_hash_set,
    absl_synchronization,
]

pubsub_cpp_lib = static_library(
    'pubsub_cpp_lib',
    ['publisher.cc', 'subscriber.cc'],
    install: false,
    dependencies: pubsub_cpp_lib_dependencies
)

pubsub_cpp_lib_dep = declare_dependency(
    link_with: [pubsub_cpp_lib],
    dependencies: pubsub_cpp_lib_dependencies
)
# === @end: pubsub_cpp_lib (original: pubsub_lib) ===
