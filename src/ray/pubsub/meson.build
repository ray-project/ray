# === @begin: pubsub_cpp_lib (original: pubsub_lib) ===
pubsub_cpp_lib_private_deps = [
    # Package-managed
    pubsub_rpc_cpp_lib_dep,
]

pubsub_cpp_lib_public_deps = [
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
    dependencies: [
        pubsub_cpp_lib_private_deps,
        pubsub_cpp_lib_public_deps
    ]
)

pubsub_cpp_lib_dep = declare_dependency(
    link_with: [pubsub_cpp_lib],
    dependencies: pubsub_cpp_lib_public_deps
)
# === @end: pubsub_cpp_lib (original: pubsub_lib) ===
