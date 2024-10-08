# === @begin: ray_syncer_cpp_lib (original: ray_syncer) ===
ray_syncer_cpp_lib_private_deps = [
    # Package-managed
    asio_cpp_lib_dep,
    id_cpp_lib_dep,
    ray_syncer_proto_cpp_grpc_lib_dep,
]

ray_syncer_cpp_lib_public_deps = [
    # Eugo-managed
    grpc,
    absl_container_flat_hash_map
]

ray_syncer_cpp_lib = static_library(
    'ray_syncer_cpp_lib',
    ['ray_syncer.cc'],
    install: false,
    dependencies: [
        ray_syncer_cpp_lib_private_deps,
        ray_syncer_cpp_lib_public_deps
    ]
)

ray_syncer_cpp_lib_dep = declare_dependency(
    link_with: [ray_syncer_cpp_lib],
    dependencies: ray_syncer_cpp_lib_public_deps
)
# === @end: ray_syncer_cpp_lib (original: ray_syncer) ===
