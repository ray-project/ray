# === @begin: grpc_common_cpp_lib (original: grpc_common_lib) ===
grpc_common_cpp_lib_private_deps = [
    # Package-managed
    stats_metric_cpp_lib_dep,
    asio_cpp_lib_dep,
    grpc_util_cpp_lib_dep,
    id_cpp_lib_dep,
    ray_config_cpp_lib_dep,
    status_cpp_lib_dep,
]

grpc_common_cpp_lib_public_deps = [
    # Eugo-managed
    grpc,
    protobuf
]

grpc_common_cpp_lib = static_library(
    'grpc_common_lib',
    [
        'common.cc',
        'grpc_server.cc',
        'server_call.cc',
    ],
    dependencies: [
        grpc_common_cpp_lib_private_deps,
        grpc_common_cpp_lib_public_deps
    ],
    install: false
)

grpc_common_cpp_lib_dep = declare_dependency(
    link_with: [grpc_common_cpp_lib],
    dependencies: grpc_common_cpp_lib_public_deps
)
# === @end: grpc_common_cpp_lib (original: grpc_common_lib) ===


# === @begin: node_manager/ ===
subdir('node_manager/')
# === @end: node_manager/ ===


# === @begin: gcs_server/ ===
subdir('gcs_server/')
# === @end: gcs_server/ ===


# === @begin: object_manager/ ===
subdir('object_manager/')
# === @end: object_manager/ ===

# === @begin: worker/ ===
subdir('worker/')
# === @end: worker/ ===