# === @begin: grpc_common_cpp_lib (original: grpc_common_lib) ===
grpc_common_cpp_lib_dependencies = [
    # Package-managed
    stats_metric_cpp_lib_dep,
    asio_cpp_lib_dep,
    grpc_util_cpp_lib_dep,
    id_cpp_lib_dep,
    ray_config_cpp_lib_dep,
    status_cpp_lib_dep,

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
    dependencies: grpc_common_cpp_lib_dependencies,
    install: false
)

grpc_common_cpp_lib_dep = declare_dependency(
    link_with: [grpc_common_cpp_lib],
    dependencies: grpc_common_cpp_lib_dependencies
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


# === @begin: reporter_rpc_cpp_lib (original: reporter_rpc) ===
reporter_rpc_cpp_lib_private_dependencies = [
    # Package-managed
    grpc_common_cpp_lib_dep,
    reporter_proto_cpp_grpc_lib_dep,
    status_cpp_lib_dep,

    # Eugo-managed
    boost_asio,
    boost_thread,
    grpc
]

reporter_rpc_cpp_lib_dep = declare_dependency(
    sources: ['metrics_agent_client.h'],
    dependencies: reporter_rpc_cpp_lib_private_dependencies
)
# === @end: reporter_rpc_cpp_lib (original: reporter_rpc) ===


# === @begin: pubsub_rpc_cpp_lib (original: pubsub_rpc) ===
pubsub_rpc_cpp_lib_dep = declare_dependency(
    dependencies: [
        pubsub_proto_cpp_grpc_lib_dep,
        grpc_common_cpp_lib_dep,
        ray_common_cpp_lib_dep
    ]
)
# === @end: pubsub_rpc_cpp_lib (original: pubsub_rpc) ===


# === @begin: autoscaler_rpc_cpp_lib (original: autoscaler_rpc) ===
autoscaler_rpc_cpp_lib_dep = declare_dependency(
    dependencies: [autoscaler_proto_cpp_grpc_lib_dep]
)
# === @end: autoscaler_rpc_cpp_lib (original: autoscaler_rpc) ===
