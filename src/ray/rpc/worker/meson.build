# === @begin: worker_rpc_cpp_lib (original: worker_rpc) ===
worker_rpc_cpp_lib_dependencies = [
    # Package-managed
    grpc_common_cpp_lib_dep,
    pubsub_proto_cpp_lib_dep,
    ray_common_cpp_lib_dep,
    core_worker_proto_cpp_grpc_lib_dep,

    # Eugo-managed
    boost_asio,
    boost_thread,
    grpc
]

worker_rpc_cpp_lib = static_library(
    'worker_rpc_cpp_lib',
    ['core_worker_client_pool.cc'],
    dependencies: worker_rpc_cpp_lib_dependencies,
    install: false
)

worker_rpc_cpp_lib_dep = declare_dependency(
    link_with: [worker_rpc_cpp_lib],
    dependencies: worker_rpc_cpp_lib_dependencies
)
# === @end: worker_rpc_cpp_lib (original: worker_rpc) ===
