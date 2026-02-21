# === @begin: core_worker_client_interface_cpp_lib (@original: core_worker_client_interface) ===
core_worker_client_interface_cpp_lib_dependencies = [
        core_worker_proto_cpp_lib_dep,
        pubsub_proto_cpp_lib_dep,
        subscriber_interface_cpp_lib_dep,
        rpc_callback_types_cpp_lib_dep,
]


core_worker_client_interface_cpp_lib_dep = declare_dependency(
    sources: ['core_worker_client_interface.h'],
    dependencies: [
        core_worker_client_interface_cpp_lib_dependencies
    ]
)
# === @end: core_worker_client_interface_cpp_lib ===


# === @begin: core_worker_client_cpp_lib (@original: core_worker_client) ===
core_worker_client_cpp_lib_dependencies = [
    core_worker_client_interface_cpp_lib_dep,
    core_worker_proto_cpp_grpc_lib_dep,
    core_worker_proto_cpp_lib_dep,
    retryable_grpc_client_cpp_lib_dep,
    rpc_callback_types_cpp_lib_dep,
    logging_cpp_lib_dep,
    absl_synchronization,
]

core_worker_client_cpp_lib = static_library(
    'core_worker_client_cpp_lib',
    ['core_worker_client.cc'],
    install: false,
    dependencies: core_worker_client_cpp_lib_dependencies
)

core_worker_client_cpp_lib_dep = declare_dependency(
    link_with: [core_worker_client_cpp_lib],
    dependencies: core_worker_client_cpp_lib_dependencies
)
# === @end: core_worker_client_cpp_lib ===


# === @begin: core_worker_client_pool_cpp_lib (@original: core_worker_client_pool) ===
core_worker_client_pool_cpp_lib_dependencies = [
    core_worker_client_interface_cpp_lib_dep,
    id_cpp_lib_dep,
    status_cpp_lib_dep,
    gcs_client_cpp_lib_dep,
    raylet_client_pool_cpp_lib_dep,
    logging_cpp_lib_dep,
    network_util_cpp_lib_dep,
    absl_synchronization,
]

core_worker_client_pool_cpp_lib = static_library(
    'core_worker_client_pool_cpp_lib',
    ['core_worker_client_pool.cc'],
    install: false,
    dependencies: core_worker_client_pool_cpp_lib_dependencies
)

core_worker_client_pool_cpp_lib_dep = declare_dependency(
    link_with: [core_worker_client_pool_cpp_lib],
    dependencies: core_worker_client_pool_cpp_lib_dependencies
)
# === @end: core_worker_client_pool_cpp_lib ===


# === @begin: fake_core_worker_client_cpp_lib (@original: fake_core_worker_client) ===
# header-only

fake_core_worker_client_cpp_lib_dependencies = [
    core_worker_client_interface_cpp_lib_dep,
    absl_synchronization,
]

fake_core_worker_client_cpp_lib_dep = declare_dependency(
    sources: ['fake_core_worker_client.h'],
    dependencies: fake_core_worker_client_cpp_lib_dependencies
)
# === @end: fake_core_worker_client_cpp_lib ===


# === @begin: Umbrella dependency — backward compatible with consumers using the consolidated target
worker_rpc_cpp_lib_dep = declare_dependency(
    dependencies: [
        core_worker_client_interface_cpp_lib_dep,
        core_worker_client_cpp_lib_dep,
        core_worker_client_pool_cpp_lib_dep,
        fake_core_worker_client_cpp_lib_dep
    ]
)
# === @end: Umbrella dependency — backward compatible with consumers using the consolidated target