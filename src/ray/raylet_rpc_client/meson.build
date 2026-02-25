# IMPORTANT: Ordering within this file respects the internal dependency graph.
# You will see things "out of order" from a BUILD.bazel perspective because
# Meson requires define-before-use: if target A depends on target B and both
# are in this file, B must appear first regardless of Bazel ordering.
# Specifically:
#   - `raylet_client_lib` moved before `raylet_client_pool` (`raylet_client_pool` depends on
#     `gcs_client` which is NOT AVAILABLE (processed after), but `raylet_client_with_io_context` depends on
#     `raylet_client_lib`)
#
# @skip: `fake_raylet_client` (`fake_raylet_client.h`) (@original: `fake_raylet_client`) — test-only header; not compiled in production Meson build.


# === @begin: raylet_client_interface_cpp_lib (@original: raylet_client_interface) ===
# Header-only
raylet_client_interface_cpp_lib_dependencies = [
    # Package-managed
    autoscaler_proto_cpp_lib_dep,
    common_proto_cpp_lib_dep,
    node_manager_proto_cpp_lib_dep,
    rpc_callback_types_cpp_lib_dep,
]

raylet_client_interface_cpp_lib_dep = declare_dependency(
    sources: ['raylet_client_interface.h'],
    dependencies: raylet_client_interface_cpp_lib_dependencies
)
# === @end: raylet_client_interface_cpp_lib ===


# === @begin: raylet_client_lib_cpp_lib (@original: raylet_client_lib) ===
raylet_client_lib_cpp_lib_dependencies = [
    # Package-managed
    raylet_client_interface_cpp_lib_dep,
    bundle_spec_cpp_lib_dep,
    ray_config_cpp_lib_dep,
    node_manager_proto_cpp_grpc_lib_dep,
    retryable_grpc_client_cpp_lib_dep,
    rpc_callback_types_cpp_lib_dep,
    logging_cpp_lib_dep,
]

raylet_client_lib_cpp_lib = static_library(
    'raylet_client_lib_cpp_lib',
    ['raylet_client.cc'],
    install: false,
    dependencies: raylet_client_lib_cpp_lib_dependencies
)

raylet_client_lib_cpp_lib_dep = declare_dependency(
    link_with: [raylet_client_lib_cpp_lib],
    dependencies: raylet_client_lib_cpp_lib_dependencies
)
# === @end: raylet_client_lib_cpp_lib ===


# === @begin: raylet_client_pool_cpp_lib (@original: raylet_client_pool) ===
raylet_client_pool_cpp_lib_dependencies = [
    # Package-managed
    raylet_client_interface_cpp_lib_dep,
    # `gcs_client_cpp_lib_dep` is NOT AVAILABLE (processed after): //src/ray/gcs_rpc_client:gcs_client
]

raylet_client_pool_cpp_lib = static_library(
    'raylet_client_pool_cpp_lib',
    ['raylet_client_pool.cc'],
    install: false,
    dependencies: raylet_client_pool_cpp_lib_dependencies
)

raylet_client_pool_cpp_lib_dep = declare_dependency(
    link_with: [raylet_client_pool_cpp_lib],
    dependencies: raylet_client_pool_cpp_lib_dependencies
)
# === @end: raylet_client_pool_cpp_lib ===


# === @begin: raylet_client_with_io_context_cpp_lib (@original: raylet_client_with_io_context_lib) ===
raylet_client_with_io_context_cpp_lib_dependencies = [
    # Package-managed
    raylet_client_interface_cpp_lib_dep,
    raylet_client_lib_cpp_lib_dep,
    bundle_spec_cpp_lib_dep,
    ray_config_cpp_lib_dep,
    node_manager_proto_cpp_grpc_lib_dep,
    retryable_grpc_client_cpp_lib_dep,
    rpc_callback_types_cpp_lib_dep,
    logging_cpp_lib_dep,
]

raylet_client_with_io_context_cpp_lib = static_library(
    'raylet_client_with_io_context_cpp_lib',
    ['raylet_client_with_io_context.cc'],
    install: false,
    dependencies: raylet_client_with_io_context_cpp_lib_dependencies
)

raylet_client_with_io_context_cpp_lib_dep = declare_dependency(
    link_with: [raylet_client_with_io_context_cpp_lib],
    dependencies: raylet_client_with_io_context_cpp_lib_dependencies
)
# === @end: raylet_client_with_io_context_cpp_lib ===


# Umbrella dependency — backward compatible with consumers using the consolidated target
raylet_client_cpp_lib_dep = declare_dependency(
    dependencies: [
        raylet_client_interface_cpp_lib_dep,
        raylet_client_lib_cpp_lib_dep,
        raylet_client_pool_cpp_lib_dep,
        raylet_client_with_io_context_cpp_lib_dep,
    ]
)
