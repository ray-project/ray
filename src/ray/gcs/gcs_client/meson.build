# === @begin: gcs_client_cpp_lib (original: gcs_client_lib) ===
gcs_client_cpp_lib_dependencies = [
    # Package-managed
    gcs_cpp_lib_dep,
    gcs_pub_sub_cpp_lib_dep,
    gcs_service_rpc_cpp_lib_dep,
    pubsub_cpp_lib_dep,
    ray_common_cpp_lib_dep,
    redis_store_client_cpp_lib_dep,
    usage_proto_cpp_lib_dep
]

gcs_client_cpp_lib = static_library(
    'gcs_client_cpp_lib',
    ['accessor.cc', 'gcs_client.cc'],
    install: false,
    dependencies: gcs_client_cpp_lib_dependencies
)

gcs_client_cpp_lib_dep = declare_dependency(
    link_with: [gcs_client_cpp_lib],
    dependencies: gcs_client_cpp_lib_dependencies
)
# === @end: gcs_client_cpp_lib (original: gcs_client_lib) ===


# === @begin: global_state_accessor_cpp_lib (original: global_state_accessor_lib) ===
global_state_accessor_cpp_lib_dependencies = [
    # Package-managed
    gcs_client_cpp_lib_dep
]

global_state_accessor_cpp_lib = static_library(
    'global_state_accessor_cpp_lib',
    ['global_state_accessor.cc'],
    install: false,
    dependencies: global_state_accessor_cpp_lib_dependencies
)

global_state_accessor_cpp_lib_dep = declare_dependency(
    link_with: [global_state_accessor_cpp_lib],
    dependencies: global_state_accessor_cpp_lib_dependencies
)
# === @end: global_state_accessor_cpp_lib (original: global_state_accessor_lib) ===


# === @begin: python_callbacks_cpp_lib (original: python_callbacks) ===
# header-only
python_callbacks_cpp_lib_dep = declare_dependency(
    sources: ['python_callbacks.h'],
    dependencies: global_state_accessor_cpp_lib_dependencies
)
# === @end: python_callbacks_cpp_lib (original: python_callbacks) ===
