# === @begin: accessor_factory_interface_cpp_lib (@original: accessor_factory_interface) ===
accessor_factory_interface_cpp_lib_dep = declare_dependency(
    sources: ['accessor_factory_interface.h'],
)
# === @end: accessor_factory_interface_cpp_lib ===


# === @begin: rpc_client_cpp_lib (@original: rpc_client) ===
# Header-only
rpc_client_cpp_lib_dependencies = [
    # Package-managed
    ray_config_cpp_lib_dep,
    autoscaler_proto_cpp_grpc_lib_dep,
    gcs_service_proto_cpp_grpc_lib_dep,
    retryable_grpc_client_cpp_lib_dep,
    rpc_callback_types_cpp_lib_dep,
    network_util_cpp_lib_dep,
]

rpc_client_cpp_lib_dep = declare_dependency(
    sources: ['rpc_client.h'],
    dependencies: rpc_client_cpp_lib_dependencies
)
# === @end: rpc_client_cpp_lib ===


# === @begin: actor_accessor_interface_cpp_lib (@original: actor_accessor_interface) ===
# Header-only
actor_accessor_interface_cpp_lib_dependencies = [
    # Package-managed
    id_cpp_lib_dep,
    ray_common_cpp_lib_dep,     # placement_group, task_common (consolidated),
    autoscaler_proto_cpp_lib_dep,
    gcs_proto_cpp_lib_dep,
    rpc_callback_types_cpp_lib_dep,
]

actor_accessor_interface_cpp_lib_dep = declare_dependency(
    sources: ['accessors/actor_info_accessor_interface.h'],
    dependencies: actor_accessor_interface_cpp_lib_dependencies
)
# === @end: actor_accessor_interface_cpp_lib ===


# === @begin: gcs_client_context_cpp_lib (@original: gcs_client_context) ===
# Header-only
gcs_client_context_cpp_lib_dependencies = [
    # Package-managed
    rpc_client_cpp_lib_dep,
    gcs_subscriber_cpp_lib_dep,
]

gcs_client_context_cpp_lib_dep = declare_dependency(
    sources: ['gcs_client_context.h'],
    dependencies: gcs_client_context_cpp_lib_dependencies
)
# === @end: gcs_client_context_cpp_lib ===


# === @begin: actor_accessor_implementation_cpp_lib (@original: actor_accessor_implementation) ===
actor_accessor_implementation_cpp_lib_dependencies = [
    # Package-managed
    actor_accessor_interface_cpp_lib_dep,
    gcs_client_context_cpp_lib_dep,
    rpc_client_cpp_lib_dep,
    asio_cpp_lib_dep,
    id_cpp_lib_dep,
    ray_common_cpp_lib_dep,     # placement_group, protobuf_utils (consolidated),
    redis_store_client_cpp_lib_dep,
    usage_proto_cpp_lib_dep,
    gcs_subscriber_cpp_lib_dep,
    subscriber_cpp_lib_dep,
    container_util_cpp_lib_dep,
    network_util_cpp_lib_dep,
    sequencer_cpp_lib_dep,

    # Eugo-managed
    absl_container_flat_hash_map,
    absl_container_flat_hash_set,
]

actor_accessor_implementation_cpp_lib = static_library(
    'actor_accessor_implementation_cpp_lib',
    ['accessors/actor_info_accessor.cc'],
    install: false,
    dependencies: actor_accessor_implementation_cpp_lib_dependencies
)

actor_accessor_implementation_cpp_lib_dep = declare_dependency(
    link_with: [actor_accessor_implementation_cpp_lib],
    dependencies: actor_accessor_implementation_cpp_lib_dependencies
)
# === @end: actor_accessor_implementation_cpp_lib ===


# === @begin: gcs_client_defaults_cpp_lib (@original: gcs_client_defaults) ===
gcs_client_defaults_cpp_lib_dependencies = [
    # Package-managed
    accessor_factory_interface_cpp_lib_dep,
    actor_accessor_implementation_cpp_lib_dep,
    actor_accessor_interface_cpp_lib_dep,
    gcs_client_context_cpp_lib_dep,
]

gcs_client_defaults_cpp_lib = static_library(
    'gcs_client_defaults_cpp_lib',
    [
        'default_accessor_factory.cc',
        'default_gcs_client_context.cc',
    ],
    install: false,
    dependencies: gcs_client_defaults_cpp_lib_dependencies
)

gcs_client_defaults_cpp_lib_dep = declare_dependency(
    link_with: [gcs_client_defaults_cpp_lib],
    dependencies: gcs_client_defaults_cpp_lib_dependencies
)
# === @end: gcs_client_defaults_cpp_lib ===


# === @begin: gcs_client_target_cpp_lib (@original: gcs_client) ===
gcs_client_target_cpp_lib_dependencies = [
    # Package-managed
    accessor_factory_interface_cpp_lib_dep,
    actor_accessor_interface_cpp_lib_dep,
    gcs_client_context_cpp_lib_dep,
    gcs_client_defaults_cpp_lib_dep,
    rpc_client_cpp_lib_dep,
    asio_cpp_lib_dep,
    id_cpp_lib_dep,
    ray_common_cpp_lib_dep,     # placement_group, protobuf_utils (consolidated),
    redis_store_client_cpp_lib_dep,
    usage_proto_cpp_lib_dep,
    gcs_subscriber_cpp_lib_dep,
    subscriber_cpp_lib_dep,
    container_util_cpp_lib_dep,
    network_util_cpp_lib_dep,
    sequencer_cpp_lib_dep,
]

gcs_client_target_cpp_lib = static_library(
    'gcs_client_target_cpp_lib',
    [
        'accessor.cc',
        'gcs_client.cc',
    ],
    install: false,
    dependencies: gcs_client_target_cpp_lib_dependencies
)

gcs_client_target_cpp_lib_dep = declare_dependency(
    link_with: [gcs_client_target_cpp_lib],
    dependencies: gcs_client_target_cpp_lib_dependencies
)
# === @end: gcs_client_target_cpp_lib ===


# Umbrella dependency — backward compatible with consumers using the consolidated target
gcs_client_cpp_lib_dep = declare_dependency(
    dependencies: [
        accessor_factory_interface_cpp_lib_dep,
        rpc_client_cpp_lib_dep,
        actor_accessor_interface_cpp_lib_dep,
        gcs_client_context_cpp_lib_dep,
        actor_accessor_implementation_cpp_lib_dep,
        gcs_client_defaults_cpp_lib_dep,
        gcs_client_target_cpp_lib_dep,
    ]
)


# === @begin: global_state_accessor_cpp_lib (@original: global_state_accessor_lib) ===
global_state_accessor_cpp_lib_dependencies = [
    # Package-managed
    gcs_client_cpp_lib_dep,
    time_cpp_lib_dep,
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
# === @end: global_state_accessor_cpp_lib ===


# === @begin: python_callbacks_cpp_lib (original: python_callbacks) ===
# Header-only — python_callbacks.h is now in common/ but this dep is kept for
# backward compatibility with downstream consumers
python_callbacks_cpp_lib_dep = declare_dependency(
    dependencies: global_state_accessor_cpp_lib_dependencies
)
# === @end: python_callbacks_cpp_lib ===
