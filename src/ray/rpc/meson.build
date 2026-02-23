# === @begin: authentication/ ===
# @IMPORTANT: authentication must come BEFORE rpc targets because `grpc_client.cc`,
# `server_call.cc`, and `grpc_server.cc` all depend on authentication targets.
subdir('authentication/')
# === @end: authentication/ ===


# === @begin: rpc_common_cpp_lib (@original: common) ===
rpc_common_cpp_lib = static_library(
    'rpc_common_cpp_lib',
    ['common.cc'],
    install: false,
)

rpc_common_cpp_lib_dep = declare_dependency(
    link_with: [rpc_common_cpp_lib],
)
# === @end: rpc_common_cpp_lib ===


# === @begin: rpc_callback_types_cpp_lib (@original: rpc_callback_types) ===
# Header-only
rpc_callback_types_cpp_lib_dependencies = [
    # Package-managed
    status_cpp_lib_dep,
]

rpc_callback_types_cpp_lib_dep = declare_dependency(
    sources: ['rpc_callback_types.h'],
    dependencies: rpc_callback_types_cpp_lib_dependencies
)
# === @end: rpc_callback_types_cpp_lib ===


# === @begin: rpc_metrics_cpp_lib (@original: metrics) ===
# Header-only
rpc_metrics_cpp_lib_dependencies = [
    # Package-managed
    stats_metric_cpp_lib_dep,
]

rpc_metrics_cpp_lib_dep = declare_dependency(
    sources: ['metrics.h'],
    dependencies: rpc_metrics_cpp_lib_dependencies
)
# === @end: rpc_metrics_cpp_lib ===


# === @begin: rpc_chaos_cpp_lib (@original: rpc_chaos) ===
rpc_chaos_cpp_lib_dependencies = [
    # Package-managed
    ray_config_cpp_lib_dep,

    # Eugo-managed
    absl_container_flat_hash_map,
    absl_synchronization,
    nlohmann_json,
]

rpc_chaos_cpp_lib = static_library(
    'rpc_chaos_cpp_lib',
    ['rpc_chaos.cc'],
    install: false,
    dependencies: rpc_chaos_cpp_lib_dependencies
)

rpc_chaos_cpp_lib_dep = declare_dependency(
    link_with: [rpc_chaos_cpp_lib],
    dependencies: rpc_chaos_cpp_lib_dependencies
)
# === @end: rpc_chaos_cpp_lib ===


# === @begin: client_call_cpp_lib (@original: client_call) ===
# Header-only
client_call_cpp_lib_dependencies = [
    # Package-managed
    rpc_metrics_cpp_lib_dep,
    rpc_callback_types_cpp_lib_dep,
    asio_cpp_lib_dep,
    grpc_util_cpp_lib_dep,
    id_cpp_lib_dep,
    status_cpp_lib_dep,

    # Eugo-managed
    absl_synchronization,
]

client_call_cpp_lib_dep = declare_dependency(
    sources: ['client_call.h'],
    dependencies: client_call_cpp_lib_dependencies
)
# === @end: client_call_cpp_lib ===


# === @begin: grpc_client_cpp_lib (@original: grpc_client) ===
grpc_client_cpp_lib_dependencies = [
    # Package-managed
    client_call_cpp_lib_dep,
    rpc_common_cpp_lib_dep,
    rpc_chaos_cpp_lib_dep,
    constants_cpp_lib_dep,
    grpc_util_cpp_lib_dep,
    ray_config_cpp_lib_dep,
    status_cpp_lib_dep,
    authentication_mode_cpp_lib_dep,
    token_auth_client_interceptor_cpp_lib_dep,
    network_util_cpp_lib_dep,
]

grpc_client_cpp_lib = static_library(
    'grpc_client_cpp_lib',
    ['grpc_client.cc'],
    install: false,
    dependencies: grpc_client_cpp_lib_dependencies
)

grpc_client_cpp_lib_dep = declare_dependency(
    link_with: [grpc_client_cpp_lib],
    dependencies: grpc_client_cpp_lib_dependencies
)
# === @end: grpc_client_cpp_lib ===


# === @begin: retryable_grpc_client_cpp_lib (@original: retryable_grpc_client) ===
retryable_grpc_client_cpp_lib_dependencies = [
    # Package-managed
    grpc_client_cpp_lib_dep,
    exponential_backoff_cpp_lib_dep,

    # Eugo-managed
    absl_container_btree,
    absl_strings,
    absl_time,
]

retryable_grpc_client_cpp_lib = static_library(
    'retryable_grpc_client_cpp_lib',
    ['retryable_grpc_client.cc'],
    install: false,
    dependencies: retryable_grpc_client_cpp_lib_dependencies
)

retryable_grpc_client_cpp_lib_dep = declare_dependency(
    link_with: [retryable_grpc_client_cpp_lib],
    dependencies: retryable_grpc_client_cpp_lib_dependencies
)
# === @end: retryable_grpc_client_cpp_lib ===


# === @begin: server_call_cpp_lib (@original: server_call) ===
server_call_cpp_lib_dependencies = [
    # Package-managed
    rpc_metrics_cpp_lib_dep,
    rpc_callback_types_cpp_lib_dep,
    asio_cpp_lib_dep,
    grpc_util_cpp_lib_dep,
    id_cpp_lib_dep,
    ray_config_cpp_lib_dep,
    status_cpp_lib_dep,
    authentication_mode_cpp_lib_dep,
    authentication_token_cpp_lib_dep,
    authentication_token_loader_cpp_lib_dep,
    authentication_token_validator_cpp_lib_dep,
    stats_metric_cpp_lib_dep,

    # Eugo-managed
    grpc,
]

server_call_cpp_lib = static_library(
    'server_call_cpp_lib',
    ['server_call.cc'],
    install: false,
    dependencies: server_call_cpp_lib_dependencies
)

server_call_cpp_lib_dep = declare_dependency(
    link_with: [server_call_cpp_lib],
    dependencies: server_call_cpp_lib_dependencies
)
# === @end: server_call_cpp_lib ===


# === @begin: grpc_server_cpp_lib (@original: grpc_server) ===
grpc_server_cpp_lib_dependencies = [
    # Package-managed
    rpc_common_cpp_lib_dep,
    server_call_cpp_lib_dep,
    asio_cpp_lib_dep,
    ray_config_cpp_lib_dep,
    status_cpp_lib_dep,
    authentication_token_loader_cpp_lib_dep,
    network_util_cpp_lib_dep,
    thread_utils_cpp_lib_dep,

    # Eugo-managed
    grpc,  # covers @com_github_grpc_grpc//:grpc++, :grpc++_reflection, :grpcpp_admin
]

grpc_server_cpp_lib = static_library(
    'grpc_server_cpp_lib',
    ['grpc_server.cc'],
    install: false,
    dependencies: grpc_server_cpp_lib_dependencies
)

grpc_server_cpp_lib_dep = declare_dependency(
    link_with: [grpc_server_cpp_lib],
    dependencies: grpc_server_cpp_lib_dependencies
)
# === @end: grpc_server_cpp_lib ===


# === @begin: rpc_utils_cpp_lib (@original: utils) ===
# Header-only
rpc_utils_cpp_lib_dependencies = [
    # Package-managed
    common_proto_cpp_lib_dep,

    # Eugo-managed
    protobuf,
]

rpc_utils_cpp_lib_dep = declare_dependency(
    sources: ['utils.h'],
    dependencies: rpc_utils_cpp_lib_dependencies
)
# === @end: rpc_utils_cpp_lib ===


# Umbrella dependency — backward compatible with consumers using the consolidated grpc_common target
grpc_common_cpp_lib_dep = declare_dependency(
    dependencies: [
        rpc_common_cpp_lib_dep,
        rpc_callback_types_cpp_lib_dep,
        rpc_metrics_cpp_lib_dep,
        rpc_chaos_cpp_lib_dep,
        client_call_cpp_lib_dep,
        grpc_client_cpp_lib_dep,
        retryable_grpc_client_cpp_lib_dep,
        server_call_cpp_lib_dep,
        grpc_server_cpp_lib_dep,
        rpc_utils_cpp_lib_dep,
    ]
)


# === @begin: node_manager/ ===
subdir('node_manager/')
# === @end: node_manager/ ===


# === @begin: node_manager_server_cpp_lib (@original: node_manager_server) ===
# Header-only
# @IMPORTANT: the actual header lives at node_manager/node_manager_server.h but the Bazel target
# is in `/src/ray/rpc/BUILD.bazel`. Defined here after node_manager/ subdir for include path.
node_manager_server_cpp_lib_dependencies = [
    # Package-managed
    grpc_server_cpp_lib_dep,
    node_manager_proto_cpp_grpc_lib_dep,
    authentication_token_cpp_lib_dep,

    # Eugo-managed
    grpc,
]

node_manager_server_cpp_lib_dep = declare_dependency(
    sources: ['node_manager/node_manager_server.h'],
    dependencies: node_manager_server_cpp_lib_dependencies
)
# === @end: node_manager_server_cpp_lib ===


# === @begin: object_manager_server_cpp_lib (@original: object_manager_server) ===
# Header-only
object_manager_server_cpp_lib_dependencies = [
    # Package-managed
    asio_cpp_lib_dep,
    # `object_manager_grpc_client_manager_cpp_lib_dep` is NOT AVAILABLE (processed after): `//src/ray/object_manager:object_manager_grpc_client_manager`
    object_manager_proto_cpp_grpc_lib_dep,
    grpc_server_cpp_lib_dep,
    authentication_token_cpp_lib_dep,

    # Eugo-managed
    boost_asio,
    grpc,
]

object_manager_server_cpp_lib_dep = declare_dependency(
    sources: ['object_manager_server.h'],
    dependencies: object_manager_server_cpp_lib_dependencies
)
# backward compat alias
object_manager_rpc_cpp_lib_dep = object_manager_server_cpp_lib_dep
# === @end: object_manager_server_cpp_lib ===


# === @begin: metrics_agent_client_cpp_lib (@original: metrics_agent_client) ===
metrics_agent_client_cpp_lib_dependencies = [
    # Package-managed
    grpc_client_cpp_lib_dep,
    reporter_proto_cpp_grpc_lib_dep,
    reporter_proto_cpp_lib_dep,
    logging_cpp_lib_dep,
    network_util_cpp_lib_dep,

    # Eugo-managed
    grpc,
]

metrics_agent_client_cpp_lib = static_library(
    'metrics_agent_client_cpp_lib',
    ['metrics_agent_client.cc'],
    install: false,
    dependencies: metrics_agent_client_cpp_lib_dependencies
)

metrics_agent_client_cpp_lib_dep = declare_dependency(
    link_with: [metrics_agent_client_cpp_lib],
    dependencies: metrics_agent_client_cpp_lib_dependencies
)
# backward compat alias
reporter_rpc_cpp_lib_dep = metrics_agent_client_cpp_lib_dep
# === @end: metrics_agent_client_cpp_lib ===


# === @begin: event_aggregator_client_cpp_lib (@original: event_aggregator_client) ===
# Header-only
event_aggregator_client_cpp_lib_dependencies = [
    # Package-managed
    grpc_client_cpp_lib_dep,
    events_event_aggregator_service_proto_cpp_grpc_lib_dep,
    logging_cpp_lib_dep,

    # Eugo-managed
    grpc,
]

event_aggregator_client_cpp_lib_dep = declare_dependency(
    sources: ['event_aggregator_client.h'],
    dependencies: event_aggregator_client_cpp_lib_dependencies
)
# === @end: event_aggregator_client_cpp_lib ===


# === @begin: gcs_service_rpc_cpp_lib (@original: convenience aggregate) ===
# NOTE: Not a direct 1:1 Bazel target — convenience aggregation for gcs service RPC deps
gcs_service_rpc_cpp_lib_dependencies = [
    # Package-managed
    autoscaler_proto_cpp_grpc_lib_dep,
    gcs_service_proto_cpp_grpc_lib_dep,
    grpc_common_cpp_lib_dep,
    pubsub_proto_cpp_lib_dep,
    ray_common_cpp_lib_dep,

    # Eugo-managed
    boost_asio,
    grpc,
    absl_container_btree,
]

gcs_service_rpc_cpp_lib_dep = declare_dependency(
    dependencies: gcs_service_rpc_cpp_lib_dependencies
)
# === @end: gcs_service_rpc_cpp_lib ===


# === @begin: pubsub_rpc_cpp_lib (@original: convenience aggregate) ===
pubsub_rpc_cpp_lib_dep = declare_dependency(
    dependencies: [
        pubsub_proto_cpp_grpc_lib_dep,
        grpc_common_cpp_lib_dep,
        ray_common_cpp_lib_dep,
    ]
)
# === @end: pubsub_rpc_cpp_lib ===


# === @begin: autoscaler_rpc_cpp_lib (@original: convenience aggregate) ===
autoscaler_rpc_cpp_lib_dep = declare_dependency(
    dependencies: [autoscaler_proto_cpp_grpc_lib_dep]
)
# === @end: autoscaler_rpc_cpp_lib ===
