# === @begin: publisher_interface_cpp_lib (@original: publisher_interface) ===
publisher_interface_cpp_lib_dep = declare_dependency(
    sources: ['publisher_interface.h'],
    dependencies: [
        id_cpp_lib_dep,
        pubsub_proto_cpp_grpc_lib_dep,
        rpc_callback_types_cpp_lib_dep,
    ]
)
# === @end: publisher_interface_cpp_lib ===


# === @begin: publisher_cpp_lib (@original: publisher) ===
publisher_cpp_lib_dependencies = [
    asio_cpp_lib_dep,
    pubsub_proto_cpp_grpc_lib_dep,
    publisher_interface_cpp_lib_dep,
    rpc_callback_types_cpp_lib_dep,
    absl_container_flat_hash_map,
    absl_container_flat_hash_set,
    absl_synchronization,
]

publisher_cpp_lib = static_library(
    'publisher_cpp_lib',
    ['publisher.cc'],
    install: false,
    dependencies: publisher_cpp_lib_dependencies
)

publisher_cpp_lib_dep = declare_dependency(
    link_with: [publisher_cpp_lib],
    dependencies: publisher_cpp_lib_dependencies
)
# === @end: publisher_cpp_lib ===


# === @begin: subscriber_interface_cpp_lib (@original: subscriber_interface) ===
subscriber_interface_cpp_lib_dep = declare_dependency(
    sources: ['subscriber_interface.h'],
    dependencies: [
        id_cpp_lib_dep,
        common_proto_cpp_lib_dep,
        pubsub_proto_cpp_grpc_lib_dep,
        rpc_callback_types_cpp_lib_dep,
        grpc,
    ]
)
# === @end: subscriber_interface_cpp_lib ===


# === @begin: subscriber_cpp_lib (@original: subscriber) ===
subscriber_cpp_lib_dependencies = [
    asio_cpp_lib_dep,
    pubsub_proto_cpp_grpc_lib_dep,
    subscriber_interface_cpp_lib_dep,
    rpc_callback_types_cpp_lib_dep,
    absl_container_flat_hash_map,
    absl_container_flat_hash_set,
    absl_synchronization,
]

subscriber_cpp_lib = static_library(
    'subscriber_cpp_lib',
    ['subscriber.cc'],
    install: false,
    dependencies: subscriber_cpp_lib_dependencies
)

subscriber_cpp_lib_dep = declare_dependency(
    link_with: [subscriber_cpp_lib],
    dependencies: subscriber_cpp_lib_dependencies
)
# === @end: subscriber_cpp_lib ===


# === @begin: gcs_publisher_cpp_lib (@original: gcs_publisher) ===
gcs_publisher_cpp_lib_dependencies = [
    publisher_interface_cpp_lib_dep,
    gcs_proto_cpp_lib_dep,
]

gcs_publisher_cpp_lib = static_library(
    'gcs_publisher_cpp_lib',
    ['gcs_publisher.cc'],
    install: false,
    dependencies: gcs_publisher_cpp_lib_dependencies
)

gcs_publisher_cpp_lib_dep = declare_dependency(
    link_with: [gcs_publisher_cpp_lib],
    dependencies: gcs_publisher_cpp_lib_dependencies
)
# === @end: gcs_publisher_cpp_lib ===


# === @begin: gcs_subscriber_cpp_lib (@original: gcs_subscriber) ===
gcs_subscriber_cpp_lib_dependencies = [
    subscriber_interface_cpp_lib_dep,
    gcs_proto_cpp_lib_dep,
    rpc_callback_types_cpp_lib_dep,
]

gcs_subscriber_cpp_lib = static_library(
    'gcs_subscriber_cpp_lib',
    ['gcs_subscriber.cc'],
    install: false,
    dependencies: gcs_subscriber_cpp_lib_dependencies
)

gcs_subscriber_cpp_lib_dep = declare_dependency(
    link_with: [gcs_subscriber_cpp_lib],
    dependencies: gcs_subscriber_cpp_lib_dependencies
)
# === @end: gcs_subscriber_cpp_lib ===


# === @begin: python_gcs_subscriber_cpp_lib (@original: python_gcs_subscriber) ===
# NOTE: Bazel deps include gcs_rpc_client:rpc_client which is processed after pubsub/.
# Proto/RPC deps listed directly instead — provides equivalent functionality.
python_gcs_subscriber_cpp_lib_dependencies = [
    status_cpp_lib_dep,
    gcs_service_proto_cpp_lib_dep,
    pubsub_proto_cpp_lib_dep,
    visibility_cpp_lib_dep,
    retryable_grpc_client_cpp_lib_dep,
    rpc_callback_types_cpp_lib_dep,
    grpc,
    absl_synchronization,
]

python_gcs_subscriber_cpp_lib = static_library(
    'python_gcs_subscriber_cpp_lib',
    ['python_gcs_subscriber.cc'],
    install: false,
    dependencies: python_gcs_subscriber_cpp_lib_dependencies
)

python_gcs_subscriber_cpp_lib_dep = declare_dependency(
    link_with: [python_gcs_subscriber_cpp_lib],
    dependencies: python_gcs_subscriber_cpp_lib_dependencies
)
# === @end: python_gcs_subscriber_cpp_lib ===


# Umbrella dependency — backward compatible with consumers using the consolidated target
pubsub_cpp_lib_dep = declare_dependency(
    dependencies: [
        publisher_interface_cpp_lib_dep,
        publisher_cpp_lib_dep,
        subscriber_interface_cpp_lib_dep,
        subscriber_cpp_lib_dep,
        gcs_publisher_cpp_lib_dep,
        gcs_subscriber_cpp_lib_dep,
        python_gcs_subscriber_cpp_lib_dep,
    ]
)
