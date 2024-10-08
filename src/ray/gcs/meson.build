# === @begin: gcs_cpp_lib (original: gcs) ===
gcs_cpp_lib_private_deps = [
    # Package-managed
    node_manager_cpp_fbs_lib_dep,
    node_manager_rpc_cpp_lib_dep,
    ray_common_cpp_lib_dep,
    stats_cpp_lib_dep,
    agent_manager_proto_cpp_lib_dep,
    gcs_proto_cpp_lib_dep,
    gcs_service_proto_cpp_lib_dep,
    util_cpp_lib_dep,
]

gcs_cpp_lib_public_deps = [
    # Eugo-managed
    hiredis,
    boost_asio
]

gcs_cpp_lib = static_library(
    'gcs_cpp_lib',
    [
        'asio.cc',
        'redis_async_context.cc',
        'redis_client.cc',
        'redis_context.cc'
    ],
    install: false,
    dependencies: [
        gcs_cpp_lib_private_deps,
        gcs_cpp_lib_public_deps
    ]
)

gcs_cpp_lib_dep = declare_dependency(
    link_with: [gcs_cpp_lib],
    dependencies: gcs_cpp_lib_public_deps
)
# === @end: gcs_cpp_lib (original: gcs) ===


# === @begin: redis_client_cpp_lib (original: redis_client) ===
redis_client_cpp_lib_private_deps = [
    # Package-managed
    ray_common_cpp_lib_dep,
    stats_cpp_lib_dep,
    util_cpp_lib_dep
]

redis_client_cpp_lib_public_deps = [
    # Eugo-managed
    hiredis,
    boost_asio
]

redis_client_cpp_lib = static_library(
    'redis_client_cpp_lib',
    [
        'asio.cc',
        'redis_async_context.cc',
        'redis_client.cc',
        'redis_context.cc'
    ],
    install: false,
    dependencies: [
        redis_client_cpp_lib_private_deps,
        redis_client_cpp_lib_public_deps
    ]
)

redis_client_cpp_lib_dep = declare_dependency(
    link_with: [redis_client_cpp_lib],
    dependencies: redis_client_cpp_lib_public_deps
)
# === @end: redis_client_cpp_lib (original: redis_client) ===


# === @begin: pubsub/ ===
subdir('pubsub/')
# === @end: pubsub/ ===


# === @begin: pubsub/ ===
subdir('store_client/')
# === @end: pubsub/ ===



