# === @begin: raylet_client_cpp_lib (original: raylet_client_lib) ===
raylet_client_cpp_lib_private_deps = [
    # Package-managed
    node_manager_cpp_fbs_lib_dep,
    node_manager_rpc_cpp_lib_dep,
    ray_common_cpp_lib_dep,
    gcs_proto_cpp_lib_dep,
    util_cpp_lib_dep,
]

raylet_client_cpp_lib_public_deps = [
    # Eugo-managed
    boost_asio,

    # System-managed
    threads
]

raylet_client_cpp_lib = static_library(
    'raylet_client_cpp_lib',
    ['raylet_client.cc'],
    install: false,
    dependencies: [
        raylet_client_cpp_lib_private_deps,
        raylet_client_cpp_lib_public_deps
    ]
)

raylet_client_cpp_lib_dep = declare_dependency(
    link_with: [raylet_client_cpp_lib],
    dependencies: raylet_client_cpp_lib_public_deps
)
# === @end: raylet_client_cpp_lib (original: raylet_client_lib) ===
