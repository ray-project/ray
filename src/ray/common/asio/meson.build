# === @begin: asio_cpp_lib (originial: asio) ===
asio_cpp_lib_dependencies = [
    # Project-managed
    ray_config_cpp_lib_dep,
    util_cpp_lib_dep,
    event_stats_cpp_lib_dep,

    # Eugo-managed
    boost_asio,

    absl_container_flat_hash_map,
    absl_synchronization
]

asio_cpp_lib = static_library(
    'asio_cpp_lib',
    [
        'asio_chaos.cc',
        'instrumented_io_context.cc',
        'io_service_pool.cc',
        'periodical_runner.cc'
    ],
    install: false,
    dependencies: asio_cpp_lib_dependencies
)

asio_cpp_lib_dep = declare_dependency(
    link_with: [asio_cpp_lib],
    dependencies: asio_cpp_lib_dependencies
)
# === @end: asio_cpp_lib (originial: asio) ===
