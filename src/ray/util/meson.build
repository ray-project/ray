# === @begin: util_cpp_lib (original: util) ===
util_cpp_lib_private_deps = [
    # Project-managed
    aligned_alloc_cpp_lib_dep,
    sha256_cpp_lib_dep,
    event_proto_cpp_lib_dep,
    export_event_proto_cpp_lib_dep,
]

util_cpp_lib_public_deps = [
    # Eugo-managed
    boost_asio,
    spdlog,

    absl_container_flat_hash_map,
    absl_container_flat_hash_set,
    absl_debugging_failure_signal_handler,
    absl_debugging_stacktrace,
    absl_debugging_symbolize,
    absl_random_random,
    absl_synchronization,
    absl_time,

    # @com_google_googletest//:gtest_main"

    protobuf,
    nlohmann_json,

    # OS-managed
    threads
]

util_cpp_lib = static_library(
    'util_cpp_lib',
    [
        'event.cc',
        'exponential_backoff.cc',
        'filesystem.cc',
        'logging.cc',
        'memory.cc',
        'process.cc',
        'subreaper.cc',
        'util.cc',
    ],
    dependencies: [
        util_cpp_lib_private_deps,
        util_cpp_lib_public_deps
    ],
    install: false
)

util_cpp_lib_dep = declare_dependency(
    link_with: [util_cpp_lib],
    dependencies: util_cpp_lib_public_deps
)
# === @end: util_cpp_lib (original: util) ===