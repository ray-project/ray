subdir('protobuf/')
subdir('thirdparty/')
subdir('util/')
subdir('stats/')

# We can't traverse the entire `raylet` tree, as it includes parts which depend on targets from `common`.
subdir('raylet/format/')

subdir('common/')

subdir('rpc/')

# @Important: due to circular dependency between `stats` and `rpc` folders, this guy should go here
# === @begin: stats/stats_cpp_lib (original: stats_lib) ===
stats_cpp_lib_private_deps = [
    # Package-managed
    reporter_rpc_cpp_lib_dep,
    stats_metric_cpp_lib_dep
]

stats_cpp_lib_public_deps = [
    # Eugo-managed
    grpc,

    # System-managed
    threads
]

stats_cpp_lib = static_library(
    'stats_cpp_lib',
    ['stats/metric_exporter.cc'],
    install: false,
    dependencies: [
        stats_cpp_lib_private_deps,
        stats_cpp_lib_public_deps
    ]
)

stats_cpp_lib_dep = declare_dependency(
    link_with: [stats_cpp_lib],
    dependencies: [stats_cpp_lib_public_deps]
)
# === @end: stats/stats_cpp_lib (original: stats_lib) ===

subdir('object_manager/')

subdir('pubsub/')

subdir('raylet_client/')

subdir('gcs/')

# We can't traverse the entire `raylet` tree, as it includes parts which depend on targets from `gcs`.
subdir('raylet/scheduling/')

# Such division is required to avoid circular raptors
subdir('gcs/gcs_server/')

#subdir('raylet/')
