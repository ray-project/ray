# === @begin: scheduler_cpp_lib (original: scheduler) ===
scheduler_cpp_lib_dependencies = [
    # Package-managed
    gcs_client_cpp_lib_dep,
    ray_common_cpp_lib_dep,
    stats_cpp_lib_dep,
    common_proto_cpp_lib_dep,
    util_cpp_lib_dep,

    # Eugo-managed
    boost_asio,
    boost_system,
    prometheus_cpp_pull,
    absl_base_core_headers,
    absl_container_flat_hash_set,
    absl_memory,
    absl_random_random,
    absl_random_bit_gen_ref,
    absl_strings,
    #"@com_google_googletest//:gtest",
    opencensus_cpp_prometheus_exporter,
    opencensus_cpp_stats,
    opencensus_cpp_tags,

    # System-managed
    threads
]

scheduler_cpp_lib = static_library(
    'scheduler_cpp_lib',
    [
        'scheduling_policy.cc',
        'scheduler_resource_reporter.cc',
        'scheduler_stats.cc',
        'cluster_resource_scheduler.cc',
        'cluster_resource_manager.cc',
        'cluster_task_manager.cc',
        'local_resource_manager.cc',

        'policy/node_affinity_scheduling_policy.cc',
        'policy/scorer.cc',
        'policy/affinity_with_bundle_scheduling_policy.cc',
        'policy/random_scheduling_policy.cc',
        'policy/hybrid_scheduling_policy.cc',
        'policy/spread_scheduling_policy.cc',
        'policy/node_label_scheduling_policy.cc',
        'policy/bundle_scheduling_policy.cc',
        'policy/composite_scheduling_policy.cc'
    ],
    install: false,
    dependencies: scheduler_cpp_lib_dependencies
)

scheduler_cpp_lib_dep = declare_dependency(
    link_with: [scheduler_cpp_lib],
    dependencies: scheduler_cpp_lib_dependencies
)
# === @end: scheduler_cpp_lib (original: scheduler) ===

