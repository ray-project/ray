# === @begin: stats_metric (original: stats_metric) ===
stats_metric_cpp_lib = static_library(
    'stats_metric_cpp_lib',
    ['metric.cc', 'metric_defs.cc', 'tag_defs.cc'],
    install: false,
    dependencies: [
        # Package-managed
        util_cpp_lib_dep,

        # Eugo-managed
        prometheus_cpp_pull,

        absl_base_core_headers,
        absl_container_flat_hash_map,
        absl_memory,
        absl_strings,

        #    "@com_google_googletest//:gtest",

        opencensus_cpp_prometheus_exporter,
        opencensus_cpp_stdout_exporter,
        opencensus_cpp_stats,
        opencensus_cpp_tags
    ]
)

stats_metric_cpp_lib_dep = declare_dependency(
    link_with: [stats_metric_cpp_lib]
)
# === @end: stats_metric (original: stats_metric) ===
