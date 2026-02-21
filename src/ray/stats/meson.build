# =============================================================================
# stats/meson.build — 1:1 Bazel-to-Meson translation
#
# Bazel targets in BUILD.bazel:
#   1. tag_defs      (static lib)
#   2. stats_metric  (static lib)
#   3. stats_lib     (static lib — defined INLINE in src/ray/meson.build
#                     to break circular dep: stats_lib → rpc:metrics_agent_client
#                     → ... → stats_metric → stats_lib)
#
# Note on metric_interface_cpp_lib_dep: This target is defined EARLY in
# src/ray/meson.build (before subdir('stats/')) because tag_defs and
# stats_metric depend on it, but it canonically belongs to observability/.
# =============================================================================


# === @begin: tag_defs_cpp_lib (@original: tag_defs) ===
tag_defs_cpp_lib_dependencies = [
    metric_interface_cpp_lib_dep,
]

tag_defs_cpp_lib = static_library(
    'tag_defs_cpp_lib',
    ['tag_defs.cc'],
    install: false,
    dependencies: tag_defs_cpp_lib_dependencies
)

tag_defs_cpp_lib_dep = declare_dependency(
    link_with: [tag_defs_cpp_lib],
    dependencies: tag_defs_cpp_lib_dependencies
)
# === @end: tag_defs_cpp_lib ===


# === @begin: stats_metric_cpp_lib (@original: stats_metric) ===
# Note: open_telemetry_metric_recorder_cpp_lib_dep is a real Bazel dep of
# stats_metric (metric.h includes open_telemetry_metric_recorder.h, and
# metric.cc calls OpenTelemetryMetricRecorder::GetInstance()). However, it
# cannot be listed here because it's defined in observability/ which is
# processed AFTER stats/. This works because:
#   1. Headers are found via project-wide -I include paths
#   2. Symbol resolution happens at final _raylet link time, not static_library time
# If this ever causes issues, open_telemetry_metric_recorder would need to be
# moved inline into src/ray/meson.build before the stats subdir call.
stats_metric_cpp_lib_dependencies = [
    # Package-managed
    tag_defs_cpp_lib_dep,
    metric_interface_cpp_lib_dep,
    # open_telemetry_metric_recorder_cpp_lib_dep,  # circular: defined in observability/ after stats/
    ray_config_cpp_lib_dep,
    logging_cpp_lib_dep,
    size_literals_cpp_lib_dep,

    # Eugo-managed
    prometheus_cpp_pull,

    absl_base_core_headers,
    absl_container_flat_hash_map,
    absl_memory,
    absl_strings,

    opencensus_cpp_stats,
    opencensus_cpp_tags,
]

stats_metric_cpp_lib = static_library(
    'stats_metric_cpp_lib',
    ['metric.cc'],
    install: false,
    dependencies: stats_metric_cpp_lib_dependencies
)

stats_metric_cpp_lib_dep = declare_dependency(
    link_with: [stats_metric_cpp_lib],
    dependencies: stats_metric_cpp_lib_dependencies
)
# === @end: stats_metric_cpp_lib ===


