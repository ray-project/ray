# IMPORTANT: Ordering within this file respects the internal dependency graph.
# You will see things "out of order" from a BUILD.bazel perspective because
# Meson requires define-before-use: if target A depends on target B and both
# are in this file, B must appear first regardless of Bazel ordering.
# Specifically: tag_defs must appear before stats_metric because stats_metric
# depends on tag_defs_cpp_lib_dep.


# === @begin: tag_defs_cpp_lib (@original: tag_defs) ===
tag_defs_cpp_lib_dependencies = [
    # Package-managed
    metric_interface_cpp_lib_dep,   # defined in `src/ray/meson.build` before `subdir('stats/')`
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
# === @end: tag_defs_cpp_lib (@original: tag_defs) ===


# === @begin: stats_metric_cpp_lib (@original: stats_metric) ===
# @IMPORTANT: open_telemetry_metric_recorder_cpp_lib_dep: circular — defined in src/ray/meson.build
# after subdir('rpc/'), which is after subdir('stats/'). It is resolved at final link time.
stats_metric_cpp_lib_dependencies = [
    # Package-managed
    tag_defs_cpp_lib_dep,
    ray_config_cpp_lib_dep,
    metric_interface_cpp_lib_dep,                   # defined in `src/ray/meson.build` before `subdir('stats/')`
    # open_telemetry_metric_recorder_cpp_lib_dep,   # `open_telemetry_metric_recorder_cpp_lib_dep` is NOT AVAILABLE (processed after): defined in `src/ray/meson.build` after `subdir('stats/')`
    logging_cpp_lib_dep,
    size_literals_cpp_lib_dep,

    # Eugo-managed
    prometheus_cpp_pull,
    absl_base_core_headers,
    absl_container_flat_hash_map,
    absl_memory,
    absl_strings,
    # "@com_google_googletest//:gtest_prod",
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
# === @end: stats_metric_cpp_lib (@original: stats_metric) ===


# === @begin: stats_cpp_lib (@original: stats_lib) ===
# @IMPORTANT: MOVED INLINE to src/ray/meson.build (after subdir('rpc/')) to break circular dep:
# `stats_lib` depends on `rpc:metrics_agent_client`, which is defined in rpc/ (after stats/).
# === @end: stats_cpp_lib (@original: stats_lib) ===