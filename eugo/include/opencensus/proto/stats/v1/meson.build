# === @begin: stats.proto ===
opencensus_stats_proto = 'stats.proto'

opencensus_stats_proto_cpp = custom_target(
    input: opencensus_stats_proto,
    kwargs: opencensus_proto_cpp_default_kwargs
)

opencensus_stats_proto_cpp_dep = declare_dependency(
    sources: opencensus_stats_proto_cpp
)
# === @end: stats.proto ===