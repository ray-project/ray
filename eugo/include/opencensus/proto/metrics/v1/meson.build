# === @begin: metrics.proto ===
opencensus_metrics_proto = ['metrics.proto']

opencensus_metrics_proto_cpp = custom_target(
    input: opencensus_metrics_proto,
    kwargs: opencensus_proto_cpp_default_kwargs
)

opencensus_metrics_proto_cpp_dep = declare_dependency(
    sources: opencensus_metrics_proto_cpp,
    dependencies: [opencensus_resource_proto_cpp_dep]
)
# === @end: metrics.proto ===