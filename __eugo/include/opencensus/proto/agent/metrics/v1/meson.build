# === @begin: metrics_service.proto ===
opencensus_metrics_service_proto = 'metrics_service.proto'

opencensus_metrics_service_proto_cpp = custom_target(
    input: opencensus_metrics_service_proto,
    kwargs: opencensus_proto_cpp_default_kwargs
)

opencensus_metrics_service_proto_cpp_dep = declare_dependency(
    sources: opencensus_metrics_service_proto_cpp,
    dependencies: [opencensus_resource_proto_cpp_dep, opencensus_metrics_proto_cpp_dep]
)
# === @end: metrics_service.proto ===
