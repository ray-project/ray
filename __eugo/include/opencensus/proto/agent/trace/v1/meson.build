# === @begin: trace_service.proto ===
opencensus_trace_service_proto = ['trace_service.proto']

opencensus_trace_service_proto_cpp = custom_target(
    input: opencensus_trace_service_proto,
    kwargs: opencensus_proto_cpp_default_kwargs
)

opencensus_trace_service_proto_cpp_dep = declare_dependency(
    sources: opencensus_trace_service_proto_cpp,
    dependencies: [
        opencensus_common_proto_cpp_dep,
        opencensus_resource_proto_cpp_dep,
        opencensus_trace_proto_cpp_dep,
        opencensus_trace_config_proto_cpp_dep
    ]
)
# === @end: trace_service.proto ===
