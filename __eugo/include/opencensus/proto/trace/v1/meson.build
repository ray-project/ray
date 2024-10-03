# === @begin: trace.proto ===
opencensus_trace_proto = 'trace.proto'

opencensus_trace_proto_cpp = custom_target(
    input: opencensus_trace_proto,
    kwargs: opencensus_proto_cpp_default_kwargs
)

opencensus_trace_proto_cpp_dep = declare_dependency(
    sources: opencensus_trace_proto_cpp,
    dependencies: [opencensus_resource_proto_cpp_dep]
)
# === @end: trace.proto ===


# === @begin: trace_config.proto ===
opencensus_trace_config_proto = 'trace_config.proto'

opencensus_trace_config_proto_cpp = custom_target(
    input: opencensus_trace_config_proto,
    kwargs: opencensus_proto_cpp_default_kwargs
)

opencensus_trace_config_proto_cpp_dep = declare_dependency(
    sources: opencensus_trace_config_proto_cpp
)
# === @end: trace_config.proto ===
