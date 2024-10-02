# === @begin: trace_service.proto ===
proto_basename = 'trace_service'

opencensus_trace_service_proto = f'@proto_basename@.proto'

opencensus_trace_service_proto_cpp = custom_target(
    f'opencensus_@proto_basename@_proto_cpp',
    input: opencensus_trace_service_proto,
    command: opencensus_protoc_cpp_with_default_arguments,
    output: [f'@proto_basename@.pb.cc', f'@proto_basename@.pb.h']
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
