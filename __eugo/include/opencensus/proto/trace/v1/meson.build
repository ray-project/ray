# === @begin: trace.proto ===
proto_basename = 'trace'

opencensus_trace_proto = f'@proto_basename@.proto'

opencensus_trace_proto_cpp = custom_target(
    f'opencensus_@proto_basename@_proto_cpp',
    input: opencensus_trace_proto,
    command: opencensus_protoc_cpp_with_default_arguments,
    output: [f'@proto_basename@.pb.cc', f'@proto_basename@.pb.h']
)

opencensus_trace_proto_cpp_dep = declare_dependency(
    sources: opencensus_trace_proto_cpp,
    dependencies: [opencensus_resource_proto_cpp_dep]
)
# === @end: trace.proto ===


# === @begin: trace_config.proto ===
proto_basename = 'trace_config'

opencensus_trace_config_proto = f'@proto_basename@.proto'

opencensus_trace_config_proto_cpp = custom_target(
    f'opencensus_@proto_basename@_proto_cpp',
    input: opencensus_trace_config_proto,
    command: opencensus_protoc_cpp_with_default_arguments,
    output: [f'@proto_basename@.pb.cc', f'@proto_basename@.pb.h']
)

opencensus_trace_config_proto_cpp_dep = declare_dependency(
    sources: opencensus_trace_config_proto_cpp
)
# === @end: trace_config.proto ===
