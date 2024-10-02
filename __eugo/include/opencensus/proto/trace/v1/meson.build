# === @begin: trace.proto ===
opencensus_proto_trace_v1_trace_proto = custom_target(
    'opencensus_proto_trace_v1_trace_proto',
    input: 'trace.proto',
    command: [protoc_cpp, '--proto_path=../__eugo/include', '--cpp_out=' + meson.project_build_root() / '__eugo' / 'include', '@INPUT@'],
    output: ['trace.pb.cc', 'trace.pb.h']
)

opencensus_proto_trace_v1_trace_proto_dep = declare_dependency(
    sources: opencensus_proto_trace_v1_trace_proto,
    dependencies: [protobuf, opencensus_proto_resource_v1_resource_proto_dep]
)
# === @end: trace.proto ===


# === @begin: trace_config.proto ===
opencensus_proto_trace_v1_trace_config_proto = custom_target(
    'opencensus_proto_trace_v1_trace_config_proto',
    input: 'trace_config.proto',
    command: [protoc_cpp, '--proto_path=../__eugo/include', '--cpp_out=' + meson.project_build_root() / '__eugo' / 'include', '@INPUT@'],
    output: ['trace_config.pb.cc', 'trace_config.pb.h']
)

opencensus_proto_trace_v1_trace_config_proto_dep = declare_dependency(
    sources: opencensus_proto_trace_v1_trace_config_proto,
    dependencies: [protobuf]
)
# === @end: trace_config.proto ===
