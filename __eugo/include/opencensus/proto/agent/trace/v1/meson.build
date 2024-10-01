opencensus_proto_agent_trace_v1_trace_service_proto = custom_target(
    'opencensus_proto_agent_trace_v1_trace_service_proto',
    input: 'trace_service.proto',
    command: [protoc, '--proto_path=../__eugo/include', '--cpp_out=' + meson.project_build_root() / '__eugo' / 'include', '@INPUT@'],
    output: ['trace_service.pb.cc', 'trace_service.pb.h']
)

opencensus_proto_agent_trace_v1_trace_service_proto_dep = declare_dependency(
    sources: opencensus_proto_agent_trace_v1_trace_service_proto,
    dependencies: [
        protobuf,
        opencensus_proto_resource_v1_resource_proto_dep,
        opencensus_proto_trace_v1_trace_proto_dep,
        opencensus_proto_trace_v1_trace_config_proto_dep,
        opencensus_proto_agent_common_v1_common_proto_dep
    ]
)
