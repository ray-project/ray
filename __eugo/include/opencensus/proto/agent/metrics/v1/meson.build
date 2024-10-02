opencensus_proto_agent_metrics_v1_metrics_service_proto = custom_target(
    'opencensus_proto_agent_metrics_v1_metrics_service_proto',
    input: 'metrics_service.proto',
    command: [protoc_cpp, '--proto_path=../__eugo/include', '--cpp_out=' + meson.project_build_root() / '__eugo' / 'include', '@INPUT@'],
    output: ['metrics_service.pb.cc', 'metrics_service.pb.h']
)

opencensus_proto_agent_metrics_v1_metrics_service_proto_dep = declare_dependency(
    sources: opencensus_proto_agent_metrics_v1_metrics_service_proto,
    dependencies: [protobuf, opencensus_proto_resource_v1_resource_proto_dep, opencensus_proto_metrics_v1_metrics_proto_dep]
)