opencensus_proto_agent_common_v1_common_proto = custom_target(
    'opencensus_agent_proto_common_v1_common_proto',
    output: ['common.pb.cc', 'common.pb.h'],
    input: 'common.proto',
    command: [protoc_cpp, '--proto_path=../__eugo/include', '--cpp_out=' + meson.project_build_root() / '__eugo' / 'include', '@INPUT@']
)

opencensus_proto_agent_common_v1_common_proto_dep = declare_dependency(
    sources: opencensus_proto_agent_common_v1_common_proto,
    dependencies: [protobuf]
)