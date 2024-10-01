opencensus_proto_resource_v1_resource_proto = custom_target(
    'opencensus_proto_resource_v1_resource_proto',
    input: 'resource.proto',
    command: [protoc, '--proto_path=../__eugo/include', '--cpp_out=' + meson.project_build_root() / '__eugo' / 'include', '@INPUT@'],
    output: ['resource.pb.cc', 'resource.pb.h']
)

opencensus_proto_resource_v1_resource_proto_dep = declare_dependency(
    sources: opencensus_proto_resource_v1_resource_proto,
    dependencies: [protobuf]
)