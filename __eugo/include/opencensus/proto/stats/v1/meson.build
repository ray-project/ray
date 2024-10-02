opencensus_proto_stats_v1_stats_proto = custom_target(
    'opencensus_proto_stats_v1_stats_proto',
    input: 'stats.proto',
    command: [protoc_cpp, '--proto_path=../__eugo/include', '--cpp_out=' + meson.project_build_root() / '__eugo' / 'include', '@INPUT@'],
    output: ['stats.pb.cc', 'stats.pb.h']
)

opencensus_proto_stats_v1_stats_proto_dep = declare_dependency(
    sources: opencensus_proto_stats_v1_stats_proto,
    dependencies: [protobuf]
)