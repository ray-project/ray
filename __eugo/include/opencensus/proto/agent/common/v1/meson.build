# === @begin: common.proto ===
proto_basename = 'common'

opencensus_common_proto = f'@proto_basename@.proto'

opencensus_common_proto_cpp = custom_target(
    f'opencensus_@proto_basename@_proto_cpp',
    input: opencensus_common_proto,
    command: opencensus_protoc_cpp_with_default_arguments,
    output: [f'@proto_basename@.pb.cc', f'@proto_basename@.pb.h']
)

opencensus_common_proto_cpp_dep = declare_dependency(
    sources: opencensus_common_proto_cpp
)
# === @end: common.proto ===