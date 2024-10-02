# === @begin: resource.proto ===
proto_basename = 'resource'

opencensus_resource_proto = f'@proto_basename@.proto'

opencensus_resource_proto_cpp = custom_target(
    f'opencensus_@proto_basename@_proto_cpp',
    input: opencensus_resource_proto,
    command: opencensus_protoc_cpp_with_default_arguments,
    output: [f'@proto_basename@.pb.cc', f'@proto_basename@.pb.h']
)

opencensus_resource_proto_cpp_dep = declare_dependency(
    sources: opencensus_resource_proto_cpp
)
# === @end: resource.proto ===