# === @begin: metrics_service.proto ===
proto_basename = 'metrics_service'

opencensus_metrics_service_proto = f'@proto_basename@.proto'

opencensus_metrics_service_proto_cpp = custom_target(
    f'opencensus_@proto_basename@_proto_cpp',
    input: opencensus_metrics_service_proto,
    command: opencensus_protoc_cpp_with_default_arguments,
    output: [f'@proto_basename@.pb.cc', f'@proto_basename@.pb.h']
)

opencensus_metrics_service_proto_cpp_dep = declare_dependency(
    sources: opencensus_metrics_service_proto_cpp,
    dependencies: [opencensus_resource_proto_cpp_dep, opencensus_metrics_proto_cpp_dep]
)
# === @end: metrics_service.proto ===
