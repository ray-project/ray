# === @begin: resource.proto ===
opencensus_resource_proto = ['resource.proto']

opencensus_resource_proto_cpp = custom_target(
    input: opencensus_resource_proto,
    kwargs: opencensus_proto_cpp_default_kwargs
)

opencensus_resource_proto_cpp_dep = declare_dependency(
    sources: opencensus_resource_proto_cpp
)
# === @end: resource.proto ===