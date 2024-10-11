# === @begin: resource.proto ===
opencensus_resource_proto = ['resource.proto']

# @begin: proto_cpp
opencensus_resource_proto_cpp = custom_target(
    input: opencensus_resource_proto,
    kwargs: opencensus_proto_cpp_default_kwargs
)

opencensus_resource_proto_cpp_dep = declare_dependency(
    sources: opencensus_resource_proto_cpp
)
# @end: proto_cpp

# @begin: proto_py
opencensus_resource_proto_py = custom_target(
    input: opencensus_resource_proto,
    kwargs: opencensus_proto_py_default_kwargs,
    install: true,
    install_dir: py.get_install_dir() / 'ray/core/generated/'
)
# @end: proto_py
# === @end: resource.proto ===