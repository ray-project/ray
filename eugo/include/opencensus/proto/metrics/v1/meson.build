# === @begin: metrics.proto ===
opencensus_metrics_proto = ['metrics.proto']

opencensus_metrics_proto_cpp = custom_target(
    input: opencensus_metrics_proto,
    kwargs: opencensus_proto_cpp_default_kwargs
)

opencensus_metrics_proto_cpp_dep = declare_dependency(
    sources: opencensus_metrics_proto_cpp,
    dependencies: [opencensus_resource_proto_cpp_dep]
)

# @begin: proto_py
opencensus_metrics_proto_py = custom_target(
    input: opencensus_metrics_proto,
    kwargs: opencensus_proto_py_default_kwargs,
    install: true,
    install_dir: py.get_install_dir() / 'ray/core/generated/'
)
# @end: proto_py
# === @end: metrics.proto ===