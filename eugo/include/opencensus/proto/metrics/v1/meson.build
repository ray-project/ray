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
# @IMPORTANT: Variable is unused — no meson.build references it. Kept because the custom_target
# generates and installs *_pb2.py into the wheel via opencensus_proto_py_default_kwargs (install: true).
opencensus_metrics_proto_py = custom_target(
    input: opencensus_metrics_proto,
    kwargs: opencensus_proto_py_default_kwargs
)
# @end: proto_py
# === @end: metrics.proto ===