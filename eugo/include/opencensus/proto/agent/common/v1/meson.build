# === @begin: common.proto ===
opencensus_common_proto = ['common.proto']

opencensus_common_proto_cpp = custom_target(
    input: opencensus_common_proto,
    kwargs: opencensus_proto_cpp_default_kwargs
)

opencensus_common_proto_cpp_dep = declare_dependency(
    sources: opencensus_common_proto_cpp
)
# === @end: common.proto ===