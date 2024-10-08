#run_command(['mkdir', '-p', '@BUILD_ROOT@/src/proto/grpc/health/v1/'], check: true)


# === @begin: health.proto ===
grpc_health_proto = ['health.proto']

# @begin: proto_cpp
grpc_health_proto_cpp = custom_target(
    input: grpc_health_proto,
    kwargs: proto_cpp_default_kwargs,
    build_by_default: true
)

grpc_health_proto_cpp_dep = declare_dependency(
    sources: grpc_health_proto_cpp
)
# @end: proto_cpp

# @begin: proto_cpp_grpc
grpc_health_proto_cpp_grpc = custom_target(
    input: grpc_health_proto,
    kwargs: proto_cpp_grpc_default_kwargs,
    build_by_default: true
)

grpc_health_proto_cpp_grpc_dep = declare_dependency(
    sources: grpc_health_proto_cpp_grpc
)
# @end: proto_cpp_grpc

# === @end: health.proto ===