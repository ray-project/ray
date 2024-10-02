# === @begin: instance_manager.proto ===
proto_basename = 'instance_manager'

instance_manager_proto = f'@proto_basename@.proto'

instance_manager_proto_cpp = custom_target(
    f'@proto_basename@_proto_cpp',
    input: instance_manager_proto,
    command: protoc_cpp_with_default_args,
    output: [f'@proto_basename@.pb.cc', f'@proto_basename@.pb.h']
)

instance_manager_proto_cpp_dep = declare_dependency(
    sources: instance_manager_proto_cpp
)

instance_manager_proto_cpp_lib = static_library(
    f'@proto_basename@_proto_cpp_lib',
    dependencies: [protobuf, instance_manager_proto_cpp_dep]
)

instance_manager_proto_py = custom_target(
    f'@proto_basename@_proto_py',
    input: instance_manager_proto,
    command: protoc_py_with_default_args,
    output: [f'@proto_basename@_pb2.py', f'@proto_basename@_pb2_grpc.py']
)
# === @end: instance_manager.proto ===