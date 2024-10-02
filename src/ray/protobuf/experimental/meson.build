# === @begin: instance_manager.proto ===
instance_manager_proto = custom_target(
    'instance_manager_proto',
    output: ['instance_manager.pb.cc', 'instance_manager.pb.h'],
    input: 'instance_manager.proto',
    command: [protoc, '--proto_path=..', '--cpp_out=' + meson.project_build_root(), '@INPUT@']
)

instance_manager_proto_dep = declare_dependency(
    sources: instance_manager_proto
)

instance_manager_cc_proto = static_library(
    'instance_manager_cc_proto',
    dependencies: [instance_manager_proto_dep]
)

instance_manager_py_proto = custom_target(
    'instance_manager_py_proto',
    output: ['instance_manager_pb2.py', 'instance_manager_pb2_grpc.py'],
    input: 'instance_manager.proto',
    command: [
        'python3', '-m', 'grpc_tools.protoc',
        '--proto_path=..',
        '--python_out=' + meson.project_build_root(),
        '--grpc_python_out=' + meson.project_build_root(),
        '@INPUT@'
    ]
)
# === @end: instance_manager.proto ===