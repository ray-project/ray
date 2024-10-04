# === @begin: proto_cpp ===
protoc_cpp = find_program('protoc')
protoc_cpp_with_default_args = [
    protoc_cpp,
    '--proto_path=..',
    '--cpp_out=@BUILD_ROOT@',
    '@INPUT@'
]
proto_cpp_default_kwargs = {
    'command': protoc_cpp_with_default_args,
    'output': ['@BASENAME@.pb.h', '@BASENAME@.pb.cc']
}
# === @end: proto_cpp ==


# === @begin: proto_grpc ===
protoc_grpc_cpp_plugin = find_program('grpc_cpp_plugin')
protoc_cpp_grpc_with_default_args = [
    protoc_cpp,
    '--plugin=protoc-gen-grpc=' + protoc_grpc_cpp_plugin.full_path(),
    '--proto_path=..',
    '--grpc_out=@BUILD_ROOT@',
    '@INPUT@'
]
proto_cpp_grpc_default_kwargs = {
    'command': protoc_cpp_grpc_with_default_args,
    'output':  ['@BASENAME@.grpc.pb.cc', '@BASENAME@.gprc.pb.h']
}
# === @end: proto_grpc ===


# === @begin: proto_py ===
protoc_py = ['python3', '-m', 'grpc_tools.protoc']
protoc_py_with_default_args = [
    protoc_py,
    '--proto_path=..',
    '--python_out=@BUILD_ROOT@',
    '--grpc_python_out=@BUILD_ROOT@',
    '@INPUT@'
]
proto_py_default_kwargs = {
    'command': protoc_py_with_default_args,
    'output': ['@BASENAME@_pb2.py', '@BASENAME@_pb2_grpc.py'],
    'build_by_default': true
}
# === @end: proto_py ===