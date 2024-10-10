# === @begin: proto_cpp (original: cc_proto_library w/o final static library linking) ===
protoc_cpp_with_default_args = [
    protoc,
    '--proto_path=..',
    '--cpp_out=@BUILD_ROOT@',
    '@INPUT@'
]
proto_cpp_default_kwargs = {
    'command': protoc_cpp_with_default_args,
    'output': ['@BASENAME@.pb.h', '@BASENAME@.pb.cc']
}
# === @end: proto_cpp (original: cc_proto_library w/o final static library linking) ==


# === @begin: proto_cpp_grpc (original: cc_grpc_library w/o final static library linking) ===
protoc_grpc_cpp_plugin = find_program('grpc_cpp_plugin')
protoc_cpp_grpc_with_default_args = [
    protoc,
    '--plugin=protoc-gen-grpc=' + protoc_grpc_cpp_plugin.full_path(),
    '--proto_path=..',
    '--grpc_out=@BUILD_ROOT@',
    '@INPUT@'
]
proto_cpp_grpc_default_kwargs = {
    'command': protoc_cpp_grpc_with_default_args,
    'output':  ['@BASENAME@.grpc.pb.cc', '@BASENAME@.gprc.pb.h']
}
# === @end: proto_cpp_grpc (original: cc_grpc_library w/o final static library linking) ===


# === @begin: proto_py (original: python_grpc_compile) ===
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
    #'build_by_default': true # Required until something would explicitely depend on this
}
# === @end: proto_py (original: python_grpc_compile) ===


# === @begin: cpp_fbs (original: flatbuffer_cc_library w/o final static library linking) ===
flatc_cpp_fbs_with_default_args = [
    flatc,
    '--scoped-enums',
    '--gen-mutable',
    '--gen-object-api',
    '-o', '@OUTDIR@',
    '--cpp', '@INPUT@'
]
cpp_fbs_default_kwargs = {
    'command': flatc_cpp_fbs_with_default_args,
    'output': ['@BASENAME@_generated.h']
}
# === @end: cpp_fbs (original: flatbuffer_cc_library w/o final static library linking) ===
