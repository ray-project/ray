# === @begin: proto_cpp (original: cc_proto_library w/o final static library linking) ===
protoc_cpp_with_default_args = [
    protoc_cpp,
    '--proto_path=@SOURCE_ROOT@',
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
    protoc_cpp,
    '--plugin=protoc-gen-grpc=' + protoc_grpc_cpp_plugin.full_path(),
    '--proto_path=@SOURCE_ROOT@',
    '--grpc_out=@BUILD_ROOT@',
    '@INPUT@'
]
proto_cpp_grpc_default_kwargs = {
    'command': protoc_cpp_grpc_with_default_args,
    'output':  ['@BASENAME@.grpc.pb.cc', '@BASENAME@.gprc.pb.h']
}
# === @end: proto_cpp_grpc (original: cc_grpc_library w/o final static library linking) ===


# === @begin: proto_py (original: python_grpc_compile) ===
protoc_py = files('patching_protoc_py.sh')
protoc_py_with_default_args = [protoc_py, '@INPUT@', '@SOURCE_ROOT@', '@BUILD_ROOT@', '@OUTPUT0@', '@OUTPUT1@']
proto_py_default_kwargs = {
    'command': protoc_py_with_default_args,
    'output': ['@BASENAME@_pb2.py', '@BASENAME@_pb2_grpc.py']
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
