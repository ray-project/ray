# === @begin: proto_cpp (original: cc_proto_library w/o final static library linking) ===
protoc_cpp_with_default_args = [
    protoc_cpp,
    '--proto_path=@SOURCE_ROOT@',
    '-I@SOURCE_ROOT@/eugo/include', # Makes vendored OpenCensus `.proto` files visible to Ray's `.proto` files
    '--cpp_out=@BUILD_ROOT@',
    '@INPUT@'
]
proto_cpp_default_kwargs = {
    'command': protoc_cpp_with_default_args,
    'output': ['@BASENAME@.pb.h', '@BASENAME@.pb.cc']
}

all_proto_cpp_libs_dependencies = []
# === @end: proto_cpp (original: cc_proto_library w/o final static library linking) ==

# === @begin: proto_cpp_grpc (original: cc_grpc_library w/o final static library linking) ===
protoc_grpc_cpp_plugin = find_program('grpc_cpp_plugin')
protoc_cpp_grpc_with_default_args = [
    protoc_cpp,
    '--plugin=protoc-gen-grpc=' + protoc_grpc_cpp_plugin.full_path(),
    '--proto_path=@SOURCE_ROOT@',
    '-I@SOURCE_ROOT@/eugo/include', # Makes vendored OpenCensus `.proto` files visible to Ray's `.proto` files
    '--grpc_out=@BUILD_ROOT@',
    '@INPUT@'
]
proto_cpp_grpc_default_kwargs = {
    'command': protoc_cpp_grpc_with_default_args,
    'output':  ['@BASENAME@.grpc.pb.cc', '@BASENAME@.grpc.pb.h']
}

all_proto_cpp_grpc_libs_dependencies = []

# @Example:
# How to add extra arguments w/o rewriting the entire command:
# ```meson
# reporter_proto_cpp_grpc = custom_target(
#     input: reporter_proto,
#     kwargs: proto_cpp_grpc_default_kwargs + {
#         'command': [
#             protoc_cpp_grpc_with_default_args, '-I@SOURCE_ROOT@/eugo/include'
#         ]
#     }
# )
# ```
# === @end: proto_cpp_grpc (original: cc_grpc_library w/o final static library linking) ===


# === @begin: proto_py (original: python_grpc_compile) ===
# @begin: (# @IMPORTANT Can't do this as of this writing as `protoc` uses buildtime paths in its runtime imports so we need  to rely on sed patches:( See https://github.com/protocolbuffers/protobuf/issues/1491#issuecomment-1648982084)
# protoc_py = protoc_cpp
# protoc_grpc_python_plugin = find_program('grpc_python_plugin')
# protoc_py_grpc_with_default_args = [
#     protoc_py,
#     '--plugin=protoc-gen-grpc_python=' + protoc_grpc_python_plugin.full_path(),
#     '--proto_path=@CURRENT_SOURCE_DIR@',

#     # Do not replace SOURCE_ROOT here!
#     '-I@SOURCE_ROOT@/eugo/include',

#     '--python_out=@OUTDIR@',
#     '--grpc_python_out=@OUTDIR@',
#     '--grpc_python_opt=grpc_2_0',
#     '@INPUT@'
# ]
# @end

protoc_py = files('patching_protoc_py.sh')
protoc_py_with_default_args = [
    protoc_py,
    '@INPUT@',
    '@SOURCE_ROOT@',
    '@BUILD_ROOT@',
    '@OUTPUT0@',
    '@OUTPUT1@'
]
proto_py_default_install_dir = py.get_install_dir() / 'ray/core/generated/'
proto_py_default_kwargs = {
    'command': protoc_py_with_default_args,
    'output': ['@BASENAME@_pb2.py', '@BASENAME@_pb2_grpc.py'],
    'install': true,
    'install_dir': proto_py_default_install_dir
}
# === @end: proto_py (original: python_grpc_compile) ===


# === @begin: cpp_fbs (original: `flatbuffer_cc_library` w/o final static library linking) ===
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

all_cpp_fbs_libs_dependencies = []
# === @end: cpp_fbs (original: `flatbuffer_cc_library` w/o final static library linking) ===
