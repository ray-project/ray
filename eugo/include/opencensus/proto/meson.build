# === @begin: proto_cpp ===
opencensus_protoc_cpp_with_default_arguments = [
    protoc_cpp,
    '--proto_path=@SOURCE_ROOT@/eugo/include',
    '--cpp_out=@BUILD_ROOT@/eugo/include',
    '@INPUT@'
]

opencensus_proto_cpp_default_kwargs = proto_cpp_default_kwargs + {'command': opencensus_protoc_cpp_with_default_arguments}
# === @end: proto_cpp ===


# === @begin: proto_py ===
opencensus_protoc_py_with_default_arguments = [protoc_py, '@INPUT@', '@SOURCE_ROOT@/eugo/include', '@BUILD_ROOT@/eugo/include', '@OUTPUT0@', '@OUTPUT1@']

opencensus_proto_py_default_kwargs = proto_py_default_kwargs + {'command': opencensus_protoc_py_with_default_arguments}
# === @end: proto_py ===


# @Important: Order matters!
subdir('resource/')
subdir('metrics/')
