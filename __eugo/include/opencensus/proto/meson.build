opencensus_protoc_cpp_with_default_arguments = [
    protoc_cpp,
    '--proto_path=../__eugo/include',
    '--cpp_out=@BUILD_ROOT@/__eugo/include',
    '@INPUT@'
]

opencensus_proto_cpp_default_kwargs = proto_cpp_default_kwargs + {'command': opencensus_protoc_cpp_with_default_arguments}


# @Important: Order matters!
subdir('resource/')
subdir('stats/')
subdir('metrics/')
subdir('trace/')
subdir('agent/')
