# === @begin: instance_manager.proto ===
instance_manager_proto = ['instance_manager.proto']

# @begin: proto_cpp (original: instance_manager_cc_proto)
instance_manager_proto_cpp = custom_target(
    input: instance_manager_proto,
    kwargs: proto_cpp_default_kwargs
)

instance_manager_proto_cpp_lib = static_library(
    'instance_manager_proto_cpp_lib',
    dependencies: [protobuf, declare_dependency(sources: instance_manager_proto_cpp)],
    install: false
)

instance_manager_proto_cpp_lib_dep = declare_dependency(
    link_with: [instance_manager_proto_cpp_lib],
    dependencies: [protobuf]
)
# @end: proto_cpp (original: instance_manager_cc_proto)

# @begin: proto_py (original: instance_manager_py_proto)
instance_manager_proto_py = custom_target(
    input: instance_manager_proto,
    kwargs: proto_py_default_kwargs
)
# @end: proto_py (original: instance_manager_py_proto)
# === @end: instance_manager.proto ===