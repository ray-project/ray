# === @begin: instance_manager.proto ===
instance_manager_proto = ['instance_manager.proto']

instance_manager_proto_cpp = custom_target(
    input: instance_manager_proto,
    kwargs: proto_cpp_default_kwargs
)

instance_manager_proto_cpp_dep = declare_dependency(
    sources: instance_manager_proto_cpp
)

instance_manager_proto_cpp_lib = static_library(
    'instance_manager_proto_cpp_lib',
    dependencies: [protobuf, instance_manager_proto_cpp_dep],
    install: false
)

instance_manager_proto_cpp_lib_dep = declare_dependency(
    link_with: [instance_manager_proto_cpp_lib]
)

instance_manager_proto_py = custom_target(
    input: instance_manager_proto,
    kwargs: proto_py_default_kwargs
)
# === @end: instance_manager.proto ===