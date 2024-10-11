# === @begin: instance_manager.proto ===
instance_manager_proto = ['instance_manager.proto']

# @begin: proto_cpp (original: instance_manager_cc_proto)
instance_manager_proto_cpp = custom_target(
    input: instance_manager_proto,
    kwargs: proto_cpp_default_kwargs
)

instance_manager_proto_cpp_dep = declare_dependency(
    sources: instance_manager_proto_cpp
)

instance_manager_proto_cpp_lib_dependencies = [
    # Package-managed
    instance_manager_proto_cpp_dep,

    # Eugo-managed
    protobuf
]

instance_manager_proto_cpp_lib = static_library(
    'instance_manager_proto_cpp_lib',
    dependencies: instance_manager_proto_cpp_lib_dependencies,
    install: false
)

instance_manager_proto_cpp_lib_dep = declare_dependency(
    link_with: [instance_manager_proto_cpp_lib],
    dependencies: instance_manager_proto_cpp_lib_dependencies
)
# @end: proto_cpp (original: instance_manager_cc_proto)

# @begin: proto_py (original: instance_manager_py_proto)
instance_manager_proto_py = custom_target(
    input: instance_manager_proto,
    kwargs: proto_py_default_kwargs,
    install: true,
    install_dir: py.get_install_dir() / 'ray/core/generated/'
)
# @end: proto_py (original: instance_manager_py_proto)
# === @end: instance_manager.proto ===
