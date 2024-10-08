# === @begin: node_manager_fbs_cpp_lib (@original:  node_manager_fbs) ===
node_manager_cpp_fbs = custom_target(
    input: ['node_manager.fbs'],
    kwargs: cpp_fbs_default_kwargs
)

# header-only
node_manager_cpp_fbs_lib_dep = declare_dependency(
    sources: [node_manager_cpp_fbs],
    dependencies: [flatbuffers],
    include_directories: ['.']
)
# === @end: node_manager_fbs_cpp_lib (@original:  node_manager_fbs) ===