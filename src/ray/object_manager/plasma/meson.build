# === @begin: plasma_cpp_fbs_lib (@original: plasma_fbs) ===
plasma_cpp_fbs = custom_target(
    input: ['plasma.fbs'],
    kwargs: cpp_fbs_default_kwargs
)

# header-only
plasma_cpp_fbs_lib_dep = declare_dependency(
    sources: [plasma_cpp_fbs],
    dependencies: [flatbuffers]
)
# === @end: plasma_cpp_fbs_lib (@original: plasma_fbs) ===