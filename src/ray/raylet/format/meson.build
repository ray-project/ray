# === @begin: node_manager_fbs (original: node_manager_fbs) ===
# build_by_default add to the wrapper!
node_manager_cpp_fbs = custom_target(
    input: ['node_manager.fbs'],
    output: ['@BASENAME@_generated.h'],
    command: ['flatc', '--scoped-enums', '--gen-mutable', '--gen-object-api', '-o', '@OUTDIR@', '--cpp', '@INPUT@'],
    build_by_default: true
)

node_manager_cpp_fbs_dep = declare_dependency(
    sources: [node_manager_cpp_fbs] # + inc dir with this file instead
    # and dependency on flatbuffers package
)

node_manager_cpp_fbs_lib = static_library(
    'node_manager_cpp_fbs_lib',
    dependencies: [node_manager_cpp_fbs_dep],
    install: false
)

node_manager_cpp_fbs_lib_dep = declare_dependency(
    link_with: [node_manager_cpp_fbs_lib]
)
# Depends on flatbuffers!
# === @end: node_manager_fbs (original: node_manager_fbs) ===


#flatbuffer_cc_library(
#    name = "node_manager_fbs",
#    srcs = ["src/ray/raylet/format/node_manager.fbs"],
#    flatc_args = FLATC_ARGS,
#    out_prefix = "ray/raylet/format/",
#)