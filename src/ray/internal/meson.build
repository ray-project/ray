# === @begin: exported_internal_cpp_lib (original: exported_internal) ===
exported_internal_cpp_lib_dependencies = [
    # Package-managed
    core_worker_cpp_lib_dep
]

exported_internal_cpp_lib = static_library(
    'exported_internal_cpp_lib',
    ['internal.cc'],
    install: false,
    dependencies: exported_internal_cpp_lib_dependencies
)

exported_internal_cpp_lib_dep = declare_dependency(
    link_with: [exported_internal_cpp_lib],
    dependencies: exported_internal_cpp_lib_dependencies
)
# === @end: exported_internal_cpp_lib (original: exported_internal) ===


#
## This header is used to wrap some internal codes so we can reduce suspicious
## symbols export.
#ray_cc_library(
#    name = "exported_internal",
#    srcs =
#    [
#    "src/ray/internal/internal.cc",
#    ],
#    hdrs =
#    [
#    "src/ray/internal/internal.h",
#    ],
#    copts = COPTS,
#    strip_include_prefix = "src",
#    deps = [
#    ":core_worker_lib",
#    ],
#    alwayslink = 1,
#)