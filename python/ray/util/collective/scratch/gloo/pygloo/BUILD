load("@rules_foreign_cc//tools/build_defs:cmake.bzl", "cmake_external")
load("@pybind11_bazel//:build_defs.bzl", "pybind_library")
load("@pybind11_bazel//:build_defs.bzl", "pybind_extension")

# cmake_external(
#     name = "libuv",
#     # Values to be passed as -Dkey=value on the CMake command line;
#     # here are serving to provide some CMake script configuration options
#     cache_entries = {
#         "NOFORTRAN": "on",
#         "BUILD_WITHOUT_LAPACK": "no",
#     },

#     lib_source = "@libuv//:all",

#     # We are selecting the resulting static library to be passed in C/C++ provider
#     # as the result of the build;
#     # However, the cmake_external dependants could use other artefacts provided by the build,
#     # according to their CMake script
#     # static_libraries = ["libuv.a"],
# )

cmake_external(
    name = "gloo",
    # Values to be passed as -Dkey=value on the CMake command line;
    # here are serving to provide some CMake script configuration options
    cache_entries = {
        "NOFORTRAN": "on",
        "BUILD_WITHOUT_LAPACK": "no",
        "USE_REDIS": "off",
        "USE_IBVERBS": "on",
        "USE_NCCL": "on",
        "USE_RCCL": "on",
        "USE_LIBUV": "off",
        "USE_TCP_OPENSSL": "off",
    },
    cmake_options = [],
    # tools_deps = [
    #     ":libuv"
    # ],
    # env_vars = [
    #     "export PATH=/path/to/install/bin:$PATH",
    #     "export LD_LIBRARY_PATH=/path/to/install/lib:$LD_LIBRARY_PATH"
    # ],

    lib_source = "@gloo//:all",

    # We are selecting the resulting static library to be passed in C/C++ provider
    # as the result of the build;
    # However, the cmake_external dependants could use other artefacts provided by the build,
    # according to their CMake script
    static_libraries = ["libgloo.a"],
)

pybind_library(
    name = "pygloo-deps",
    srcs = glob(["src/*.cc"]),
    hdrs = glob(["include/*.h"]),
    strip_include_prefix = "include",
    # copts = ["-Iinclude"],
    visibility = ["//visibility:public"],
    deps = [":gloo"]
)

pybind_extension(
    name = "pygloo",
    srcs = ["main.cc"],
    deps = [":gloo", ":pygloo-deps"],
)