load("@rules_foreign_cc//foreign_cc:defs.bzl", "cmake")

filegroup(
    name = "all",
    srcs = glob(["**"]),
)

cmake(
    name = "arrow",
    lib_source = ":all",
    working_directory = "cpp",
    # Disable sandboxing — Arrow's BUNDLED deps download sources during cmake.
    tags = ["no-sandbox"],
    env = {
        "HOME": "/tmp",
    },
    # Override Bazel's ar/ranlib with system tools to fix Arrow's bundled
    # ExternalProject builds (Bazel's ar wrapper has incompatible flags).
    tool_prefix = "",
    generate_args = [
        "-DCMAKE_RANLIB=/usr/bin/ranlib",
        "-DCMAKE_AR=/usr/bin/ar",
    ],
    cache_entries = {
        "CMAKE_BUILD_TYPE": "Release",
        # Core components.
        "ARROW_IPC": "ON",
        "ARROW_FLIGHT": "ON",
        "ARROW_COMPUTE": "OFF",
        "ARROW_CSV": "OFF",
        "ARROW_DATASET": "OFF",
        "ARROW_FILESYSTEM": "OFF",
        "ARROW_JSON": "OFF",
        "ARROW_PARQUET": "OFF",
        "ARROW_PYTHON": "OFF",
        # Build options.
        "ARROW_BUILD_SHARED": "ON",
        "ARROW_BUILD_STATIC": "OFF",
        "ARROW_BUILD_TESTS": "OFF",
        "ARROW_BUILD_BENCHMARKS": "OFF",
        "ARROW_BUILD_UTILITIES": "OFF",
        "ARROW_BUILD_EXAMPLES": "OFF",
        "ARROW_BUILD_INTEGRATION": "OFF",
        # Let Arrow download and build its own deps.
        "ARROW_DEPENDENCY_SOURCE": "BUNDLED",
        # Disable optional features.
        "ARROW_WITH_UTF8PROC": "OFF",
        "ARROW_WITH_RE2": "OFF",
        "ARROW_WITH_ZLIB": "OFF",
        "ARROW_WITH_ZSTD": "OFF",
        "ARROW_WITH_LZ4": "OFF",
        "ARROW_WITH_SNAPPY": "OFF",
        "ARROW_WITH_BROTLI": "OFF",
        "ARROW_WITH_BZ2": "OFF",
        "ARROW_SIMD_LEVEL": "NONE",
        "ARROW_RUNTIME_SIMD_LEVEL": "NONE",
        "ARROW_DEPENDENCY_USE_SHARED": "OFF",
        # Disable gflags/glog — they fail with Bazel's toolchain ar/ranlib
        # wrappers, and are only needed for benchmarks/logging which we don't need.
        "ARROW_USE_GLOG": "OFF",
    },
    out_shared_libs = select({
        "@platforms//os:osx": [
            "libarrow.dylib",
            "libarrow_flight.dylib",
        ],
        "//conditions:default": [
            "libarrow.so",
            "libarrow_flight.so",
        ],
    }),
    out_include_dir = "include",
    visibility = ["//visibility:public"],
)
