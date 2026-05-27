load("@rules_foreign_cc//foreign_cc:defs.bzl", "cmake")

filegroup(
    name = "all_srcs",
    srcs = glob(
        include = ["**"],
        exclude = ["*.bazel"],
    ),
)

cmake(
    name = "rocksdb",
    cache_entries = {
        # Static-only build. No shared lib, no shell tools, no benchmarks,
        # no tests — librocksdb.a is the only artifact GCS links against.
        "ROCKSDB_BUILD_SHARED": "OFF",
        "WITH_TESTS": "OFF",
        "WITH_BENCHMARK_TOOLS": "OFF",
        "WITH_TOOLS": "OFF",
        "WITH_CORE_TOOLS": "OFF",
        "WITH_TRACE_TOOLS": "OFF",
        "WITH_EXAMPLES": "OFF",
        "WITH_GFLAGS": "OFF",

        # No compression deps for the initial cut — the REP's tuning
        # table calls for LZ4 on cold levels, which can be enabled in a
        # follow-up once the link footprint is measured against the
        # tradeoff.
        "WITH_SNAPPY": "OFF",
        "WITH_LZ4": "OFF",
        "WITH_ZSTD": "OFF",
        "WITH_ZLIB": "OFF",
        "WITH_BZ2": "OFF",

        # Ray vendors its own jemalloc; mixing them is out of scope here.
        "WITH_JEMALLOC": "OFF",
        "WITH_LIBURING": "OFF",
        "WITH_NUMA": "OFF",
        "WITH_TBB": "OFF",

        # CMake's GNUInstallDirs picks lib64/ on 64-bit Linux but lib/ on
        # macOS/BSD. Pinning to lib/ keeps the out_static_libs path
        # portable across platforms.
        "CMAKE_INSTALL_LIBDIR": "lib",

        "PORTABLE": "ON",
        "FAIL_ON_WARNINGS": "OFF",
        "USE_RTTI": "1",
        "ROCKSDB_INSTALL_ON_WINDOWS": "OFF",
        "WITH_RUNTIME_DEBUG": "OFF",
    },
    generate_args = ["-G Ninja"],
    lib_source = ":all_srcs",
    out_static_libs = ["librocksdb.a"],
    visibility = ["//visibility:public"],
)
