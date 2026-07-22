load("@rules_foreign_cc//foreign_cc:defs.bzl", "cmake")

filegroup(
    name = "all_srcs",
    srcs = glob(
        include = ["**"],
        exclude = ["*.bazel"],
    ),
)

# Set by `--config=tsan` (see .bazelrc). RocksDB is compiled by an
# out-of-band CMake action, so Bazel's `--copt -fsanitize=thread` never
# reaches it. Under this config we instead flip RocksDB's own `WITH_TSAN`
# CMake option, which adds `-fsanitize=thread` to its compile/link flags.
# Without this, a TSAN-instrumented GCS test links an *un*instrumented
# librocksdb.a, and ThreadSanitizer reports false data races inside
# RocksDB's lock-free memtable skiplist (it cannot see the acquire/release
# atomics that synchronize concurrent memtable writers and readers).
config_setting(
    name = "tsan_build",
    values = {"define": "ray_rocksdb_tsan=true"},
)

# Static-only build. No shared lib, no shell tools, no benchmarks,
# no tests — librocksdb.a is the only artifact GCS links against.
_CACHE_ENTRIES = {
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
}

# Same as _CACHE_ENTRIES, but with RocksDB's native TSAN instrumentation
# turned on. Used under `--config=tsan` so ThreadSanitizer understands
# RocksDB's lock-free internals. `PORTABLE=ON` above keeps
# `-fsanitize=thread` compatible with the pie/relocation flags RocksDB
# adds when WITH_TSAN is set.
_TSAN_CACHE_ENTRIES = dict(
    _CACHE_ENTRIES,
    WITH_TSAN = "ON",
)

cmake(
    name = "rocksdb",
    cache_entries = select({
        ":tsan_build": _TSAN_CACHE_ENTRIES,
        "//conditions:default": _CACHE_ENTRIES,
    }),
    # Use CMake's Make generator, not Ninja. rules_foreign_cc has no system
    # ninja, so "-G Ninja" makes it bootstrap ninja from source, which invokes
    # `c++` under a scrubbed PATH (/bin:/usr/bin:/usr/local/bin). The aarch64
    # manylinux image has no `c++` there, so the ninja bootstrap fails with
    # "c++: command not found" whenever the tool isn't served from the Bazel
    # remote cache -- breaking the aarch64 core wheel build. Make uses the
    # preinstalled toolchain (like jemalloc/openssl) and needs no bootstrap.
    generate_args = ["-G", "Unix Makefiles"],
    lib_source = ":all_srcs",
    out_static_libs = ["librocksdb.a"],
    visibility = ["//visibility:public"],
)
