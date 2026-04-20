"""Repository rule that discovers PyArrow's bundled Arrow C++ headers and libraries."""

def _pyarrow_repo_impl(repository_ctx):
    python = repository_ctx.which("python3") or repository_ctx.which("python")
    if not python:
        fail("Could not find python3 or python on PATH")

    # Discover PyArrow paths.
    result = repository_ctx.execute([
        python,
        "-c",
        "import pyarrow; print(pyarrow.get_include()); print(pyarrow.get_library_dirs()[0])",
    ])
    if result.return_code != 0:
        fail("Failed to discover PyArrow paths: " + result.stderr)

    lines = result.stdout.strip().split("\n")
    include_dir = lines[0]
    lib_dir = lines[1]

    # Symlink include and lib directories into the repository.
    repository_ctx.symlink(include_dir, "include")
    repository_ctx.symlink(lib_dir, "lib")

    # Write BUILD file exposing Arrow C++ as cc_library targets.
    repository_ctx.file("BUILD.bazel", """
load("@rules_cc//cc:defs.bzl", "cc_library", "cc_import")

cc_library(
    name = "arrow",
    hdrs = glob(["include/arrow/**/*.h"]),
    includes = ["include"],
    linkopts = select({
        "@platforms//os:osx": ["-undefined", "dynamic_lookup"],
        "//conditions:default": [],
    }),
    srcs = select({
        "@platforms//os:osx": glob(["lib/libarrow.dylib", "lib/libarrow_flight.dylib", "lib/libarrow_python.dylib"]),
        "@platforms//os:linux": glob(["lib/libarrow.so*", "lib/libarrow_flight.so*", "lib/libarrow_python.so*"]),
    }),
    visibility = ["//visibility:public"],
)
""")

pyarrow_repo = repository_rule(
    implementation = _pyarrow_repo_impl,
    environ = ["PATH", "PYTHONPATH"],
)
