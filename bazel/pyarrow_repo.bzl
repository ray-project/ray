"""Repository rule that discovers PyArrow's bundled Arrow C++ headers and libraries.

Uses PYTHON3_BIN_PATH (set by setup.py) to find the correct Python interpreter.
If pyarrow is not installed, creates a stub so the build doesn't fail unless
something actually depends on the Flight store.
"""

_STUB_BUILD = """\
cc_library(
    name = "arrow",
    visibility = ["//visibility:public"],
)
"""

def _pyarrow_repo_impl(repository_ctx):
    # setup.py sets PYTHON3_BIN_PATH to sys.executable before calling Bazel.
    python = repository_ctx.os.environ.get("PYTHON3_BIN_PATH")
    if not python:
        python = repository_ctx.which("python3") or repository_ctx.which("python")
    if not python:
        # buildifier: disable=print
        print("NOTE: No python found, Arrow Flight store will not be available.")
        repository_ctx.file("BUILD.bazel", _STUB_BUILD)
        return

    result = repository_ctx.execute([
        str(python),
        "-c",
        "import pyarrow; print(pyarrow.get_include()); print(pyarrow.get_library_dirs()[0])",
    ])

    if result.return_code != 0:
        # buildifier: disable=print
        print("NOTE: pyarrow not found, Arrow Flight store will not be available.")
        repository_ctx.file("BUILD.bazel", _STUB_BUILD)
        return

    lines = result.stdout.strip().split("\n")
    include_dir = lines[0]
    lib_dir = lines[1]

    repository_ctx.symlink(include_dir, "include")
    repository_ctx.symlink(lib_dir, "lib")

    repository_ctx.file("BUILD.bazel", """\
load("@rules_cc//cc:defs.bzl", "cc_library")

cc_library(
    name = "arrow",
    hdrs = glob(["include/arrow/**/*.h"]),
    includes = ["include"],
    linkopts = select({
        "@platforms//os:osx": ["-undefined", "dynamic_lookup"],
        "//conditions:default": [],
    }),
    srcs = select({
        "@platforms//os:osx": glob([
            "lib/libarrow.dylib",
            "lib/libarrow.*.dylib",
            "lib/libarrow_flight.dylib",
            "lib/libarrow_flight.*.dylib",
        ]),
        "//conditions:default": glob([
            "lib/libarrow.so*",
            "lib/libarrow_flight.so*",
        ]),
    }),
    visibility = ["//visibility:public"],
)
""")

pyarrow_repo = repository_rule(
    implementation = _pyarrow_repo_impl,
    environ = ["PATH", "PYTHONPATH", "PYTHON3_BIN_PATH", "VIRTUAL_ENV"],
)
