"""Driver dep aliases: bundled from @py_deps_py310 off Windows, unbundled on Windows."""

load("@py_deps_py310//:requirements.bzl", _require = "requirement")
load("@rules_python//python:defs.bzl", "py_library")
load("//bazel:ci_require.bzl", "WINDOWS_DRIVER_DEPS", "normalize_dep")

def ci_require_aliases():
    py_library(
        name = "_unbundled",
        visibility = ["//visibility:public"],
    )

    for name in WINDOWS_DRIVER_DEPS:
        native.alias(
            name = normalize_dep(name),
            actual = select({
                "@platforms//os:windows": ":_unbundled",
                "//conditions:default": _require(name),
            }),
            visibility = ["//visibility:public"],
        )
