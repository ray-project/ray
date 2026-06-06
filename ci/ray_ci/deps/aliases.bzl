"""Defines the per-platform alias targets for the Windows driver deps.

This file loads both pip repositories, so it is intentionally separate from
//bazel:ci_require.bzl: it is only loaded by //ci/ray_ci/deps:BUILD.bazel, which
is parsed solely when a Windows driver target (e.g. //ci/ray_ci:test_in_docker)
is built. Minimal contexts that never build those targets never trigger the
@py_deps_windows fetch through here.
"""

load("@py_deps_py310//:requirements.bzl", _require = "requirement")
load("@py_deps_windows//:requirements.bzl", _windows_require = "requirement")
load("//bazel:ci_require.bzl", "WINDOWS_DRIVER_DEPS", "normalize_dep")

def ci_require_aliases():
    """Define one alias per WINDOWS_DRIVER_DEPS package, selecting the pip repo by platform."""
    for name in WINDOWS_DRIVER_DEPS:
        native.alias(
            name = normalize_dep(name),
            actual = select({
                "@platforms//os:windows": _windows_require(name),
                "//conditions:default": _require(name),
            }),
            visibility = ["//visibility:public"],
        )

    # Windows-only: pyopenssl is bundled so it lands in the driver's runfiles and
    # shadows the agent's stale conda pyOpenSSL (botocore imports OpenSSL via
    # urllib3.contrib.pyopenssl). It is an optional botocore extra, so nothing
    # pulls it transitively; the driver must depend on it explicitly. It is not
    # in @py_deps_py310, so this alias has no default branch and must only be
    # referenced from a select() //conditions:windows branch.
    native.alias(
        name = "pyopenssl_windows",
        actual = _windows_require("pyopenssl"),
        visibility = ["//visibility:public"],
    )
