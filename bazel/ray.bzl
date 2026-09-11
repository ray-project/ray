load("@com_github_google_flatbuffers//:build_defs.bzl", "flatbuffer_library_public")
load("@rules_cc//cc:defs.bzl", "cc_binary", "cc_library", "cc_test")

COPTS_TESTS = select({
    "//:opt": ["-DBAZEL_OPT"],
    "//conditions:default": [],
}) + select({
    "@platforms//os:windows": [
        # TODO(mehrdadn): (How to) support dynamic linking?
        "-DRAY_STATIC",
        # Prevent Windows.h from including WinSock.h, which conflicts with
        # WinSock2.h used by Boost.Asio.
        "-DWIN32_LEAN_AND_MEAN",
    ],
    "//conditions:default": [
        "-Wunused-result",
        "-Wconversion-null",
        "-Wno-misleading-indentation",
        "-Wimplicit-fallthrough",
    ],
}) + select({
    "//:clang-cl": [
        "-Wno-builtin-macro-redefined",  # To get rid of warnings caused by deterministic build macros (e.g. #define __DATE__ "redacted")
        "-Wno-microsoft-unqualified-friend",  # This shouldn't normally be enabled, but otherwise we get: google/protobuf/map_field.h: warning: unqualified friend declaration referring to type outside of the nearest enclosing namespace is a Microsoft extension; add a nested name specifier (for: friend class DynamicMessage)
    ],
    "//conditions:default": [],
})

COPTS = COPTS_TESTS + select({
    "@platforms//os:windows": [""],
    "//conditions:default": ["-Wshadow"],
})

PYX_COPTS = select({
    "//:msvc-cl": [],
    "//conditions:default": [
        # Ignore this warning since CPython and Cython have issue removing deprecated tp_print on MacOS
        "-Wno-deprecated-declarations",
        "-Wno-shadow",
        "-Wno-implicit-fallthrough",
    ],
}) + select({
    "@platforms//os:windows": [
        "/FI" + "src/shims/windows/python-nondebug.h",
    ],
    "//conditions:default": [],
})

PYX_SRCS = [] + select({
    "@platforms//os:windows": [
        "src/shims/windows/python-nondebug.h",
    ],
    "//conditions:default": [],
})

def flatbuffer_py_library(name, srcs, outs, out_prefix, includes = [], include_paths = []):
    flatbuffer_library_public(
        name = name,
        srcs = srcs,
        outs = outs,
        language_flag = "-p",
        out_prefix = out_prefix,
        include_paths = include_paths,
        includes = includes,
    )

def ray_cc_library(name, strip_include_prefix = "/src", copts = [], visibility = ["//visibility:public"], **kwargs):
    cc_library(
        name = name,
        strip_include_prefix = strip_include_prefix,
        copts = COPTS + copts,
        visibility = visibility,
        **kwargs
    )

def ray_cc_test(name, deps = [], linkopts = [], copts = [], use_ray_gtest_main = True, **kwargs):
    # By default, all `ray_cc_test` targets use `ray_gtest_main`.
    # Some tests might need bespoke setup logic, so let them skip this dependency.
    if use_ray_gtest_main:
      deps = deps + ["//src/ray/common:ray_gtest_main"]

    cc_test(
        name = name,
        deps = deps,
        copts = COPTS_TESTS + copts,
        linkopts = linkopts + ["-pie"],
        **kwargs
    )

def ray_cc_binary(name, linkopts = [], copts = [], **kwargs):
    cc_binary(
        name = name,
        copts = COPTS + copts,
        linkopts = linkopts + ["-pie"],
        **kwargs
    )

def _filter_files_with_suffix_impl(ctx):
    suffix = ctx.attr.suffix
    filtered_files = [f for f in ctx.files.srcs if f.basename.endswith(suffix)]
    print(filtered_files)
    return [
        DefaultInfo(
            files = depset(filtered_files),
        ),
    ]

filter_files_with_suffix = rule(
    implementation = _filter_files_with_suffix_impl,
    attrs = {
        "srcs": attr.label_list(allow_files = True),
        "suffix": attr.string(),
    },
)

# It will be passed to the FlatBuffers compiler when defining flatbuffer_cc_library
FLATC_ARGS = [
    "--gen-object-api",
    "--gen-mutable",
    "--scoped-enums",
]
