load("@rules_foreign_cc//foreign_cc:configure.bzl", "configure_make")
load("@io_ray//bazel:ray.bzl", "filter_files_with_suffix")

filegroup(
    name = "all",
    srcs = glob(["**"]),
)

configure_make(
    name = "libjemalloc",
    lib_source = ":all",
    linkopts = ["-ldl"],
    copts = ["-fPIC"],
    args = ["-j"],
    out_shared_libs = ["libjemalloc.so"],
    # See https://salsa.debian.org/debian/jemalloc/-/blob/c0a88c37a551be7d12e4863435365c9a6a51525f/debian/rules#L8-23
    # for why we are setting "--with-lg-page" on non x86 hardware here.
    configure_options = ["--disable-static", "--enable-prof", "--enable-prof-libunwind"] +
        select({
            "@platforms//cpu:x86_64": [],
            "//conditions:default": ["--with-lg-page=16"],
        }),
    visibility = ["//visibility:public"],
)


filter_files_with_suffix(
    name = "shared",
    srcs = ["@jemalloc//:libjemalloc"],
    suffix = ".so",
    visibility = ["//visibility:public"],
)
