load("@rules_foreign_cc//foreign_cc:defs.bzl", "make")
load("@com_github_ray_project_ray//bazel:ray.bzl", "filter_files_with_suffix")

filegroup(
    name = "all",
    srcs = glob(["**"]),
)

make(
    name = "libzstd",
    lib_source = ":all",
    args = ["ZSTD_NO_ASM=1"],
    visibility = ["//visibility:public"],
)
