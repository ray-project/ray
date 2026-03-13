load("@rules_foreign_cc//foreign_cc:defs.bzl", "make")

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
