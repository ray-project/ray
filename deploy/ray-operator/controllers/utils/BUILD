load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["util.go"],
    importpath = "ray-operator/controllers/utils",
    visibility = ["//visibility:public"],
    deps = ["@io_k8s_api//core/v1:go_default_library"],
)
