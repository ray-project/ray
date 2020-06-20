load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = [
        "constant.go",
        "meta.go",
        "pod.go",
        "service.go",
    ],
    importpath = "ray-operator/controllers/common",
    visibility = ["//visibility:public"],
    deps = [
        "//api/v1alpha1:go_default_library",
        "//controllers/utils:go_default_library",
        "@io_k8s_api//core/v1:go_default_library",
        "@io_k8s_apimachinery//pkg/apis/meta/v1:go_default_library",
    ],
)
