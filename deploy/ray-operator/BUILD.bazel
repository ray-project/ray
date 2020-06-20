load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")
load("@bazel_gazelle//:def.bzl", "gazelle")

# gazelle:prefix ray-operator
# gazelle:build_file_name BUILD.bazel
gazelle(name = "gazelle")

go_library(
    name = "go_default_library",
    srcs = ["main.go"],
    importpath = "ray-operator",
    visibility = ["//visibility:private"],
    deps = [
        "//api/v1alpha1:go_default_library",
        "//controllers:go_default_library",
        "@io_k8s_apimachinery//pkg/runtime:go_default_library",
        "@io_k8s_client_go//kubernetes/scheme:go_default_library",
        "@io_k8s_client_go//plugin/pkg/client/auth/gcp:go_default_library",
        "@io_k8s_sigs_controller_runtime//:go_default_library",
        "@io_k8s_sigs_controller_runtime//pkg/log/zap:go_default_library",
    ],
)

go_binary(
    name = "ray-operator",
    embed = [":go_default_library"],
    visibility = ["//visibility:public"],
)
