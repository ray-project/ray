load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = [
        "groupversion_info.go",
        "raycluster_types.go",
        "zz_generated.deepcopy.go",
    ],
    importpath = "ray-operator/api/v1alpha1",
    visibility = ["//visibility:public"],
    deps = [
        "@io_k8s_api//core/v1:go_default_library",
        "@io_k8s_apimachinery//pkg/apis/meta/v1:go_default_library",
        "@io_k8s_apimachinery//pkg/runtime:go_default_library",
        "@io_k8s_apimachinery//pkg/runtime/schema:go_default_library",
        "@io_k8s_sigs_controller_runtime//pkg/scheme:go_default_library",
    ],
)
