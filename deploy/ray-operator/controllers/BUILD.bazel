load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = ["raycluster_controller.go"],
    importpath = "ray-operator/controllers",
    visibility = ["//visibility:public"],
    deps = [
        "//api/v1alpha1:go_default_library",
        "//controllers/common:go_default_library",
        "//controllers/utils:go_default_library",
        "@com_github_deckarep_golang_set//:go_default_library",
        "@com_github_go_logr_logr//:go_default_library",
        "@io_k8s_api//apps/v1beta1:go_default_library",
        "@io_k8s_api//core/v1:go_default_library",
        "@io_k8s_apimachinery//pkg/api/errors:go_default_library",
        "@io_k8s_apimachinery//pkg/runtime:go_default_library",
        "@io_k8s_sigs_controller_runtime//:go_default_library",
        "@io_k8s_sigs_controller_runtime//pkg/client:go_default_library",
        "@io_k8s_sigs_controller_runtime//pkg/controller:go_default_library",
        "@io_k8s_sigs_controller_runtime//pkg/controller/controllerutil:go_default_library",
        "@io_k8s_sigs_controller_runtime//pkg/handler:go_default_library",
        "@io_k8s_sigs_controller_runtime//pkg/manager:go_default_library",
        "@io_k8s_sigs_controller_runtime//pkg/reconcile:go_default_library",
        "@io_k8s_sigs_controller_runtime//pkg/runtime/log:go_default_library",
        "@io_k8s_sigs_controller_runtime//pkg/source:go_default_library",
    ],
)

go_test(
    name = "go_default_test",
    srcs = ["suite_test.go"],
    embed = [":go_default_library"],
    deps = [
        "//api/v1alpha1:go_default_library",
        "@com_github_onsi_ginkgo//:go_default_library",
        "@com_github_onsi_gomega//:go_default_library",
        "@io_k8s_client_go//kubernetes/scheme:go_default_library",
        "@io_k8s_client_go//rest:go_default_library",
        "@io_k8s_sigs_controller_runtime//pkg/client:go_default_library",
        "@io_k8s_sigs_controller_runtime//pkg/envtest:go_default_library",
        "@io_k8s_sigs_controller_runtime//pkg/log:go_default_library",
        "@io_k8s_sigs_controller_runtime//pkg/log/zap:go_default_library",
    ],
)
