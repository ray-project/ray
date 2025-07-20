load("//bazel:python.bzl", "doctest")

doctest(
    files = glob(
        ["**/*.py"],
        exclude = ["_private/aliyun/**"],
    ),
    tags = ["team:core"],
)

filegroup(
    name = "default_yamls",
    srcs = [
        "_private/readonly/defaults.yaml",
        "aws/defaults.yaml",
        "azure/defaults.yaml",
        "gcp/defaults.yaml",
        "local/defaults.yaml",
        "vsphere/defaults.yaml",
    ],
    visibility = ["//visibility:public"],
)
