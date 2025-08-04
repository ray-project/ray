load("//bazel:python.bzl", "doctest")

doctest(
    files = glob(
        ["**/*.py"],
        exclude = ["_private/aliyun/**"],
    ),
    tags = ["team:core"],
)

filegroup(
    name = "default_configs",
    srcs = [
        "_private/readonly/defaults.yaml",
        "vsphere/defaults.yaml",
        "//python/ray/autoscaler/aws:default_config",
        "//python/ray/autoscaler/azure:default_config",
        "//python/ray/autoscaler/gcp:default_config",
        "//python/ray/autoscaler/local:default_config",
    ],
    visibility = ["//:__pkg__"],
)

filegroup(
    name = "src_files",
    srcs = glob([
        "*.py",
        "_private/*.py",
    ]),
    visibility = ["//:__pkg__"],
)
