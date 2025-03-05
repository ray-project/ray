load("//bazel:python.bzl", "doctest")

doctest(
    files = glob(
        ["**/*.py"],
        exclude = ["_private/aliyun/**"],
    ),
    tags = ["team:core"],
)
