load("//bazel:python.bzl", "doctest")

doctest(
    files = glob(
        ["**/*.py"],
        exclude = ["**/thirdparty_files/**"],
    ),
    tags = ["team:core"],
)

filegroup(
    name = "src_files",
    srcs = glob(
        ["**/*.py"],
        exclude = ["**/thirdparty_files/**"],
    ),
    visibility = ["//:__pkg__"],
)
