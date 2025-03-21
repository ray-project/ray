load("//bazel:python.bzl", "doctest")

doctest(
    files = glob(
        ["**/*.py"],
        exclude = ["**/thirdparty_files/**"],
    ),
    tags = ["team:core"],
)
