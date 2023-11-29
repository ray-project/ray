load("//bazel:python.bzl", "doctest")

doctest(
    files = glob(["**/*.py"], exclude=["**/internal_thirdparty_files/**"]),
    tags = ["team:core"]
)
