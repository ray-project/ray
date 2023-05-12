load("//bazel:python.bzl", "doctest")

doctest(
    name = "doctest",
    files = glob(["*.rst"]),
    size = "small",
    tags = ["team:spam"],
    deps = ["//:ray_lib"],
)
