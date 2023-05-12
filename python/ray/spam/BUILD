load("//bazel:python.bzl", "doctest")

doctest(
    name = "ham",
    size = "small",
    srcs = ["ham.py", "eggs.py"],
    tags = ["team:data", "doctest"],
    deps = ["//:ray_lib"],
)
