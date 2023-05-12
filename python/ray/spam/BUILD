load("//bazel:python.bzl", "doctest")

doctest(
    name = "ham",
    size = "small",
    files = ["ham.py", "eggs.py"],
    tags = ["team:data", "doctest"],
    deps = ["//:ray_lib"],
)
