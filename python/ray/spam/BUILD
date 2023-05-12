load("//bazel:python.bzl", "doctest")

doctest(
    name = "doctest",
    files = glob(["*.py"], exclude=["yerb.py"]),
    size = "small",
    tags = ["team:spam"],
    deps = ["//:ray_lib"],
)

doctest(
    name = "doctest[gpu]",
    size = "small",
    files = ["yerb.py"],
    tags = ["team:spam", "gpu"],
    deps = ["//:ray_lib"],
)
