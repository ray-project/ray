load("//bazel:python.bzl", "doctest")

doctest(
    name = "doctest",
    files = glob(["*.py"], exclude=["yerb.py"]),
    size = "small",
    tags = ["team:spam", "doctest"],
    deps = ["//:ray_lib"],
)

doctest(
    name = "doctest_gpu",
    size = "small",
    files = ["yerb.py"],
    tags = ["team:spam", "doctest", "gpu_only"],
    deps = ["//:ray_lib"],
)
