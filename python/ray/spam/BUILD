load("//bazel:python.bzl", "doctest")

doctest(
    files = glob(["*.py"], exclude=["gpu_module.py"]),
    tags = ["team:spam"],
)

doctest(
    files = ["gpu_module.py"],
    gpu = True,
    tags = ["team:spam"],
)
