load("//bazel:python.bzl", "doctest")

doctest(
    files = glob(["*.rst"], exclude=["gpu_guide.rst"]),
    tags = ["team:spam"],
)

doctest(
    files = ["gpu_guide.rst"],
    tags = ["team:spam"],
    gpu = True,
)
