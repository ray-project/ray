load("//bazel:python.bzl", "doctest")

doctest(
    name = "doctest_spam",
    size = "small",
    files = glob(["*.py"], exclude=["yerb.py"]),
    tags = ["team:spam", "doctest"],
    deps = ["//:ray_lib"],
)

doctest(
    name = "doctest_spam_gpu",
    size = "small",
    files = ["yerb.py"],
    tags = ["team:spam", "doctest"],
    deps = ["//:ray_lib"],
)
