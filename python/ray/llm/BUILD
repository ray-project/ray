load("//bazel:python.bzl", "doctest")

doctest(
    files = glob(["**/*.py"], exclude=glob(["examples/**/*", "tests/**/*"])),
    tags = ["team:llm"]
)
# --------------------------------------------------------------------
# Tests from the python/ray/llm/tests directory.
# Covers all tests starting with `test_`.
# Please keep these sorted alphabetically.
# --------------------------------------------------------------------
load("//bazel:python.bzl", "py_test_module_list")

py_test_module_list(
    files = glob(["tests/batch/**/test_*.py"]),
    size = "medium",
    tags = ["team:llm", "exclusive"],
    deps = ["//:ray_lib"],
)
