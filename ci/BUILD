py_test(
    name = "test_bazel_sharding",
    size = "small",
    srcs = ["tests/test_bazel_sharding.py", "tests/mock_BUILD"],
    tags = [],
    deps = [":ci_tools_run"]
)

# This is a dummy test dependency that causes the above tests to be
# re-run if any of these files changes.
py_library(
    name = "ci_tools_run",
    srcs = glob(["run/**/*"]),
)
