py_test(
    name = "test_bazel_sharding",
    size = "small",
    srcs = ["tests/test_bazel_sharding.py"],
    data = ["tests/mock_BUILD", "bazel_sharding.py"],
    tags = ["team:ml"],
    deps = [":bazel_sharding"]
)

# This is a dummy test dependency that causes the above tests to be
# re-run if any of these files changes.
py_library(
    name = "bazel_sharding",
    srcs = glob(["**/*.py"], exclude=["tests/*"]),
)
