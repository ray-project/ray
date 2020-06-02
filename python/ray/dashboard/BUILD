# This is a dummy test dependency that causes the above tests to be
# re-run if any of these files changes.
py_library(
    name = "dashboard_lib",
    srcs = glob(["**/*.py"],exclude=["tests/*"]),
)

py_test(
    name = "test_node_stats",
    size = "small",
    srcs = glob(["tests/*.py"]),
    deps = [":dashboard_lib"]
)
