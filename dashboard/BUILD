load("//bazel:python.bzl", "py_test_run_all_subdirectory")

# This is a dummy test dependency that causes the above tests to be
# re-run if any of these files changes.
py_library(
    name = "dashboard_lib",
    srcs = glob(
        ["**/*.py"],
        exclude = ["tests/*"],
    ),
)

py_test_run_all_subdirectory(
    size = "medium",
    include = ["**/test*.py"],
    exclude = ["modules/test/**", "modules/node/tests/test_node.py", "tests/test_dashboard.py"],
    extra_srcs = [],
    tags = ["exclusive", "team:serve"],
)

py_test(
    name = "test_node",
    size = "medium",
    srcs = ["modules/node/tests/test_node.py"],
    tags = ["exclusive", "team:serve"],
)

py_test(
    name = "test_dashboard",
    size = "medium",
    srcs = ["tests/test_dashboard.py"],
    tags = ["exclusive", "team:serve"],
)
