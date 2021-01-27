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
    size = "small",
    include = ["**/test*.py"],
    exclude = ["modules/test/**", "modules/stats_collector/tests/test_stats_collector.py", "tests/test_dashboard.py"],
    extra_srcs = [],
    tags = ["exclusive"],
)

py_test(
    name = "test_stats_collector",
    size = "medium",
    srcs = ["modules/stats_collector/tests/test_stats_collector.py"],
)

py_test(
    name = "test_dashboard",
    size = "medium",
    srcs = ["tests/test_dashboard.py"],
)
