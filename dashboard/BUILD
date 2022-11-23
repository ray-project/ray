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

py_library(
    name = "conftest",
    srcs = ["tests/conftest.py"],
    deps = ["//python/ray/tests:conftest"],
)

py_test(
    name = "test_job_agent_only_conda",
    size = "medium",
    srcs = ["modules/job/tests/test_job_agent.py"],
    tags = ["team:core"],
)
