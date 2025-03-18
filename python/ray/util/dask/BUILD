# --------------------------------------------------------------------
# Tests from the python/ray/util/dask/tests directory.
# Please keep these sorted alphabetically.
# --------------------------------------------------------------------

load("@rules_python//python:defs.bzl", "py_library", "py_test")

py_test(
    name = "test_dask_callback",
    size = "small",
    srcs = ["tests/test_dask_callback.py"],
    tags = [
        "exclusive",
        "team:core",
    ],
    deps = [":dask_lib"],
)

py_test(
    name = "test_dask_callback_client_mode",
    size = "medium",
    srcs = ["tests/test_dask_callback.py"],
    main = "test_dask_callback.py",
    tags = [
        "client",
        "exclusive",
        "team:serverless",
    ],
    deps = [":dask_lib"],
)

py_test(
    name = "test_dask_optimization",
    size = "small",
    srcs = ["tests/test_dask_optimization.py"],
    tags = [
        "exclusive",
        "team:core",
    ],
    deps = [":dask_lib"],
)

py_test(
    name = "test_dask_optimization_client_mode",
    size = "small",
    srcs = ["tests/test_dask_optimization.py"],
    main = "test_dask_optimization.py",
    tags = [
        "client",
        "exclusive",
        "team:serverless",
    ],
    deps = [":dask_lib"],
)

py_test(
    name = "test_dask_scheduler",
    size = "small",
    srcs = ["tests/test_dask_scheduler.py"],
    tags = [
        "exclusive",
        "team:core",
    ],
    deps = [":dask_lib"],
)

py_test(
    name = "test_dask_scheduler_client_mode",
    size = "small",
    srcs = ["tests/test_dask_scheduler.py"],
    main = "test_dask_scheduler.py",
    tags = [
        "client",
        "exclusive",
        "team:serverless",
    ],
    deps = [":dask_lib"],
)

# This is a dummy test dependency that causes the above tests to be
# re-run if any of these files changes.
py_library(
    name = "dask_lib",
    srcs = glob(
        ["**/*.py"],
        exclude = ["tests/*.py"],
    ),
)
