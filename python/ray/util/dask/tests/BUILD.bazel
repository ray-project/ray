# --------------------------------------------------------------------
# Tests from the python/ray/util/dask/tests directory.
# Please keep these sorted alphabetically.
# --------------------------------------------------------------------

load("@rules_python//python:defs.bzl", "py_test")

py_test(
    name = "test_dask_callback",
    size = "small",
    srcs = ["test_dask_callback.py"],
    tags = [
        "exclusive",
        "team:core",
    ],
    deps = ["//python/ray/util/dask:dask_lib"],
)

py_test(
    name = "test_dask_callback_client_mode",
    size = "medium",
    srcs = ["test_dask_callback.py"],
    main = "test_dask_callback.py",
    tags = [
        "client",
        "exclusive",
        "team:core",
    ],
    deps = ["//python/ray/util/dask:dask_lib"],
)

py_test(
    name = "test_dask_optimization",
    size = "small",
    srcs = ["test_dask_optimization.py"],
    tags = [
        "exclusive",
        "team:core",
    ],
    deps = ["//python/ray/util/dask:dask_lib"],
)

py_test(
    name = "test_dask_multi_node",
    size = "medium",
    srcs = ["test_dask_multi_node.py"],
    main = "test_dask_multi_node.py",
    tags = [
        "exclusive",
        "team:core",
    ],
    deps = ["//python/ray/util/dask:dask_lib"],
)

py_test(
    name = "test_dask_optimization_client_mode",
    size = "small",
    srcs = ["test_dask_optimization.py"],
    main = "test_dask_optimization.py",
    tags = [
        "client",
        "exclusive",
        "team:core",
    ],
    deps = ["//python/ray/util/dask:dask_lib"],
)

py_test(
    name = "test_dask_scheduler",
    size = "small",
    srcs = ["test_dask_scheduler.py"],
    tags = [
        "exclusive",
        "team:core",
    ],
    deps = ["//python/ray/util/dask:dask_lib"],
)

py_test(
    name = "test_dask_scheduler_client_mode",
    size = "small",
    srcs = ["test_dask_scheduler.py"],
    main = "test_dask_scheduler.py",
    tags = [
        "client",
        "exclusive",
        "team:core",
    ],
    deps = ["//python/ray/util/dask:dask_lib"],
)
