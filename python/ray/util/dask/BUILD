# --------------------------------------------------------------------
# Tests from the python/ray/util/dask/tests directory.
# Please keep these sorted alphabetically.
# --------------------------------------------------------------------

py_test(
    name = "test_dask_callback",
    size = "small",
    srcs = ["tests/test_dask_callback.py"],
    tags = ["team:core", "exclusive"],
    deps = [":dask_lib"]
)

py_test(
    name = "test_dask_callback_client_mode",
    size = "medium",
    main = "test_dask_callback.py",
    srcs = ["tests/test_dask_callback.py"],
    tags = ["team:serverless", "exclusive", "client"],
    deps = [":dask_lib"]
)

py_test(
    name = "test_dask_optimization",
    size = "small",
    srcs = ["tests/test_dask_optimization.py"],
    tags = ["team:core", "exclusive"],
    deps = [":dask_lib"]
)

py_test(
    name = "test_dask_optimization_client_mode",
    size = "small",
    main = "test_dask_optimization.py",
    srcs = ["tests/test_dask_optimization.py"],
    tags = ["team:serverless", "exclusive", "client"],
    deps = [":dask_lib"]
)

py_test(
    name = "test_dask_scheduler",
    size = "small",
    srcs = ["tests/test_dask_scheduler.py"],
    tags = ["team:core", "exclusive"],
    deps = [":dask_lib"]
)

py_test(
    name = "test_dask_scheduler_client_mode",
    size = "small",
    main = "test_dask_scheduler.py",
    srcs = ["tests/test_dask_scheduler.py"],
    tags = ["team:serverless", "exclusive", "client"],
    deps = [":dask_lib"]
)

# --------------------------------------------------------------------
# Tests from the python/ray/util/dask/examples directory.
# Please keep these sorted alphabetically.
# --------------------------------------------------------------------

py_test(
    name = "dask_ray_persist_example",
    size = "medium",
    srcs = ["examples/dask_ray_persist_example.py"],
    tags = ["team:core", "exclusive"],
    deps = [":dask_lib"],
)

py_test(
    name = "dask_ray_persist_example_client_mode",
    size = "medium",
    main = "dask_ray_persist_example.py",
    srcs = ["examples/dask_ray_persist_example.py"],
    tags = ["team:serverless", "exclusive", "client"],
    deps = [":dask_lib"],
)

py_test(
    name = "dask_ray_scheduler_example",
    size = "medium",
    srcs = ["examples/dask_ray_scheduler_example.py"],
    tags = ["team:core", "exclusive"],
    deps = [":dask_lib"],
)

py_test(
    name = "dask_ray_scheduler_example_client_mode",
    size = "medium",
    main = "dask_ray_scheduler_example.py",
    srcs = ["examples/dask_ray_scheduler_example.py"],
    tags = ["team:serverless", "exclusive", "client"],
    deps = [":dask_lib"],
)

py_test(
    name = "dask_ray_shuffle_optimization",
    size = "medium",
    srcs = ["examples/dask_ray_shuffle_optimization.py"],
    tags = ["team:core", "exclusive"],
    deps = [":dask_lib"],
)


py_test(
    name = "dask_ray_shuffle_optimization_client_mode",
    size = "medium",
    main = "dask_ray_shuffle_optimization.py",
    srcs = ["examples/dask_ray_shuffle_optimization.py"],
    tags = ["team:serverless", "exclusive", "client"],
    deps = [":dask_lib"],
)

# This is a dummy test dependency that causes the above tests to be
# re-run if any of these files changes.
py_library(
    name = "dask_lib",
    srcs = glob(["**/*.py"], exclude=["tests/*.py"]),
)
