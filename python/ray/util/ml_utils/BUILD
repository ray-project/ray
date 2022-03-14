# --------------------------------------------------------------------
# Tests from the python/ray/util/ml_util/tests directory.
# Please keep these sorted alphabetically.
# --------------------------------------------------------------------
py_test(
    name = "test_mlflow",
    size = "medium",
    srcs = ["tests/test_mlflow.py"],
    tags = ["team:ml", "exclusive"],
    deps = [":ml_util_lib"]
)

# This is a dummy test dependency that causes the above tests to be
# re-run if any of these files changes.
py_library(
    name = "ml_util_lib",
    srcs = glob(["**/*.py"], exclude=["tests/*.py"]),
)
