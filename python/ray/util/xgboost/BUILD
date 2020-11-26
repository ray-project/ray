# --------------------------------------------------------------------
# Tests from the python/ray/util/sgd/tests directory.
# Please keep these sorted alphabetically.
# --------------------------------------------------------------------
py_test(
    name = "example",
    size = "small",
    srcs = ["simple_example.py"],
    deps = [":xgb_lib"],
    tags = ["exclusive"],
)

# This is a dummy test dependency that causes the above tests to be
# re-run if any of these files changes.
py_library(
    name = "xgb_lib",
    srcs = glob(["**/*.py"], exclude=["tests/*.py"]),
)