# --------------------------------------------------------------------
# Tests from the python/ray/util/sgd/v2/tests directory.
# Please keep these sorted alphabetically.
# --------------------------------------------------------------------

py_test(
    name = "test_trainer",
    size = "small",
    srcs = ["tests/test_trainer.py"],
    tags = ["exclusive"],
    deps = [":sgd_v2_lib"]
)

py_test(
    name = "test_worker_group",
    size = "small",
    srcs = ["tests/test_worker_group.py"],
    tags = ["exclusive"],
    deps = [":sgd_v2_lib"]
)



# This is a dummy test dependency that causes the above tests to be
# re-run if any of these files changes.
py_library(
    name = "sgd_v2_lib",
    srcs = glob(["**/*.py"], exclude=["tests/*.py"]),
)
