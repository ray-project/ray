# --------------------------------------------------------------------
# Tests from the python/ray/util/horovod directory.
# Please keep these sorted alphabetically.
# --------------------------------------------------------------------

py_test(
    name = "test_horovod",
    size = "medium",
    srcs = ["tests/test_horovod.py"],
    deps = [":horovod_lib"],
    tags = ["team:ml", "exclusive"]
)

# This is a dummy test dependency that causes the above tests to be
# re-run if any of these files changes.
py_library(
 name = "horovod_lib",
 srcs = glob(["**/*.py"]),
)


