# --------------------------------------------------------------------
# Tests from the python/ray/util/xgboost directory.
# Please keep these sorted alphabetically.
# --------------------------------------------------------------------
py_test(
 name = "simple_example",
 size = "small",
 srcs = ["simple_example.py"],
 deps = [":xgb_lib"],
 tags = ["team:ml", "exclusive"],
)

py_test(
 name = "simple_tune",
 size="small",
 srcs = ["simple_tune.py"],
 deps = [":xgb_lib"],
 tags = ["team:ml", "exclusive"]
)

py_test(
    name = "test_client",
    size = "small",
    srcs = ["tests/test_client.py"],
    deps = [":xgb_lib"],
    tags = ["team:ml", "exclusive", "client"]
)

# This is a dummy test dependency that causes the above tests to be
# re-run if any of these files changes.
py_library(
 name = "xgb_lib",
 srcs = glob(["**/*.py"]),
)


