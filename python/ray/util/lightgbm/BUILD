# --------------------------------------------------------------------
# Tests from the python/ray/util/lightgbm directory.
# Please keep these sorted alphabetically.
# --------------------------------------------------------------------
py_test(
 name = "simple_example",
 size = "small",
 srcs = ["simple_example.py"],
 deps = [":lgbm_lib"],
 tags = ["team:ml", "exclusive"],
)

py_test(
 name = "simple_tune",
 size="small",
 srcs = ["simple_tune.py"],
 deps = [":lgbm_lib"],
 tags = ["team:ml", "exclusive"]
)

py_test(
    name = "test_client",
    size = "small",
    srcs = ["tests/test_client.py"],
    deps = [":lgbm_lib"],
    tags = ["team:ml", "exclusive", "client"]
)

# This is a dummy test dependency that causes the above tests to be
# re-run if any of these files changes.
py_library(
 name = "lgbm_lib",
 srcs = glob(["**/*.py"]),
)


