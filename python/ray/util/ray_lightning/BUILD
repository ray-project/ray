# --------------------------------------------------------------------
# Tests from the python/ray/util/ray_lightning directory.
# Please keep these sorted alphabetically.
# --------------------------------------------------------------------
py_test(
 name = "simple_example",
 size = "small",
 srcs = ["simple_example.py"],
 deps = [":lightning_lib"],
 tags = ["team:ml", "exclusive"],
)

py_test(
 name = "simple_tune",
 size="small",
 srcs = ["simple_tune.py"],
 deps = [":lightning_lib"],
 tags = ["team:ml", "exclusive"]
)

# This is a dummy test dependency that causes the above tests to be
# re-run if any of these files changes.
py_library(
 name = "lightning_lib",
 srcs = glob(["**/*.py"]),
)