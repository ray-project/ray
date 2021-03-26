# --------------------------------------------------------------------
# Tests from the python/ray/util/lightning_accelerators/tests directory.
# Please keep these sorted alphabetically.
# --------------------------------------------------------------------

py_test(
    name = "test_horovod_ray_accelerator",
    size = "medium",
    srcs = ["tests/test_horovod_ray_accelerator.py"],
    tags = ["exclusive", "pytorch-lightning", "pytorch", "horovod"],
    deps = [":accelerator_lib"],
)

# --------------------------------------------------------------------
# Tests from the python/ray/util/lightning_accelerators/examples directory.
# Please keep these sorted alphabetically.
# --------------------------------------------------------------------

# py_test(
#     name = "ptl_horovod_ray_example",
#     size = "medium",
#     srcs = ["examples/ptl_horovod_ray_example.py"],
#     tags = ["exclusive", "example", "pytorch-lightning", "pytorch", "horovod"],
#     deps = [":accelerator_lib"],
#     args = ["--smoke-test"]
# )

# # This is a dummy test dependency that causes the above tests to be
# # re-run if any of these files changes.
py_library(
 name = "accelerator_lib",
 srcs = glob(["**/*.py"], exclude=["tests/*.py"]),
)
