# --------------------------------------------------------------------
# Tests from the python/ray/data/tests directory.
# Covers all tests starting with `test_`.
# Please keep these sorted alphabetically.
# --------------------------------------------------------------------
load("//bazel:python.bzl", "py_test_module_list")

py_library(
    name = "conftest",
    srcs = ["tests/conftest.py"],
    deps = ["//python/ray/tests:conftest"],
)

py_test_module_list(
    files = glob(["tests/preprocessors/test_*.py"]),
    size = "small",
    tags = ["team:ml", "exclusive", "ray_air"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_dataset_formats",
    size = "large",
    srcs = ["tests/test_dataset_formats.py"],
    data = glob(["tests/image-folder/**/*"]),
    tags = ["team:core", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_mongo_dataset",
    size = "large",
    srcs = ["tests/test_mongo_dataset.py"],
    tags = ["team:core", "exclusive", "dataset_integration"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test_module_list(
  files = glob(
    include=["tests/test_*.py"],
    exclude=[
        "tests/test_preprocessors.py",
        "tests/test_dataset_formats.py",
	"tests/test_mongo_dataset.py",
    ],
  ),
  size = "large",
  tags = ["team:core", "exclusive"],
  deps = ["//:ray_lib", ":conftest"],
)
