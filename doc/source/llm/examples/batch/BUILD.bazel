load("//bazel:python.bzl", "py_test_run_all_notebooks")

filegroup(
    name = "batch_examples",
    srcs = glob(["*.ipynb"]),
    visibility = ["//doc:__subpackages__"],
)

# GPU Tests
py_test_run_all_notebooks(
    size = "large",
    include = ["*.ipynb"],
    data = ["//doc/source/llm/examples/batch:batch_examples"],
    exclude = [],
    tags = [
        "exclusive",
        "gpu",
        "ray_air",
        "team:llm",
    ],
)
