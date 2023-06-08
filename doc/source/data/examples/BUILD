load("//bazel:python.bzl", "py_test_run_all_notebooks")

filegroup(
    name = "data_examples",
    srcs = glob(["*.ipynb"]),
    visibility = ["//doc:__subpackages__"]
)

py_test_run_all_notebooks(
    size = "medium",
    include = ["batch_inference_object_detection.ipynb", "pytorch_resnet_batch_prediction.ipynb",
    "huggingface_vit_batch_prediction.ipynb"],
    exclude = [],
    data = ["//doc/source/data/examples:data_examples"],
    tags = ["exclusive", "team:data", "gpu"],
)
