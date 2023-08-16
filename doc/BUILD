load("//bazel:python.bzl", "py_test_run_all_subdirectory")
load("//bazel:python.bzl", "py_test_run_all_notebooks")
load("//bazel:python.bzl", "doctest")

exports_files(["test_myst_doc.py"])

# --------------------------------------------------------------------
# Tests from the doc directory.
# Please keep these sorted alphabetically, but start with the
# root directory.
# --------------------------------------------------------------------

py_test(
    name = "highly_parallel",
    size = "medium",
    main = "test_myst_doc.py",
    srcs = ["test_myst_doc.py"],
    tags = ["exclusive", "team:ml", "highly_parallel"],
    data = ["//doc/source/ray-core/examples:core_examples"],
    args = ["--path", "doc/source/ray-core/examples/highly_parallel.ipynb"]
)

### Ray Data
py_test(
    name = "datasets_train",
    size = "medium",
    srcs = ["source/ray-core/_examples/datasets_train/datasets_train.py"],
    tags = ["exclusive", "team:ml", "datasets_train"],
    args = ["--smoke-test", "--num-workers=2", "--use-gpu"]
)

py_test(
    name = "plot_hyperparameter",
    size = "small",
    main = "test_myst_doc.py",
    srcs = ["test_myst_doc.py"],
    args = ["--path", "doc/source/ray-core/examples/plot_hyperparameter.ipynb"],
    data = ["//doc/source/ray-core/examples:core_examples"],
    tags = ["exclusive", "team:ml"]
)

py_test(
    name = "automl_for_time_series",
    size = "medium",
    main = "test_myst_doc.py",
    srcs = ["test_myst_doc.py"],
    tags = ["exclusive", "team:ml", "timeseries_libs"],
    data = ["//doc/source/ray-core/examples:core_examples"],
    args = ["--path", "doc/source/ray-core/examples/automl_for_time_series.ipynb"]
)

py_test(
    name = "batch_prediction",
    size = "medium",
    main = "test_myst_doc.py",
    srcs = ["test_myst_doc.py"],
    tags = ["exclusive", "team:ml"],
    data = ["//doc/source/ray-core/examples:core_examples"],
    args = ["--path", "doc/source/ray-core/examples/batch_prediction.ipynb"]
)

py_test(
    name = "batch_training",
    size = "medium",
    main = "test_myst_doc.py",
    srcs = ["test_myst_doc.py"],
    tags = ["exclusive", "team:ml"],
    data = ["//doc/source/ray-core/examples:core_examples"],
    args = ["--path", "doc/source/ray-core/examples/batch_training.ipynb"]
)

py_test(
    name = "plot_parameter_server",
    size = "medium",
    main = "test_myst_doc.py",
    srcs = ["test_myst_doc.py"],
    args = ["--path", "doc/source/ray-core/examples/plot_parameter_server.ipynb"],
    data = ["//doc/source/ray-core/examples:core_examples"],
    tags = ["exclusive", "team:ml"]
)

py_test(
    name = "plot_pong_example",
    size = "large",
    main = "test_myst_doc.py",
    srcs = ["test_myst_doc.py"],
    args = ["--path", "doc/source/ray-core/examples/plot_pong_example.ipynb"],
    data = ["//doc/source/ray-core/examples:core_examples"],
    tags = ["exclusive", "team:ml"]
)

# --------------------------------------------------------------------
# Test all doc/source/ray-observability/doc_code code included in rst/md files.
# --------------------------------------------------------------------

py_test_run_all_subdirectory(
    size = "medium",
    include = ["source/ray-observability/doc_code/*.py"],
    exclude = [],
    extra_srcs = [],
    tags = ["exclusive", "team:core"],
)

# --------------------------------------------------------------------
# Test all doc/source/ray-core/doc_code code included in rst/md files.
# --------------------------------------------------------------------

py_test(
    name = "doc_code_runtime_env_example",
    size = "small",
    main = "source/ray-core/doc_code/runtime_env_example.py",
    srcs = ["source/ray-core/doc_code/runtime_env_example.py"],
    tags = ["exclusive", "post_wheel_build", "team:serve"]
)

py_test_run_all_subdirectory(
    size = "medium",
    include = ["source/ray-core/doc_code/*.py"],
    exclude = ["source/ray-core/doc_code/runtime_env_example.py",
               "source/ray-core/doc_code/cross_language.py"],
    extra_srcs = [],
    tags = ["exclusive", "team:core"],
)

# --------------------------------------------------------------------
# Test all doc/source/serve/doc_code code included in rst/md files.
# --------------------------------------------------------------------

py_test_run_all_subdirectory(
    size = "medium",
    include = ["source/serve/doc_code/**/*.py"],
    exclude = [
        "source/serve/doc_code/distilbert.py",
        "source/serve/doc_code/stable_diffusion.py",
        "source/serve/doc_code/object_detection.py",
        "source/serve/doc_code/http_guide/streaming_example.py",
        "source/serve/doc_code/http_guide/websockets_example.py",
        "source/serve/doc_code/vllm_example.py",
    ],
    extra_srcs = [],
    tags = ["exclusive", "team:serve"],
)

py_test_run_all_subdirectory(
    size = "medium",
    include = [
        "source/serve/doc_code/http_guide/streaming_example.py",
        "source/serve/doc_code/http_guide/websockets_example.py",
    ],
    exclude = [],
    extra_srcs = [],
    tags = ["exclusive", "team:serve"],
    env = {"RAY_SERVE_ENABLE_EXPERIMENTAL_STREAMING": "1"},
)

py_test_run_all_subdirectory(
    size = "medium",
    include = [
        "source/serve/doc_code/distilbert.py",
        "source/serve/doc_code/stable_diffusion.py",
        "source/serve/doc_code/object_detection.py",
    ],
    exclude = [],
    extra_srcs = [],
    tags = ["exclusive", "team:serve", "gpu"],
    env = {"RAY_SERVE_PROXY_READY_CHECK_TIMEOUT_S": "60"},
)




# --------------------------------------------------------------------
# Test all doc/source/tune/doc_code code included in rst/md files.
# --------------------------------------------------------------------

py_test_run_all_subdirectory(
    size = "medium",
    include = ["source/tune/doc_code/*.py"],
    exclude = [],
    extra_srcs = [],
    tags = ["exclusive", "team:ml"],
)

# --------------------------------------------------------------------
# Test all doc/source/rllib/doc_code code included in rst/md files.
# --------------------------------------------------------------------

py_test_run_all_subdirectory(
    size = "medium",
    include = ["source/rllib/doc_code/*.py"],
    exclude = [],
    extra_srcs = [],
    tags = ["exclusive", "team:rllib"],
)

# --------------------------------------------------------------------
# Test all doc/source/ray-air/doc_code code included in rst/md files.
# --------------------------------------------------------------------

py_test_run_all_subdirectory(
    size = "large",
    include = ["source/ray-air/doc_code/*.py"],
    exclude = [
        # Too large. Use a custom test below.
        "source/ray-air/doc_code/computer_vision.py",
    ],
    extra_srcs = [],
    tags = ["exclusive", "team:ml"],
)

py_test(
    size = "enormous",
    name = "computer_vision_test",
    main = "source/ray-air/doc_code/computer_vision.py",
    srcs = ["source/ray-air/doc_code/computer_vision.py"],
    tags = ["exclusive", "team:ml"],
)

# --------------------------------------------------------------------
# Test all doc/source/train/doc_code code included in rst/md files.
# --------------------------------------------------------------------

py_test_run_all_subdirectory(
    size = "large",
    include = ["source/train/doc_code/*.py"],
    exclude = [
        "source/train/doc_code/hf_trainer.py",  # Too large
        "source/train/doc_code/accelerate_torch_trainer.py", # GPU test
        "source/train/doc_code/deepspeed_torch_trainer.py",  # GPU test
    ],
    extra_srcs = [],
    tags = ["exclusive", "team:ml"],
)

# --------------------------------------------------------------------
# Test all doc/source/ray-overview/doc_test snippets, used on ray.io
# --------------------------------------------------------------------

py_test_run_all_subdirectory(
    size = "small",
    include = ["source/ray-overview/doc_test/*.py"],
    exclude = ["source/ray-overview/doc_test/ray_data.py"],
    extra_srcs = [],
    tags = ["exclusive", "team:ml"],
)

# --------------------------------------------------------------------
# Test all doc/source/data/doc_code code included in rst/md files.
# --------------------------------------------------------------------

py_test_run_all_subdirectory(
    size = "large",
    include = ["source/data/doc_code/*.py"],
    exclude = [
        "source/data/doc_code/loading_data_untested.py",
    ],
    extra_srcs = [],
    tags = ["exclusive", "team:data"],
)

# --------------
# Run GPU tests
# --------------

py_test(
    name = "pytorch_resnet_finetune",
    size = "large",
    main = "test_myst_doc.py",
    srcs = ["test_myst_doc.py"],
    tags = ["exclusive", "team:ml", "gpu", "ray_air"],
    data = ["//doc/source/train/examples/pytorch:train_pytorch_examples"],
    args = ["--path", "doc/source/train/examples/pytorch/pytorch_resnet_finetune.ipynb"]
)

py_test_run_all_subdirectory(
    size = "large",
    include = [
        "source/train/doc_code/accelerate_torch_trainer.py", # GPU test
        "source/train/doc_code/deepspeed_torch_trainer.py",  # GPU test
    ],
    exclude = [],
    extra_srcs = [],
    tags = ["exclusive", "team:ml", "gpu"],
)

# --------------------------------------------------------------------
# Test all doc/external code
# --------------------------------------------------------------------

py_test_run_all_subdirectory(
    size = "large",
    include = ["external/*.py"],
    exclude = [],
    extra_srcs = [],
    tags = ["exclusive", "team:ml", "external"],
)


# --------------------------------------------------------------------
# Tests code snippets in user guides.
# --------------------------------------------------------------------

doctest(
    files = glob(
        include=["source/**/*.rst", "source/**/*.md"],
        exclude=[
            "source/ray-contribute/getting-involved.rst",
            "source/ray-core/handling-dependencies.rst",
            "source/ray-core/tasks/nested-tasks.rst",
            "source/ray-observability/user-guides/ray-tracing.rst",
            "source/ray-observability/user-guides/cli-sdk.rst",
            "source/rllib/rllib-env.rst",
            "source/rllib/rllib-sample-collection.rst",
            "source/serve/advanced-guides/inplace-updates.md",
            "source/serve/deploy-many-models/multi-app.md",
            "source/serve/production-guide/deploy-vm.md",
            "source/serve/production-guide/fault-tolerance.md",
            "source/data/batch_inference.rst",
            "source/data/transforming-data.rst",
            "source/train/faq.rst",
            "source/workflows/**/*.rst",
            "source/workflows/**/*.md",
            "source/templates/04_finetuning_llms_with_deepspeed/README.md"
        ]
    ),
    size = "large",
    tags = ["team:none"]
)


doctest(
    name="doctest[workflow]",
    files = glob(
        include=[
            "source/workflows/**/*.rst",
            "source/workflows/**/*.md"
        ]
    ),
    tags = ["team:core"]
)

doctest(
    files = [
        "source/data/batch_inference.rst",
        "source/data/transforming-data.rst"
    ],
    tags = ["team:data"],
    gpu = True
)
