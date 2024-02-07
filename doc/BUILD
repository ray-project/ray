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

py_test(
    name = "doc_code_ray_oom_prevention",
    size = "medium",
    main = "source/ray-core/doc_code/ray_oom_prevention.py",
    srcs = ["source/ray-core/doc_code/ray_oom_prevention.py"],
    tags = ["exclusive", "mem_pressure", "team:core"]
)

py_test_run_all_subdirectory(
    size = "medium",
    include = ["source/ray-core/doc_code/*.py"],
    exclude = ["source/ray-core/doc_code/runtime_env_example.py",
               "source/ray-core/doc_code/cross_language.py",
               "source/ray-core/doc_code/ray_oom_prevention.py"],
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
        "source/serve/doc_code/aws_neuron_core_inference_serve.py",
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
    exclude = [
        "source/serve/doc_code/aws_neuron_core_inference_serve.py"
    ],
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
# Test all doc/source/train/doc_code code included in rst/md files.
# --------------------------------------------------------------------

py_test_run_all_subdirectory(
    size = "large",
    include = ["source/train/doc_code/*.py"],
    exclude = [],
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
    exclude = [],
    extra_srcs = [],
    tags = ["exclusive", "team:data"],
)

# --------------------------------------------------------------------
# Test all doc/source/ray-more-libs/doc_code code included in rst/md files.
# --------------------------------------------------------------------

py_test_run_all_subdirectory(
    size = "large",
    include = ["source/ray-more-libs/doc_code/*.py"],
    exclude = [],
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

# --------------------------------------------------------------------
# Test all doc/external code
# --------------------------------------------------------------------

py_test_run_all_subdirectory(
    size = "enormous",
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
            "source/serve/advanced-guides/inplace-updates.md",
            "source/serve/deploy-many-models/multi-app.md",
            "source/serve/production-guide/deploy-vm.md",
            "source/serve/production-guide/fault-tolerance.md",
            "source/data/batch_inference.rst",
            "source/data/transforming-data.rst",
            "source/train/**/*.rst",
            "source/train/**/*.md",
            "source/workflows/**/*.rst",
            "source/workflows/**/*.md",
            "source/rllib/**/*.rst",
            "source/rllib/**/*.md",
            "source/templates/04_finetuning_llms_with_deepspeed/README.md"
        ]
    ),
    size = "large",
    tags = ["team:none"]
)


doctest(
    name="doctest[train]",
    files = glob(
        include=[
            "source/train/**/*.rst",
            "source/train/**/*.md"
        ],
        exclude=[
            # GPU
            "source/train/user-guides/data-loading-preprocessing.rst",
            "source/train/user-guides/using-gpus.rst"
        ]
    ),
    tags = ["team:ml"]
)

doctest(
    name="doctest[train]",
    files = [
        "source/train/user-guides/data-loading-preprocessing.rst",
        "source/train/user-guides/using-gpus.rst"
    ],
    tags = ["team:ml"],
    gpu = True,
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
    name="doctest[rllib]",
    files = glob(
        include=[
            "source/rllib/**/*.rst",
            "source/rllib/**/*.md"
        ],
        exclude=[
            "source/rllib/rllib-env.rst",
            "source/rllib/rllib-sample-collection.rst",
        ]
    ),
    size = "enormous",
    tags = ["team:rllib"]
)

doctest(
    files = [
        "source/data/batch_inference.rst",
        "source/data/transforming-data.rst"
    ],
    tags = ["team:data"],
    gpu = True
)

