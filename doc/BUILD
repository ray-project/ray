load("@py_deps_buildkite//:requirements.bzl", ci_require = "requirement")
load("@rules_python//python:defs.bzl", "py_test")
load("//bazel:python.bzl", "doctest", "py_test_run_all_notebooks", "py_test_run_all_subdirectory")

exports_files(["test_myst_doc.py"])

# --------------------------------------------------------------------
# Tests from the doc directory.
# Please keep these sorted alphabetically, but start with the
# root directory.
# --------------------------------------------------------------------

py_test(
    name = "highly_parallel",
    size = "medium",
    srcs = ["test_myst_doc.py"],
    args = [
        "--path",
        "doc/source/ray-core/examples/highly_parallel.ipynb",
    ],
    data = ["//doc/source/ray-core/examples:core_examples"],
    main = "test_myst_doc.py",
    tags = [
        "exclusive",
        "highly_parallel",
        "team:ml",
    ],
)

py_test(
    name = "plot_hyperparameter",
    size = "small",
    srcs = ["test_myst_doc.py"],
    args = [
        "--path",
        "doc/source/ray-core/examples/plot_hyperparameter.ipynb",
    ],
    data = ["//doc/source/ray-core/examples:core_examples"],
    main = "test_myst_doc.py",
    tags = [
        "exclusive",
        "team:ml",
    ],
)

py_test(
    name = "automl_for_time_series",
    size = "medium",
    srcs = ["test_myst_doc.py"],
    args = [
        "--path",
        "doc/source/ray-core/examples/automl_for_time_series.ipynb",
    ],
    data = ["//doc/source/ray-core/examples:core_examples"],
    main = "test_myst_doc.py",
    tags = [
        "exclusive",
        "team:ml",
        "timeseries_libs",
    ],
)

py_test(
    name = "batch_prediction",
    size = "medium",
    srcs = ["test_myst_doc.py"],
    args = [
        "--path",
        "doc/source/ray-core/examples/batch_prediction.ipynb",
    ],
    data = ["//doc/source/ray-core/examples:core_examples"],
    main = "test_myst_doc.py",
    tags = [
        "exclusive",
        "team:ml",
    ],
)

py_test(
    name = "plot_parameter_server",
    size = "medium",
    srcs = ["test_myst_doc.py"],
    args = [
        "--path",
        "doc/source/ray-core/examples/plot_parameter_server.ipynb",
    ],
    data = ["//doc/source/ray-core/examples:core_examples"],
    main = "test_myst_doc.py",
    tags = [
        "exclusive",
        "team:ml",
    ],
)

py_test(
    name = "plot_pong_example",
    size = "large",
    srcs = ["test_myst_doc.py"],
    args = [
        "--path",
        "doc/source/ray-core/examples/plot_pong_example.ipynb",
    ],
    data = ["//doc/source/ray-core/examples:core_examples"],
    main = "test_myst_doc.py",
    tags = [
        "exclusive",
        "team:ml",
    ],
)

# --------------------------------------------------------------------
# Test all doc/source/ray-observability/doc_code code included in rst/md files.
# --------------------------------------------------------------------

py_test_run_all_subdirectory(
    size = "medium",
    include = ["source/ray-observability/doc_code/*.py"],
    exclude = ["source/ray-observability/doc_code/ray-distributed-debugger.py"],
    extra_srcs = [],
    tags = [
        "exclusive",
        "team:core",
    ],
)

# --------------------------------------------------------------------
# Test all doc/source/ray-core/doc_code code included in rst/md files.
# --------------------------------------------------------------------

py_test(
    name = "doc_code_runtime_env_example",
    size = "small",
    srcs = ["source/ray-core/doc_code/runtime_env_example.py"],
    main = "source/ray-core/doc_code/runtime_env_example.py",
    tags = [
        "exclusive",
        "post_wheel_build",
        "team:serve",
    ],
)

py_test(
    name = "doc_code_ray_oom_prevention",
    size = "medium",
    srcs = ["source/ray-core/doc_code/ray_oom_prevention.py"],
    main = "source/ray-core/doc_code/ray_oom_prevention.py",
    tags = [
        "exclusive",
        "mem_pressure",
        "team:core",
    ],
)

py_test(
    name = "doc_code_cgraph_profiling",
    size = "small",
    srcs = ["source/ray-core/doc_code/cgraph_profiling.py"],
    main = "source/ray-core/doc_code/cgraph_profiling.py",
    tags = [
        "exclusive",
        "multi_gpu",
        "team:core",
    ],
)

py_test(
    name = "doc_code_cgraph_nccl",
    size = "small",
    srcs = ["source/ray-core/doc_code/cgraph_nccl.py"],
    main = "source/ray-core/doc_code/cgraph_nccl.py",
    tags = [
        "exclusive",
        "multi_gpu",
        "team:core",
    ],
)

py_test(
    name = "doc_code_cgraph_overlap",
    size = "small",
    srcs = ["source/ray-core/doc_code/cgraph_overlap.py"],
    main = "source/ray-core/doc_code/cgraph_overlap.py",
    tags = [
        "exclusive",
        "multi_gpu",
        "team:core",
    ],
)

py_test_run_all_subdirectory(
    size = "medium",
    include = ["source/ray-core/doc_code/*.py"],
    exclude = [
        "source/ray-core/doc_code/runtime_env_example.py",
        "source/ray-core/doc_code/cross_language.py",
        "source/ray-core/doc_code/ray_oom_prevention.py",
        "source/ray-core/doc_code/cgraph_profiling.py",
        "source/ray-core/doc_code/cgraph_nccl.py",
        "source/ray-core/doc_code/cgraph_overlap.py",
        # not testing this as it purposefully segfaults
        "source/ray-core/doc_code/cgraph_troubleshooting.py",
    ],
    extra_srcs = [],
    tags = [
        "exclusive",
        "team:core",
    ],
)

# --------------------------------------------------------------------
# Test all doc/source/serve/doc_code code included in rst/md files.
# --------------------------------------------------------------------

py_test_run_all_subdirectory(
    size = "medium",
    include = ["source/serve/doc_code/**/*.py"],
    exclude = [
        "source/serve/doc_code/aws_neuron_core_inference_serve.py",
        "source/serve/doc_code/aws_neuron_core_inference_serve_stable_diffusion.py",
        "source/serve/doc_code/intel_gaudi_inference_serve.py",
        "source/serve/doc_code/intel_gaudi_inference_serve_deepspeed.py",
        "source/serve/doc_code/intel_gaudi_inference_client.py",
        "source/serve/doc_code/distilbert.py",
        "source/serve/doc_code/stable_diffusion.py",
        "source/serve/doc_code/object_detection.py",
        "source/serve/doc_code/http_guide/streaming_example.py",
        "source/serve/doc_code/http_guide/websockets_example.py",
        "source/serve/doc_code/vllm_example.py",
    ],
    extra_srcs = [],
    tags = [
        "exclusive",
        "team:serve",
    ],
)

py_test_run_all_subdirectory(
    size = "medium",
    include = [
        "source/serve/doc_code/http_guide/streaming_example.py",
        "source/serve/doc_code/http_guide/websockets_example.py",
    ],
    env = {"RAY_SERVE_ENABLE_EXPERIMENTAL_STREAMING": "1"},
    exclude = [],
    extra_srcs = [],
    tags = [
        "exclusive",
        "team:serve",
    ],
)

py_test_run_all_subdirectory(
    size = "medium",
    include = [
        "source/serve/doc_code/distilbert.py",
        # TODO: Re-enable after fixing diffusers dependency in runtime_env
        # "source/serve/doc_code/stable_diffusion.py",
        "source/serve/doc_code/object_detection.py",
    ],
    env = {"RAY_SERVE_PROXY_READY_CHECK_TIMEOUT_S": "60"},
    exclude = [
        "source/serve/doc_code/aws_neuron_core_inference_serve.py",
        "source/serve/doc_code/aws_neuron_core_inference_serve_stable_diffusion.py",
        "source/serve/doc_code/intel_gaudi_inference_serve.py",
        "source/serve/doc_code/intel_gaudi_inference_serve_deepspeed.py",
        "source/serve/doc_code/intel_gaudi_inference_client.py",
    ],
    extra_srcs = [],
    tags = [
        "exclusive",
        "gpu",
        "team:serve",
    ],
)

# --------------------------------------------------------------------
# Test all doc/source/tune/doc_code code included in rst/md files.
# --------------------------------------------------------------------

py_test_run_all_subdirectory(
    size = "medium",
    include = ["source/tune/doc_code/*.py"],
    exclude = [],
    extra_srcs = [],
    tags = [
        "exclusive",
        "team:ml",
    ],
)

# --------------------------------------------------------------------
# Test all doc/source/rllib/doc_code code included in rst/md files.
# --------------------------------------------------------------------

py_test_run_all_subdirectory(
    size = "medium",
    include = ["source/rllib/doc_code/*.py"],
    exclude = [],
    extra_srcs = [],
    tags = [
        "exclusive",
        "team:rllib",
    ],
)

# --------------------------------------------------------------------
# Test all doc/source/train/doc_code code included in rst/md files.
# --------------------------------------------------------------------

py_test_run_all_subdirectory(
    size = "large",
    include = ["source/train/doc_code/*.py"],
    exclude = [
        "source/train/doc_code/hvd_trainer.py",  # CI do not have Horovod
    ],
    extra_srcs = [],
    tags = [
        "exclusive",
        "team:ml",
    ],
)

# --------------------------------------------------------------------
# Test all doc/source/data/doc_code code included in rst/md files.
# --------------------------------------------------------------------

py_test_run_all_subdirectory(
    size = "large",
    include = ["source/data/doc_code/*.py"],
    exclude = [],
    extra_srcs = [],
    tags = [
        "exclusive",
        "team:data",
    ],
)

# --------------------------------------------------------------------
# Test all doc/source/ray-more-libs/doc_code code included in rst/md files.
# --------------------------------------------------------------------

py_test_run_all_subdirectory(
    size = "large",
    include = ["source/ray-more-libs/doc_code/*.py"],
    exclude = [],
    extra_srcs = [],
    tags = [
        "exclusive",
        "team:data",
    ],
)

# --------------
# Run GPU tests
# --------------

py_test(
    name = "pytorch_resnet_finetune",
    size = "large",
    srcs = ["test_myst_doc.py"],
    args = [
        "--path",
        "doc/source/train/examples/pytorch/pytorch_resnet_finetune.ipynb",
    ],
    data = ["//doc/source/train/examples/pytorch:train_pytorch_examples"],
    main = "test_myst_doc.py",
    tags = [
        "exclusive",
        "gpu",
        "ray_air",
        "team:ml",
    ],
)

# --------------------------------------------------------------------
# Test all doc/external code
# --------------------------------------------------------------------

py_test_run_all_subdirectory(
    size = "enormous",
    include = ["external/*.py"],
    exclude = ["external/test_hashes.py"],
    extra_srcs = [],
    tags = [
        "exclusive",
        "external",
        "team:ml",
    ],
)

py_test(
    name = "test_external_hashes",
    srcs = ["external/test_hashes.py"],
    data = glob(
        ["external/*.py"],
        exclude = ["external/test_hashes.py"],
    ),
    exec_compatible_with = ["//:hermetic_python"],
    main = "external/test_hashes.py",
    tags = ["team:ml"],
    deps = [
        ci_require("pytest"),
        ci_require("bazel-runfiles"),
    ],
)

# --------------------------------------------------------------------
# Tests code snippets in user guides.
# --------------------------------------------------------------------

doctest(
    size = "large",
    files = glob(
        include = [
            "source/**/*.rst",
            "source/**/*.md",
        ],
        exclude = [
            "source/ray-contribute/getting-involved.rst",
            "source/ray-contribute/testing-tips.rst",
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
            "source/templates/04_finetuning_llms_with_deepspeed/README.md",
        ],
    ),
    tags = ["team:none"],
)

doctest(
    name = "doctest[train]",
    files = glob(
        include = [
            "source/train/**/*.rst",
            "source/train/**/*.md",
        ],
        exclude = [
            # GPU
            "source/train/user-guides/data-loading-preprocessing.rst",
            "source/train/user-guides/using-gpus.rst",
            "source/train/horovod.rst",  # CI do not have Horovod
        ],
    ),
    tags = ["team:ml"],
)

doctest(
    name = "doctest[train]",
    files = [
        "source/train/user-guides/data-loading-preprocessing.rst",
        "source/train/user-guides/using-gpus.rst",
    ],
    gpu = True,
    tags = ["team:ml"],
)

doctest(
    name = "doctest[workflow]",
    files = glob(
        include = [
            "source/workflows/**/*.rst",
            "source/workflows/**/*.md",
        ],
    ),
    tags = ["team:core"],
)

doctest(
    name = "doctest[rllib]",
    size = "large",
    data = ["//rllib:cartpole-v1_large"],
    files = glob(
        include = [
            "source/rllib/**/*.rst",
            "source/rllib/**/*.md",
        ],
        exclude = [
            "source/rllib/getting-started.rst",
        ],
    ),
    tags = ["team:rllib"],
)

doctest(
    name = "doctest[rllib2]",
    size = "large",
    files = glob(
        include = [
            "source/rllib/getting-started.rst",
        ],
    ),
    tags = ["team:rllib"],
)

doctest(
    files = [
        "source/data/batch_inference.rst",
        "source/data/transforming-data.rst",
    ],
    gpu = True,
    tags = ["team:data"],
)
