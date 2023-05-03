load("//bazel:python.bzl", "py_test_run_all_subdirectory")
load("//bazel:python.bzl", "py_test_run_all_notebooks")

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
    tags = ["exclusive", "team:ml", "py37"],
    data = ["//doc/source/ray-core/examples:core_examples"],
    args = ["--path", "doc/source/ray-core/examples/highly_parallel.ipynb"]
)

### Ray Data
py_test(
    name = "datasets_train",
    size = "medium",
    srcs = ["source/ray-core/_examples/datasets_train/datasets_train.py"],
    tags = ["exclusive", "team:ml", "py37", "datasets_train"],
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
    ],
    extra_srcs = [],
    tags = ["exclusive", "team:serve"],
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
        "source/ray-air/doc_code/hf_trainer.py",  # Too large
        "source/ray-air/doc_code/predictors.py",
    ],
    extra_srcs = [],
    tags = ["exclusive", "team:ml"],
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
    exclude = [
        "source/ray-air/doc_code/predictors.py",
        "source/data/doc_code/creating_datastreams_untested.py"
    ],
    extra_srcs = [],
    tags = ["exclusive", "team:data"],
)

# --------------------------------------------------------------------
# Test all Workspace template notebooks in doc/source/templates
# as smoke tests.
# --------------------------------------------------------------------

filegroup(
    name = "workspace_templates",
    srcs = glob([
        "source/templates/tests/**/*.ipynb",
        "source/templates/tests/**/requirements.txt"
    ]),
    visibility = ["//doc:__subpackages__"]
)

# Validate that all the paths and yamls within the templates.yaml file are valid.

py_test(
    name = "templates_directory_validation",
    size = "small",
    main = "source/templates/validate.py",
    srcs = ["source/templates/validate.py"],
    data = glob(["source/templates/**/*"]),
    tags = ["exclusive", "team:ml"]
)

# Templates that only require CPU

py_test_run_all_notebooks(
    size = "large",
    # TODO(justinvyu): Merge tests/ with the regular versions of the templates.
    include = ["source/templates/tests/02_many_model_training/many_model_training.ipynb"],
    exclude = [],
    data = ["//doc:workspace_templates"],
    tags = ["exclusive", "team:ml", "ray_air"],
    env = {"SMOKE_TEST": "1"},
)

# Templates that require GPU

py_test_run_all_notebooks(
    size = "large",
    include = [
        # TODO(justinvyu): Merge tests/ with the regular versions of the templates.
        "source/templates/tests/01_batch_inference/batch_inference.ipynb",
        "source/templates/tests/03_serving_stable_diffusion/serving_stable_diffusion.ipynb"
    ],
    exclude = [],
    data = ["//doc:workspace_templates"],
    tags = ["exclusive", "team:ml", "ray_air", "gpu"],
    env = {"SMOKE_TEST": "1"},
)

# --------------
# Run GPU tests
# --------------

py_test_run_all_subdirectory(
    size = "large",
    include = ["source/ray-air/doc_code/predictors.py"],
    exclude = [],
    extra_srcs = [],
    tags = ["exclusive", "team:ml", "ray_air", "gpu"],
)

py_test(
    name = "pytorch_resnet_finetune",
    size = "large",
    main = "test_myst_doc.py",
    srcs = ["test_myst_doc.py"],
    tags = ["exclusive", "team:ml", "gpu", "ray_air"],
    data = ["//doc/source/train/examples/pytorch:train_pytorch_examples"],
    args = ["--path", "doc/source/train/examples/pytorch/pytorch_resnet_finetune.ipynb"]
)
