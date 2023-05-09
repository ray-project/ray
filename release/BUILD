load("@rules_python//python:defs.bzl", "py_library", "py_runtime", "py_runtime_pair", "py_test")
load("@rules_python//python:pip.bzl", "compile_pip_requirements")
load("@py_deps_buildkite//:requirements.bzl", bk_require = "requirement")
load("@python3_9//:defs.bzl", bk_python = "interpreter")

compile_pip_requirements(
    name = "requirements_buildkite",
    requirements_in = "requirements_buildkite.in",
    requirements_txt = "requirements_buildkite.txt",
    tags = [
        "team:ci",
    ],
    visibility = ["//visibility:private"],
)

test_srcs = glob(["**/*.py"])

####
# SERVE smoke tests
####

py_test(
    name = "serve_failure_smoke_test",
    size = "medium",
    srcs = test_srcs,
    env = {
        "RAY_UNIT_TEST": "1",
    },
    main = "serve_failure.py",
    tags = [
        "exclusive",
        "team:serve",
    ],
    deps = [
        "//:ray_lib",
        "//python/ray/serve:serve_lib",
    ],
)

py_test(
    name = "single_deployment_1k_noop_replica_smoke_test",
    size = "medium",
    srcs = test_srcs,
    env = {
        "IS_SMOKE_TEST": "1",
    },
    main = "single_deployment_1k_noop_replica.py",
    tags = [
        "exclusive",
        "team:serve",
    ],
    deps = [
        "//:ray_lib",
        "//python/ray/serve:serve_lib",
    ],
)

py_test(
    name = "multi_deployment_1k_noop_replica_smoke_test",
    size = "medium",
    srcs = test_srcs,
    env = {
        "IS_SMOKE_TEST": "1",
    },
    main = "multi_deployment_1k_noop_replica.py",
    tags = [
        "exclusive",
        "team:serve",
    ],
    deps = [
        "//:ray_lib",
        "//python/ray/serve:serve_lib",
    ],
)

py_test(
    name = "deployment_graph_long_chain",
    size = "medium",
    srcs = test_srcs,
    env = {
        "IS_SMOKE_TEST": "1",
    },
    main = "deployment_graph_long_chain.py",
    tags = [
        "exclusive",
        "team:serve",
    ],
    deps = [
        "//:ray_lib",
        "//python/ray/serve:serve_lib",
    ],
)

py_test(
    name = "deployment_graph_wide_ensemble",
    size = "medium",
    srcs = test_srcs,
    env = {
        "IS_SMOKE_TEST": "1",
    },
    main = "deployment_graph_wide_ensemble.py",
    tags = [
        "exclusive",
        "team:serve",
    ],
    deps = [
        "//:ray_lib",
        "//python/ray/serve:serve_lib",
    ],
)

py_test(
    name = "serve_handle_long_chain",
    size = "medium",
    srcs = test_srcs,
    env = {
        "IS_SMOKE_TEST": "1",
    },
    main = "serve_handle_long_chain.py",
    tags = [
        "exclusive",
        "team:serve",
    ],
    deps = [
        "//:ray_lib",
        "//python/ray/serve:serve_lib",
    ],
)

py_test(
    name = "serve_handle_wide_ensemble",
    size = "medium",
    srcs = test_srcs,
    env = {
        "IS_SMOKE_TEST": "1",
    },
    main = "serve_handle_wide_ensemble.py",
    tags = [
        "exclusive",
        "team:serve",
    ],
    deps = [
        "//:ray_lib",
        "//python/ray/serve:serve_lib",
    ],
)

py_test(
    name = "autoscaling_single_deployment_smoke_test",
    size = "medium",
    srcs = test_srcs,
    env = {
        "IS_SMOKE_TEST": "1",
    },
    main = "autoscaling_single_deployment.py",
    tags = [
        "exclusive",
        "team:serve",
    ],
    deps = [
        "//:ray_lib",
        "//python/ray/serve:serve_lib",
    ],
)

py_test(
    name = "autoscaling_multi_deployment_smoke_test",
    size = "medium",
    srcs = test_srcs,
    env = {
        "IS_SMOKE_TEST": "1",
    },
    main = "autoscaling_multi_deployment.py",
    tags = [
        "exclusive",
        "team:serve",
    ],
    deps = [
        "//:ray_lib",
        "//python/ray/serve:serve_lib",
    ],
)

py_test(
    name = "tune_serve_golden_notebook_client_smoke_test",
    size = "medium",
    srcs = test_srcs,
    env = {
        "IS_SMOKE_TEST": "1",
    },
    main = "golden_notebook_tests/workloads/torch_tune_serve_test.py",
    tags = [
        "exclusive",
        "team:serve",
    ],
    deps = [
        "//:ray_lib",
        "//python/ray/serve:serve_lib",
    ],
)

####
# AIR smoke tests
####

py_test(
    name = "air_benchmark_xgboost_smoke_test",
    size = "small",
    srcs = test_srcs,
    args = ["--smoke-test"],
    main = "air_tests/air_benchmarks/workloads/xgboost_benchmark.py",
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [
        "//:ray_lib",
        "//python/ray/air:ml_lib",
    ],
)

py_test(
    name = "air_benchmark_data_smoke_test",
    size = "small",
    srcs = test_srcs,
    args = [
        "--dataset-size-gb=1",
        "--num-workers=1",
    ],
    main = "air_tests/air_benchmarks/workloads/data_benchmark.py",
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [
        "//:ray_lib",
        "//python/ray/air:ml_lib",
    ],
)

py_test(
    name = "air_benchmark_gpu_batch_prediction_smoke_test",
    size = "small",
    srcs = test_srcs,
    args = [
        "--data-size-gb=1",
        "--smoke-test",
    ],
    main = "air_tests/air_benchmarks/workloads/gpu_batch_prediction.py",
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [
        "//:ray_lib",
        "//python/ray/air:ml_lib",
    ],
)

py_test(
    name = "air_benchmark_pytorch_training_e2e_smoke_test",
    size = "small",
    srcs = test_srcs,
    args = [
        "--data-size-gb=1",
        "--smoke-test",
    ],
    main = "air_tests/air_benchmarks/workloads/pytorch_training_e2e.py",
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [
        "//:ray_lib",
        "//python/ray/air:ml_lib",
    ],
)

py_test(
    name = "air_benchmark_tensorflow_smoke_test",
    size = "large",
    srcs = test_srcs,
    args = [
        "run",
        "--num-runs=1",
        "--num-epochs=1",
        "--num-workers=1",
        "--cpus-per-worker=1",
        "--batch-size=1",
        "--smoke-test",
        "--local",
    ],
    main = "air_tests/air_benchmarks/workloads/tensorflow_benchmark.py",
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [
        "//:ray_lib",
        "//python/ray/air:ml_lib",
    ],
)

py_test(
    name = "air_benchmark_torch_smoke_test",
    size = "large",
    srcs = test_srcs,
    args = [
        "run",
        "--num-runs=1",
        "--num-epochs=1",
        "--num-workers=1",
        "--cpus-per-worker=1",
        "--batch-size=1",
        "--smoke-test",
        "--local",
    ],
    main = "air_tests/air_benchmarks/workloads/torch_benchmark.py",
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [
        "//:ray_lib",
        "//python/ray/air:ml_lib",
    ],
)

py_test(
    name = "air_benchmark_tune_torch_smoke_test",
    # Keep this medium, as it will be slower if data has to be downloaded
    # (runtime is shorter when air_benchmark_torch_smoke_test is executed first)
    size = "medium",
    srcs = test_srcs,
    args = [
        "--num-runs=1",
        "--num-trials=1",
        "--num-workers=1",
        "--smoke-test",
    ],
    main = "air_tests/air_benchmarks/workloads/tune_torch_benchmark.py",
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [
        "//:ray_lib",
        "//python/ray/air:ml_lib",
    ],
)

####
# RELEASE TEST INFRA unit tests
####

py_runtime(
    name = "python3_runtime",
    interpreter = bk_python,
    python_version = "PY3",
    visibility = ["//visibility:private"],
)

py_runtime_pair(
    name = "python_runtime_pair",
    py2_runtime = None,
    py3_runtime = ":python3_runtime",
    visibility = ["//visibility:private"],
)

constraint_setting(name = "hermetic")

constraint_value(
    name = "hermetic_python",
    constraint_setting = ":hermetic",
    visibility = ["//visibility:private"],
)

toolchain(
    name = "python_toolchain",
    exec_compatible_with = [":hermetic_python"],
    toolchain = ":python_runtime_pair",
    toolchain_type = "@bazel_tools//tools/python:toolchain_type",
)

platform(
    name = "hermetic_python_platform",
    constraint_values = [":hermetic_python"],
    parents = ["@local_config_platform//:host"],
    visibility = ["//visibility:private"],
)

py_library(
    name = "ray_release",
    srcs = glob(
        ["ray_release/**/*.py"],
        exclude = ["ray_release/tests/*.py"],
    ),
    data = glob(["ray_release/environments/*.env"]) + [
        "ray_release/buildkite/aws_instance_types.csv",
        "ray_release/schema.json",
    ],
    imports = ["."],
    visibility = ["//visibility:public"],
    deps = [
        bk_require("anyscale"),
        bk_require("bazel-runfiles"),
        bk_require("boto3"),
        bk_require("botocore"),
        bk_require("click"),
        bk_require("google-cloud-storage"),
        bk_require("jinja2"),
        bk_require("retry"),
    ],
)

py_library(
    name = "test_utils",
    srcs = ["ray_release/tests/utils.py"],
    imports = ["."],
    visibility = ["//visibility:private"],
)

py_test(
    name = "test_alerts",
    size = "small",
    srcs = ["ray_release/tests/test_alerts.py"],
    exec_compatible_with = [":hermetic_python"],
    tags = [
        "release_unit",
        "team:ci",
    ],
    deps = [
        ":ray_release",
        bk_require("pytest"),
    ],
)

py_test(
    name = "test_anyscale_job_wrapper",
    size = "small",
    srcs = ["ray_release/tests/test_anyscale_job_wrapper.py"],
    exec_compatible_with = [":hermetic_python"],
    tags = [
        "release_unit",
        "team:ci",
    ],
    deps = [
        ":ray_release",
        bk_require("pytest"),
    ],
)

py_test(
    name = "test_bisect",
    size = "small",
    srcs = ["ray_release/tests/test_bisect.py"],
    exec_compatible_with = [":hermetic_python"],
    tags = [
        "release_unit",
        "team:ci",
    ],
    deps = [
        ":ray_release",
        bk_require("pytest"),
    ],
)

py_test(
    name = "test_buildkite",
    size = "small",
    srcs = ["ray_release/tests/test_buildkite.py"],
    exec_compatible_with = [":hermetic_python"],
    tags = [
        "release_unit",
        "team:ci",
    ],
    deps = [
        ":ray_release",
        bk_require("pyyaml"),
        bk_require("pytest"),
    ],
)

py_test(
    name = "test_cluster_manager",
    size = "small",
    srcs = ["ray_release/tests/test_cluster_manager.py"],
    exec_compatible_with = [":hermetic_python"],
    tags = [
        "release_unit",
        "team:ci",
    ],
    deps = [
        ":ray_release",
        ":test_utils",
        bk_require("freezegun"),
        bk_require("pytest"),
    ],
)

py_test(
    name = "test_config",
    size = "small",
    srcs = ["ray_release/tests/test_config.py"],
    data = glob(
        ["**/*.yaml"],
        exclude = ["ray_release/**/*.yaml"],
    ) + [
        "//python/ray/autoscaler/aws:test_configs",
    ],
    exec_compatible_with = [":hermetic_python"],
    tags = [
        "release_unit",
        "team:ci",
    ],
    deps = [
        ":ray_release",
        bk_require("pytest"),
    ],
)

py_test(
    name = "test_env",
    size = "small",
    srcs = ["ray_release/tests/test_env.py"],
    exec_compatible_with = [":hermetic_python"],
    tags = [
        "release_unit",
        "team:ci",
    ],
    deps = [
        ":ray_release",
        bk_require("pytest"),
    ],
)

py_test(
    name = "test_glue",
    size = "small",
    srcs = ["ray_release/tests/test_glue.py"],
    exec_compatible_with = [":hermetic_python"],
    tags = [
        "release_unit",
        "team:ci",
    ],
    deps = [
        ":ray_release",
        ":test_utils",
        bk_require("pytest"),
    ],
)

py_test(
    name = "test_log_aggregator",
    size = "small",
    srcs = ["ray_release/tests/test_log_aggregator.py"],
    exec_compatible_with = [":hermetic_python"],
    tags = [
        "release_unit",
        "team:ci",
    ],
    deps = [
        ":ray_release",
        bk_require("pytest"),
    ],
)

py_test(
    name = "test_run_script",
    size = "small",
    srcs = ["ray_release/tests/test_run_script.py"],
    data = [
        "ray_release/tests/_test_catch_args.py",
        "ray_release/tests/_test_run_release_test_sh.py",
        "run_release_test.sh",
    ],
    exec_compatible_with = [":hermetic_python"],
    tags = [
        "release_unit",
        "team:ci",
    ],
    deps = [
        ":ray_release",
        bk_require("pytest"),
    ],
)

py_test(
    name = "test_wheels",
    size = "small",
    srcs = ["ray_release/tests/test_wheels.py"],
    data = [
        "//:python_sources",
    ],
    exec_compatible_with = [":hermetic_python"],
    tags = [
        "release_unit",
        "team:ci",
    ],
    deps = [
        ":ray_release",
        bk_require("freezegun"),
        bk_require("pytest"),
    ],
)
