load("@rules_python//python:defs.bzl", "py_test")

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
    main = "air_tests/air_benchmarks/workloads/xgboost_benchmark.py",
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [
        "//:ray_lib",
        "//python/ray/air:ml_lib",
    ],
    args = ["--smoke-test"]
)

py_test(
    name = "air_benchmark_data_smoke_test",
    size = "small",
    srcs = test_srcs,
    main = "air_tests/air_benchmarks/workloads/data_benchmark.py",
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [
        "//:ray_lib",
        "//python/ray/air:ml_lib",
    ],
    args = ["--dataset-size-gb=1", "--num-workers=1"]
)

py_test(
     name = "air_benchmark_gpu_batch_prediction_smoke_test",
     size = "small",
     srcs = test_srcs,
     main = "air_tests/air_benchmarks/workloads/gpu_batch_prediction.py",
     tags = [
         "exclusive",
         "team:ml",
     ],
     deps = [
         "//:ray_lib",
         "//python/ray/air:ml_lib",
     ],
     args = ["--dataset-size-gb=1", "--smoke-test"]
)

py_test(
    name = "air_benchmark_pytorch_training_e2e_smoke_test",
    size = "small",
    src = test_srcs,
    main = "air_tests/air_benchmarks/workloads/pytorch_training_e2e.py",
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [
        "//:ray_lib",
        "//python/ray/air:ml_lib",
    ],
    args = ["--dataset-size-gb=1", "--smoke-test"]
)

py_test(
    name = "air_benchmark_tensorflow_smoke_test",
    size = "large",
    srcs = test_srcs,
    main = "air_tests/air_benchmarks/workloads/tensorflow_benchmark.py",
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [
        "//:ray_lib",
        "//python/ray/air:ml_lib",
    ],
    args = ["run", "--num-runs=1", "--num-epochs=1", "--num-workers=1", "--cpus-per-worker=1", "--batch-size=1", "--smoke-test", "--local"]
)

py_test(
    name = "air_benchmark_torch_smoke_test",
    size = "large",
    srcs = test_srcs,
    main = "air_tests/air_benchmarks/workloads/torch_benchmark.py",
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [
        "//:ray_lib",
        "//python/ray/air:ml_lib",
    ],
    args = ["run", "--num-runs=1", "--num-epochs=1", "--num-workers=1", "--cpus-per-worker=1", "--batch-size=1", "--smoke-test", "--local"]
)

py_test(
    name = "air_benchmark_tune_torch_smoke_test",
    # Keep this medium, as it will be slower if data has to be downloaded
    # (runtime is shorter when air_benchmark_torch_smoke_test is executed first)
    size = "medium",
    srcs = test_srcs,
    main = "air_tests/air_benchmarks/workloads/tune_torch_benchmark.py",
    tags = [
        "exclusive",
        "team:ml",
    ],
    deps = [
        "//:ray_lib",
        "//python/ray/air:ml_lib",
    ],
    args = ["--num-runs=1", "--num-trials=1", "--num-workers=1", "--smoke-test"]
)


####
# RELEASE TEST INFRA unit tests
####

py_test(
    name = "test_alerts",
    tags = ["team:ci", "release_unit"],
    size = "small",
    srcs = ["ray_release/tests/test_alerts.py"]
)

py_test(
    name = "test_buildkite",
    tags = ["team:ci", "release_unit"],
    size = "small",
    srcs = ["ray_release/tests/test_buildkite.py"]
)

py_test(
    name = "test_cluster_manager",
    tags = ["team:ci", "release_unit"],
    size = "small",
    srcs = ["ray_release/tests/test_cluster_manager.py"]
)

py_test(
    name = "test_config",
    tags = ["team:ci", "release_unit"],
    size = "small",
    srcs = ["ray_release/tests/test_config.py"],
    data = ["release_tests.yaml"],
)

py_test(
    name = "test_env",
    tags = ["team:ci", "release_unit"],
    size = "small",
    srcs = ["ray_release/tests/test_env.py"]
)

py_test(
    name = "test_glue",
    tags = ["team:ci", "release_unit"],
    size = "small",
    srcs = ["ray_release/tests/test_glue.py"]
)

py_test(
    name = "test_run_script",
    tags = ["team:ci", "release_unit"],
    size = "small",
    srcs = ["ray_release/tests/test_run_script.py"],
    data = [
        "run_release_test.sh",
        "ray_release/tests/_test_catch_args.py",
        "ray_release/tests/_test_run_release_test_sh.py",
    ],
)

py_test(
    name = "test_wheels",
    tags = ["team:ci", "release_unit"],
    size = "small",
    srcs = ["ray_release/tests/test_wheels.py"],
    deps = ["//:ray_lib"],
)
