load("@rules_python//python:defs.bzl", "py_test")

test_srcs = glob(["**/*.py"])

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
    srcs = ["ray_release/tests/test_config.py"]
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
    srcs = ["ray_release/tests/test_run_script.py"]
)

py_test(
    name = "test_wheels",
    tags = ["team:ci", "release_unit"],
    size = "small",
    srcs = ["ray_release/tests/test_wheels.py"]
)
