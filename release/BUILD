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
