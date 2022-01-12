package(default_visibility = ["//visibility:public"])

load("@rules_rust//rust:defs.bzl", "rust_library", "rust_test")

rust_library(
    name = "ray_rs_sys",
    srcs = glob(["src/**/*.rs"]),
    crate_features = ["bazel"],
    data = ["//:ray_rs_sys_bindgen.rs"],
    rustc_env = {
        "BAZEL_BINDGEN_SOURCE": "$(execpath //:ray_rs_sys_bindgen.rs)",
    },
)

rust_test(
    name = "test",
    crate = ":ray_rs_sys",
    rustc_flags = [
        "-C", "link-args=-L/home/jonch/.cache/bazel/_bazel_jonch/3819ce4da5bb63a59f7ecd1a722a08d9/execroot/com_github_ray_project_ray/bazel-out/k8-opt/bin -lcore_worker_library_c",
    ],
    crate_features = ["bazel"],
    data = ["//:ray_rs_sys_bindgen.rs"],
    rustc_env = {
        "BAZEL_BINDGEN_SOURCE": "$(execpath //:ray_rs_sys_bindgen.rs)",
    },
    env = {
        "RUST_TEST_NOCAPTURE": "1",
        "LD_LIBRARY_PATH": "/home/jonch/.cache/bazel/_bazel_jonch/3819ce4da5bb63a59f7ecd1a722a08d9/execroot/com_github_ray_project_ray/bazel-out/k8-opt/bin:LD_LIBRARY_PATH"
    }
)
