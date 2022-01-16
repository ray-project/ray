package(default_visibility = ["//visibility:public"])

load("@ray-rs-toml//:defs.bzl", "crates_from", "crate")
load("@rules_rust//rust:defs.bzl", "rust_library", "rust_test")

rust_library(
    name = "ray_rs_sys",
    srcs = glob(["src/**/*.rs"]),
    crate_features = ["bazel"],
    data = ["//:ray_rs_sys_bindgen.rs"],
    deps = [
      "//:core_worker_library_c_so",
      ":proto",
    ]
    + crates_from("//rust/ray-rs-sys:Cargo.toml"),
    rustc_env = {
        "BAZEL_BINDGEN_SOURCE": "$(execpath //:ray_rs_sys_bindgen.rs)",
    },
)

rust_test(
    name = "test",
    crate = ":ray_rs_sys",
    crate_features = ["bazel"],
    data = [
        "//:ray_rs_sys_bindgen.rs",
    ],
    rustc_env = {
        "BAZEL_BINDGEN_SOURCE": "$(execpath //:ray_rs_sys_bindgen.rs)",
    },
    env = {
        "RUST_TEST_NOCAPTURE": "1",
    }
)

rust_library(
    name = "proto",
    srcs = glob(["generated/proto/*.rs"]),
    deps = [crate("protobuf")],
)
