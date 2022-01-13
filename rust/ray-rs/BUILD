load("@rules_rust//rust:defs.bzl", "rust_library", "rust_test")
load("@ray-rs-toml//:defs.bzl", "crates_from", "crate")

rust_library(
    name = "ray_rs",
    srcs = glob(["src/**/*.rs"]),
    proc_macro_deps = [crate("paste")],
    deps = [
        "//rust/ray-rs-sys:ray_rs_sys",
    ] + crates_from("//rust/ray-rs:Cargo.toml"),
    visibility = ["//visibility:public"],
)
