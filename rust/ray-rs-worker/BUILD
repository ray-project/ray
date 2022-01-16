load("@rules_rust//rust:defs.bzl", "rust_binary")

rust_binary(
    name = "worker",
    srcs = glob(["src/**/*.rs"]),
    deps = [
        "//rust/ray-rs-sys:ray_rs_sys",
        "//rust/ray-rs:ray_rs",
    ],
    visibility = ["//rust:__subpackages__"],
)
