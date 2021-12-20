load("@rules_rust//rust:defs.bzl", "rust_binary")
load("@ray-rs-toml//:defs.bzl", "crates_from")

rust_binary(
    name = "ray_rs",
    srcs = glob(["src/**/*.rs"]),
    deps = [
        "//rust/ray-rs-sys:ray_rs_sys",
    ] + crates_from("//rust/ray-rs:Cargo.toml"),
)
