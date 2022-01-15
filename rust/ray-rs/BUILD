load("@rules_rust//rust:defs.bzl", "rust_library", "rust_test")
load("@ray-rs-toml//:defs.bzl", "crates_from", "crate")
load("@bazel_skylib//lib:new_sets.bzl", "sets")

rust_library(
    name = "ray_rs",
    srcs = glob(["src/**/*.rs"]),
    proc_macro_deps = [
        crate("paste"),
        crate("ctor"),
    ],
    deps = [
        "//rust/ray-rs-sys:ray_rs_sys",
    ] +
    sets.to_list(sets.difference(
        sets.make(crates_from("//rust/ray-rs:Cargo.toml")),
        sets.make([crate("ray-rs-sys")]),
    )),
    visibility = ["//visibility:public"],
)
