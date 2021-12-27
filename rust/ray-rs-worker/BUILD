package(default_visibility = ["//visibility:public"])

load("@rules_cc//cc:defs.bzl", "cc_binary")

cc_binary(
    name = "rust_worker",
    srcs = glob(["worker.cc"]),
    deps = [
      "//cpp:ray_api",
      "//rust/ray-rs-sys:ray_rs_sys",
    ],
)
