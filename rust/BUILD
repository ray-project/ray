load("@rules_cc//cc:defs.bzl", "cc_library")
load("@rules_rust//rust:defs.bzl", "rust_binary")
load("@cxx.rs//tools/bazel:rust_cxx_bridge.bzl", "rust_cxx_bridge")

rust_binary(
    name = "demo",
    srcs = glob(["src/**/*.rs"]),
    deps = [
        ":bridge",
        ":ray_rs_sys",
        "@cxx.rs//:cxx",
        "//:core_worker_lib",
        "//cpp:ray_api",
    ],
)

rust_cxx_bridge(
    name = "bridge",
    src = "src/main.rs",
    deps = [
      ":ray_rs_include",
      "//:core_worker_lib",
      "//cpp:ray_api",
    ],
)

cc_library(
    name = "ray_rs_sys",
    srcs = [],
    copts = ["-std=c++17"],
    deps = [
        ":bridge/include",
        "//:core_worker_lib",
        "//cpp:ray_api",
        "@cxx.rs//:core",
    ],
)

cc_library(
    name = "ray_rs_include",
    hdrs = ["include/wrapper.h"],
    deps = ["@cxx.rs//:core"],
)
