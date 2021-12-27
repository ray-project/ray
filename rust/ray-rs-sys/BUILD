package(default_visibility = ["//visibility:public"])

load("@rules_cc//cc:defs.bzl", "cc_library")
load("@rules_rust//rust:defs.bzl", "rust_library", "rust_static_library", "rust_test")
load("@cxx.rs//tools/bazel:rust_cxx_bridge.bzl", "rust_cxx_bridge")
load("@ray-rs-toml//:defs.bzl", "crates_from", "crate")

rust_test(
    name = "test",
    srcs = ["src/test.rs"],
    deps = [
      ":ray_rs_sys",
      "@cxx.rs//:cxx",
    ] + crates_from("//rust/ray-rs-sys:Cargo.toml"),
    env = {"RUST_TEST_NOCAPTURE": "1"},
)


rust_library(
    name = "ray_rs_sys",
    srcs = glob(["src/**/*.rs"]),
    proc_macro_deps = [crate("paste")],
    deps = [
        ":bridge",
        "@cxx.rs//:cxx",
        "//:core_worker_lib",
        "//cpp:ray_api",
        ":ray_rs_c",
    ] + crates_from("//rust/ray-rs-sys:Cargo.toml"),
    visibility = ["//visibility:public"],
)

cc_library(
    name = "ray_rs_c",
    srcs = [
      "cpp/tasks.cc",
      "cpp/wrapper.cc",
    ],
    deps = [
        ":bridge/include",
        "//:core_worker_lib",
        "//cpp:ray_api",
        "@cxx.rs//:core",
        ":ray_rs_include",
    ],
)

cc_binary(
    name = "rust_worker",
    srcs = glob(["cpp/worker.cc"]),
    deps = [
      "//cpp:ray_api",
      ":ray_rs_c_linked",
    ],
    linkopts = ["-fuse-ld=lld"],
)

cc_library(
    name = "ray_rs_c_linked",
    srcs = [
      "cpp/tasks.cc",
    ],
    deps = [
        ":bridge/include",
        ":ray_rs_include",
        ":ray_rs_sys_static",
        "//:core_worker_lib",
        "//cpp:ray_api",
        "@cxx.rs//:core",
    ],
    hdrs = [
      "cpp/wrapper.h",
      "cpp/tasks.h",
    ],
)

rust_static_library(
    name = "ray_rs_sys_static",
    srcs = glob(["src/**/*.rs"]),
    proc_macro_deps = [crate("paste")],
    deps = [
        ":bridge",
        "@cxx.rs//:cxx",
        "//:core_worker_lib",
        "//cpp:ray_api",
        ":ray_rs_c",
    ] + crates_from("//rust/ray-rs-sys:Cargo.toml"),
    visibility = ["//visibility:public"],
)

rust_cxx_bridge(
    name = "bridge",
    src = "src/lib.rs",
    deps = [
      ":ray_rs_include",
      "//:core_worker_lib",
      "//cpp:ray_api",
    ] + crates_from("//rust/ray-rs-sys:Cargo.toml"),
)

cc_library(
    name = "ray_rs_include",
    hdrs = [
      "cpp/wrapper.h",
      "cpp/tasks.h",
    ],
    deps = ["@cxx.rs//:core"],
)
