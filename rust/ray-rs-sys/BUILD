package(default_visibility = ["//visibility:public"])

load("@rules_cc//cc:defs.bzl", "cc_library")
load("@rules_rust//rust:defs.bzl", "rust_library", "rust_static_library","rust_shared_library", "rust_test")
load("@ray-rs-toml//:defs.bzl", "crates_from", "crate")
load("//bazel:ray.bzl", "COPTS")
load("//bazel:ray_cxx.bzl", "rust_cxx_bridge")

rust_test(
    name = "test",
    srcs = ["src/test.rs"],
    deps = [
      ":ray_rs_sys",
      "@cxx.rs//:cxx",
    ] + crates_from("//rust/ray-rs-sys:Cargo.toml"),
    data = [":ray_rs_sys_shared"],
    env = {
      "RUST_TEST_NOCAPTURE": "1",
      "RAY_RUST_LIBRARY_PATHS": "--ray_code_search_path=$(rootpath ray_rs_sys_shared)"
    },
)

rust_library(
    name = "ray_rs_sys",
    srcs = glob(["src/**/*.rs"]),
    proc_macro_deps = [crate("paste")],
    deps = [
        ":bridge",
        "@cxx.rs//:cxx",
        ":ray_rs_c",
    ] + crates_from("//rust/ray-rs-sys:Cargo.toml"),
    visibility = ["//visibility:public"],
)

rust_shared_library(
    name = "ray_rs_sys_shared",
    crate_name = "ray_rs_sys",
    srcs = glob(["src/**/*.rs"]),
    proc_macro_deps = [crate("paste")],
    deps = [
        ":bridge",
        "@cxx.rs//:cxx",
        ":ray_rs_c",
    ] + crates_from("//rust/ray-rs-sys:Cargo.toml"),
    # rustc_flags = ["-C", "link-args=-rdynamic"],
    rustc_flags = ["-C", "link-args=-Wl,-export-dynamic"],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "ray_rs_c",
    srcs = glob(["cpp/*.cc"]),
    copts = COPTS,
    deps = [
        ":bridge/include",
        "//:core_worker_lib",
        "//:global_state_accessor_lib",
        "@cxx.rs//:core",
        ":ray_rs_include",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/flags:parse",
        "@boost//:filesystem",
        "@boost//:optional",
    ],
)

rust_cxx_bridge(
    name = "bridge",
    src = "src/lib.rs",
    deps = [
      ":ray_rs_include",
      "//:core_worker_lib",
      "//:global_state_accessor_lib",
    ] + crates_from("//rust/ray-rs-sys:Cargo.toml"),
)

cc_library(
    name = "ray_rs_include",
    copts = COPTS,
    hdrs = glob(["cpp/*.h"]),
    deps = ["@cxx.rs//:core"],
)
