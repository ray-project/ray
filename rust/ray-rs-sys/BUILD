package(default_visibility = ["//visibility:public"])

load("@rules_cc//cc:defs.bzl", "cc_library")
load("@rules_rust//rust:defs.bzl", "rust_library", "rust_static_library","rust_shared_library", "rust_test")
load("@cxx.rs//tools/bazel:rust_cxx_bridge.bzl", "rust_cxx_bridge")
load("@ray-rs-toml//:defs.bzl", "crates_from", "crate")

rust_test(
    name = "test",
    srcs = ["src/test.rs"],
    deps = [
      ":ray_rs_sys",
      ":ray_rs_sys_shared",
      "@cxx.rs//:cxx",
    ] + crates_from("//rust/ray-rs-sys:Cargo.toml"),
    env = {
      "RUST_TEST_NOCAPTURE": "1",
      "RAY_RUST_LIBRARY_PATHS": "--ray_code_search_path=$(location ray_rs_sys_shared)"
    },
    # args = [
    #     "--ray_code_search_path=$(location ray_rs_sys_shared)",
    #     "--ray_head_args '--include-dashboard true'",
    # ],
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

# This needs to be statically linked
# So that the "ray_rs_c_linked" library
# has access to the rust methods it needs
rust_shared_library(
    name = "ray_rs_sys_shared",
    crate_name = "ray_rs_sys",
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

# This needs to be statically linked
# So that the "ray_rs_c_linked" library
# has access to the rust methods it needs
rust_static_library(
    name = "ray_rs_sys_static",
    crate_name = "ray_rs_sys",
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
