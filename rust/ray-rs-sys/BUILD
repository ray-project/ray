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
      # "RAY_RUST_LIBRARY_PATHS": "--ray_code_search_path=/home/jonch/.cache/bazel/_bazel_jonch/3819ce4da5bb63a59f7ecd1a722a08d9/execroot/com_github_ray_project_ray/bazel-out/k8-opt/bin/rust/ray-rs-sys/libray_rs_sys-1398298517.so"
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

    rustc_flags = ["-C", "link-args=-rdynamic"],
    # rustc_flags = ["-C", "link-args=-Wl,-export-dynamic"],
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
