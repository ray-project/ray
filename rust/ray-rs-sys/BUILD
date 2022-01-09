package(default_visibility = ["//visibility:public"])

load("@rules_rust//rust:defs.bzl", "rust_library", "rust_static_library","rust_shared_library", "rust_test")
load("@ray-rs-toml//:defs.bzl", "crates_from", "crate")
load("//bazel:ray.bzl", "COPTS")
load("//bazel:ray_cxx.bzl", "rust_cxx_bridge")

rust_test(
    name = "test",
    srcs = ["tests/test.rs"],
    deps = [
      ":ray_rs_sys",
      "@cxx.rs//:cxx",
      ":remote_rlib",
    ] + crates_from("//rust/ray-rs-sys:Cargo.toml"),
    data = [":remote_lib"],
    env = {
      "RUST_TEST_NOCAPTURE": "1",
      "RAY_RUST_LIBRARY_PATHS": "--ray_code_search_path=$(rootpath remote_lib)"
    },
)

rust_library(
    name = "ray_rs_sys",
    srcs = glob(["src/**/*.rs"]),
    proc_macro_deps = [crate("paste")],
    deps = [
        ":bridge",
        "@cxx.rs//:cxx",
        ":ray_rs_bridge_c",
        "ray_rs_c",
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
        # ":ray_rs_bridge_c",
        # "ray_rs_c",
    ] + crates_from("//rust/ray-rs-sys:Cargo.toml"),
    visibility = ["//visibility:public"],
)

rust_library(
    name = "remote_rlib",
    crate_name = "remote_lib",
    srcs = glob(["tests/remote_lib.rs"]),
    deps = [
        "ray_rs_sys",
    ],
    visibility = ["//visibility:public"],
)

rust_shared_library(
    name = "remote_lib",
    srcs = glob(["tests/remote_lib.rs"]),
    deps = [
        "ray_rs_sys",
    ],
    # rustc_flags = ["-C", "link-args=-rdynamic"],
    rustc_flags = ["-C", "link-args=-Wl,-export-dynamic"],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "ray_rs_c",
    srcs = glob(["cpp/standalone/*.cc"]) + glob(["cpp/standalone/*.h"]),
    hdrs = ["cpp/standalone/api.h"],
    copts = COPTS,
    # linkopts = ["-shared"],
    # linkstatic = True,
    strip_include_prefix = "cpp/standalone",
    deps = [
        "//:core_worker_lib",
        "//:global_state_accessor_lib",
        "//:ray_common",
        "//:ray_util",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/flags:parse",
        "@boost//:filesystem",
        "@boost//:optional",
        "@cxx.rs//:core",
    ],
)

cc_library(
    name = "ray_rs_bridge_c",
    srcs = glob(["cpp/bridge/*.cc"]),
    copts = COPTS,
    # linkstatic = True,
    deps = [
        ":bridge/include",
        ":bridge",
        "//:core_worker_lib",
        "@cxx.rs//:core",
        ":ray_rs_include_bridge",
        "ray_rs_c",
    ],
)

rust_cxx_bridge(
    name = "bridge",
    src = "src/lib.rs",
    deps = [
      "ray_rs_c",
      ":ray_rs_include_bridge",
      "//:core_worker_lib",
    ] + crates_from("//rust/ray-rs-sys:Cargo.toml"),
)

cc_library(
    name = "ray_rs_include_bridge",
    copts = COPTS,
    hdrs = glob(["cpp/bridge/*.h"]),
    deps = ["@cxx.rs//:core"],
)
