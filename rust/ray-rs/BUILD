load("@rules_rust//rust:defs.bzl", "rust_library", "rust_shared_library", "rust_test")
load("@ray-rs-toml//:defs.bzl", "crates_from", "crate")

rust_library(
    name = "ray_rs",
    srcs = glob(["src/**/*.rs"]),
    proc_macro_deps = [crate("paste")],
    deps = [
        "//rust/ray-rs-sys:ray_rs_sys",
    ] + crates_from("//rust/ray-rs:Cargo.toml"),
    rustc_flags = ["-C", "link-args=-L/home/jonch/Desktop/Programming/systems/ray/python/ray/rust/lib -lcore_worker_library_c"],
    visibility = ["//visibility:public"],
)


rust_test(
    name = "test",
    srcs = ["tests/test.rs"],
    deps = [
      ":ray_rs",
      ":remote_rlib",
    ] + crates_from("//rust/ray-rs:Cargo.toml"),
    data = [":remote_lib"],
    rustc_flags = ["-C", "link-args=-L/home/jonch/Desktop/Programming/systems/ray/python/ray/rust/lib -lcore_worker_library_c"],
    env = {
      "RUST_TEST_NOCAPTURE": "1",
      "RAY_RUST_LIBRARY_PATHS": "--ray_code_search_path=$(rootpath remote_lib)",
      "LD_LIBRARY_PATH": "/home/jonch/Desktop/Programming/systems/ray/python/ray/rust/lib",
    },
)

rust_shared_library(
    name = "remote_lib",
    srcs = glob(["tests/remote_lib.rs"]),
    proc_macro_deps = [crate("paste")],
    deps = [
        "ray_rs",
    ],
    rustc_flags = ["-C", "link-args=-Wl,-export-dynamic", "-C", "link-args=-L/home/jonch/Desktop/Programming/systems/ray/python/ray/rust/lib -lcore_worker_library_c -nostartfiles"],
    visibility = ["//visibility:public"],
)


rust_library(
    name = "remote_rlib",
    crate_name = "remote_lib",
    srcs = glob(["tests/remote_lib.rs"]),
    proc_macro_deps = [crate("paste")],
    deps = [
        "ray_rs",
    ],
    rustc_flags = ["-C", "link-args=-L/home/jonch/Desktop/Programming/systems/ray/python/ray/rust/lib -lcore_worker_library_c"],
    visibility = ["//visibility:public"],
)
