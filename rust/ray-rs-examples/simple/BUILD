load("@rules_rust//rust:defs.bzl", "rust_library", "rust_shared_library", "rust_test")
load("@ray-rs-toml//:defs.bzl", "crate", "crates_from")

rust_shared_library(
    name = "simple_so",
    srcs = glob(["src/lib.rs"]),
    proc_macro_deps = [crate("paste")],
    deps = [
        "//rust/ray-rs:ray_rs",
    ],
    rustc_flags = ["-C", "link-args=-Wl,-export-dynamic", "-C", "link-args=-nostartfiles"],
    visibility = ["//visibility:public"],
)

rust_library(
    name = "simple",
    srcs = glob(["src/lib.rs"]),
    proc_macro_deps = [crate("paste")],
    deps = [
        "//rust/ray-rs:ray_rs",
    ],
    visibility = ["//visibility:public"],
)

rust_test(
    name = "test",
    srcs = ["tests/test.rs"],
    deps = [
      "//rust/ray-rs:ray_rs",
      ":simple",
    ],
    data = [":simple_so"],
    env = {
      "RUST_TEST_NOCAPTURE": "1",
      "RAY_RUST_LIBRARY_PATHS": "--ray_code_search_path=$(rootpath :simple_so)",
    },
)
