load("@rules_rust//rust:defs.bzl", "rust_binary")


# run as `LD_LIBRARY_PATH=/home/jonch/.cache/bazel/_bazel_jonch/3819ce4da5bb63a59f7ecd1a722a08d9/execroot/com_github_ray_project_ray/bazel-out/k8-opt/bin:LD_LIBRARY_PATH bazel run worker`
rust_binary(
    name = "worker",
    srcs = glob(["src/**/*.rs"]),
    deps = [
        "//rust/ray-rs-sys:ray_rs_sys",
        "//rust/ray-rs:ray_rs",
    ],
    rustc_flags = [
        "-C", "link-args=-L/home/jonch/Desktop/Programming/systems/ray/python/ray/rust/lib -lcore_worker_library_c",
    ],
    visibility = ["//rust:__subpackages__"],
)

# TODO (jon-chuang): Try to build a rule that sets
# RUSTFLAGS="-C link-args=-L"
# genrule(
#     name = "logo_miniature",
#     data = ["//:libcore_worker_library_c.so"],
#     cmd = "echo \"RUSTFLAGS=\\\"-C link-args=-L$(execpath //:libcore_worker_library_c.so)",
# )
