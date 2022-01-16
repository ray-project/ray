load("@rules_rust//proto:proto.bzl", "rust_proto_library")
load("//bazel:ray.bzl", "copy_to_workspace")
load("@rules_proto_grpc//rust:defs.bzl", "rust_proto_compile")
load("@rules_proto_grpc//rust:rust_proto_lib.bzl", "rust_proto_lib")

genrule(
    name = "ray_rust_pkg",
    srcs = [
        "//:libcore_worker_library_c.so",
        "//rust/ray-rs-worker:worker",
        "//:cp_c_worker_includes",
        "//:cp_rust_proto_common",
    ],
    outs = ["ray_rust_pkg.out"],
    local = 1,
    visibility = ["//visibility:public"],
    cmd = """
        WORK_DIR="$$(pwd)" &&
        PY_RUST_DIR="$$WORK_DIR/python/ray/rust" &&
        rm -rf "$$PY_RUST_DIR" &&
        rm -rf "$$PY_RUST_DIR/lib" &&
        mkdir -p "$$PY_RUST_DIR" &&
        mkdir -p "$$PY_RUST_DIR/lib" &&
        cp -f $(execpath //rust/ray-rs-worker:worker) "$$PY_RUST_DIR/" &&
        cp -f $(execpath //:libcore_worker_library_c.so) "$$PY_RUST_DIR/lib/" &&
        echo "$$WORK_DIR" > $@
    """,
)

rust_proto_compile(
    name = "rust_proto_common",
    protos = [
        "@//src/ray/protobuf:common_proto",
        "@//src/ray/protobuf:runtime_env_common_proto",
    ],
    visibility = ["//:__subpackages__"],
)

rust_proto_lib(
   name = "rust_proto_lib",
   compilation = "rust_proto_common",
   externs = ["protobuf"],
)

copy_to_workspace(
    name = "cp_rust_proto_common",
    srcs = [":rust_proto_common", ":rust_proto_lib"],
    dstdir = "rust/ray-rs-sys/generated/proto",
)
