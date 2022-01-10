genrule(
    name = "ray_rust_pkg",
    srcs = [
        # "//rust/ray-rs-worker:rust_worker",
        # "//rust/ray-rs-sys:ray_rs_sys_shared",
        # "//rust/ray-rs-sys:ray_rs_c",
    ],
    outs = ["ray_rust_pkg.out"],
    local = 1,
    visibility = ["//visibility:public"],
    cmd = """
        WORK_DIR="$$(pwd)" &&
        PY_RUST_DIR="$$WORK_DIR/python/ray/rust" &&
        mkdir -p "$$PY_RUST_DIR" &&
        mkdir -p "$$PY_RUST_DIR/lib" &&
        echo "$$WORK_DIR" > $@
    """,
)
# cp -f $(execpath //rust/ray-rs-sys:ray_rs_c) "$$PY_RUST_DIR/lib/" &&
# cp -f $(execpath //rust/ray-rs-sys:ray_rs_sys_shared) "$$PY_RUST_DIR/lib/" &&

# cp -f $(execpath //rust/ray-rs-worker:rust_worker) "$$PY_RUST_DIR/" &&
# cp -f $(execpath //rust/ray-rs-sys:ray_rs_sys_shared) "$$PY_RUST_DIR/lib/" &&
