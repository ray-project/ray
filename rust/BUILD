genrule(
    name = "ray_rust_pkg",
    srcs = [
        "//:libcore_worker_library_c.so",
        "//rust/ray-rs-worker:worker",
        "//:cp_c_worker_includes",
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
