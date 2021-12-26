genrule(
    name = "ray_rust_pkg",
    srcs = [
        "//rust/ray-rs-worker:rust_worker",
    ],
    outs = ["ray_rust_pkg.out"],
    local = 1,
    visibility = ["//visibility:public"],
    cmd = """
        WORK_DIR="$$(pwd)" &&
        PY_RUST_DIR="$$WORK_DIR/python/ray/rust" &&
        mkdir -p "$$PY_RUST_DIR" &&
        cp -f $(location //rust/ray-rs-worker:rust_worker) "$$PY_RUST_DIR/" &&
        echo "$$WORK_DIR" > $@
    """,
)
