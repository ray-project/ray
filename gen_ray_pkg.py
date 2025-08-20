from bazel.gen_extract import gen_extract

if __name__ == "__main__":
    gen_extract(
        [
            "ray_pkg.zip",
            "ray_py_proto.zip",
        ],
        clear_dir_first=[
            "ray/core/generated",
            "ray/serve/generated",
        ],
    )
