from bazel.gen_extract import gen_extract

if __name__ == "__main__":
    gen_extract(
        [
            "java/ray_java_pkg.zip",
        ],
        clear_dir_first=[
            "ray/jars",
        ],
    )
