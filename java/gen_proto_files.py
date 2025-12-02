from bazel.gen_extract import gen_extract

if __name__ == "__main__":
    gen_extract(
        ["java/proto_files.zip"],
        clear_dir_first=[
            "runtime/src/main/java/io/ray/runtime/generated",
            "serve/src/main/java/io/ray/serve/generated",
        ],
        sub_dir="java",
    )
