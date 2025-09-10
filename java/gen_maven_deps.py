from bazel.gen_extract import gen_extract

if __name__ == "__main__":
    gen_extract(["java/maven_deps.zip"], sub_dir="java")
