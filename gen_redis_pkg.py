from bazel.gen_extract import gen_extract

if __name__ == "__main__":
    gen_extract(
        [
            "ray_redis.zip",
        ],
        clear_dir_first=[
            "ray/core/src/ray/thirdparty/redis/src",
        ],
    )
