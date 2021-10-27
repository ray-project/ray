def run():
    import ray
    import os
    ray.init(address="auto")

    @ray.remote
    def foo():
        return os.environ.get("TEST_SUBPROCESS_JOB_CONFIG_ENV_VAR", None)

    print(ray.get(foo.remote()))

if __name__ == "__main__":
    run()