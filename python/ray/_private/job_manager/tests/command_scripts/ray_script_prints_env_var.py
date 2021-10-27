"""
A dummy ray driver script that executes in subprocess. Prints env var
for job submission API testing.
"""


def run():
    import ray
    import os
    ray.init(address=os.environ["RAY_ADDRESS"])

    @ray.remote
    def foo():
        return "bar"

    ray.get(foo.remote())
    print(os.environ.get("TEST_SUBPROCESS_JOB_CONFIG_ENV_VAR", None))


if __name__ == "__main__":
    run()
