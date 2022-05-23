"""
A dummy ray driver script that executes in subprocess. Prints runtime_env
from ray's runtime context for job submission API testing.
"""

import ray


def run():
    ray.init()

    @ray.remote
    def foo():
        return "bar"

    ray.get(foo.remote())

    print(ray.get_runtime_context().runtime_env)


if __name__ == "__main__":
    run()
