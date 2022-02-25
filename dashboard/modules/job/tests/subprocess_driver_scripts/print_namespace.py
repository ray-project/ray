"""
A dummy ray driver script that executes in subprocess. Prints namespace
from ray's runtime context for job submission API testing.
"""

import ray
import os


def run():
    ray.init(address=os.environ["RAY_ADDRESS"])

    @ray.remote
    def foo():
        return "bar"

    ray.get(foo.remote())

    print(ray.get_runtime_context().namespace)


if __name__ == "__main__":
    run()
