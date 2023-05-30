"""
A dummy ray driver script that executes in subprocess.
Checks that job manager's environment variable is different.
"""

import ray
import os


def run():
    ray.init()

    @ray.remote
    def foo():
        print("worker", os.nice(0))

    ray.get(foo.remote())


if __name__ == "__main__":
    print("driver", os.nice(0))
    run()
