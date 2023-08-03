# Test that we print unhandled exceptions correctly in interactive
# consoles. TODO(ekl) why won't this run within pytest?

import sys
import time

import ray

num_exceptions = 0


def interceptor(e):
    global num_exceptions
    num_exceptions += 1


@ray.remote
def f():
    raise ValueError()


if __name__ == "__main__":
    # Core worker uses this to check if we are in an interactive
    # console (e.g., Jupyter)
    setattr(sys, "ps1", "dummy")
    ray.init(num_cpus=1)

    # Test we report unhandled exceptions.
    ray._private.worker._unhandled_error_handler = interceptor
    x1 = f.remote()

    start = time.time()
    while time.time() - start < 10:
        if num_exceptions == 1:
            sys.exit(0)
        time.sleep(0.5)
        print("wait for exception", num_exceptions)

    assert False
