import time
import os
import ray
import numpy as np

from contextlib import contextmanager

# Only run tests matching this filter pattern.
filter_pattern = os.environ.get("TESTS_TO_RUN", "")


def timeit(name, fn, multiplier=1):
    if filter_pattern not in name:
        return
    # warmup
    start = time.time()
    while time.time() - start < 1:
        fn()
    # real run
    stats = []
    for _ in range(4):
        start = time.time()
        count = 0
        while time.time() - start < 2:
            fn()
            count += 1
        end = time.time()
        stats.append(multiplier * count / (end - start))
    print(name, "per second", round(np.mean(stats), 2), "+-",
          round(np.std(stats), 2))


@contextmanager
def ray_setup_and_teardown(**init_args):
    ray.init(**init_args)
    try:
        yield None
    finally:
        ray.shutdown()
