import time
from typing import List, Optional, Tuple
import os
import ray
import numpy as np

from contextlib import contextmanager

# Only run tests matching this filter pattern.

filter_pattern = os.environ.get("TESTS_TO_RUN", "")


def timeit(name, fn, multiplier=1) -> List[Optional[Tuple[str, float, float]]]:
    if filter_pattern not in name:
        return [None]
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

    mean = np.mean(stats)
    sd = np.std(stats)
    print(name, "per second", round(mean, 2), "+-", round(sd, 2))
    return [(name, mean, sd)]


@contextmanager
def ray_setup_and_teardown(**init_args):
    ray.init(**init_args)
    try:
        yield None
    finally:
        ray.shutdown()
