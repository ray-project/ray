import random
import pytest
import numpy as np
import os
import pickle
try:
    import pytest_timeout
except ImportError:
    pytest_timeout = None
import sys

import ray
import ray.test_utils
import ray.cluster_utils

from ray._raylet import GlobalStateAccessor

def test_job_table(ray_start_regular):
    @ray.remote
    class Actor:
        def __init__(self):
            print("Actor created")

        def f(self):
            return 0

    @ray.remote
    def f():
        a = Actor.remote()
        x_id = a.f.remote()
        return [x_id]

    x_id = ray.get(f.remote())[0]

    state = GlobalStateAccessor("", "", False)
    state.connect()

    print(ray.get(x_id))  # This should not hang.


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
