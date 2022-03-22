import os
import sys
import pytest
import math

import ray
from ray.util.multiprocessing import Pool


def teardown_function(function):
    # Delete environment variable if set.
    if "RAY_ADDRESS" in os.environ:
        del os.environ["RAY_ADDRESS"]


@pytest.fixture
def ray_start_1_cpu():
    address_info = ray.init(num_cpus=1)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


def test_maxtasksperchild(shutdown_only):
    def f(args):
        return os.getpid()

    pool = Pool(5, maxtasksperchild=1)
    assert len(set(pool.map(f, range(20)))) == 20
    pool.terminate()
    pool.join()


def test_deadlock_avoidance_in_recursive_tasks(ray_start_1_cpu):
    def poolit_a(_):
        with Pool(ray_address="auto") as pool:
            return list(pool.map(math.sqrt, range(0, 2, 1)))

    def poolit_b():
        with Pool(ray_address="auto") as pool:
            return list(pool.map(poolit_a, range(2, 4, 1)))

    result = poolit_b()
    assert result == [[0.0, 1.0], [0.0, 1.0]]


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
