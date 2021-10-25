import ray

from ray.tests.conftest import *  # noqa
from ray.data.impl.simple_block import SimpleBlockBuilder


SMALL_VALUE = "a" * 100
LARGE_VALUE = "a" * 10000


def assert_close(actual, expected, tolerance=.5):
    print("assert_close", actual, expected)
    assert abs(actual - expected) / expected < tolerance, \
        (actual, expected)


def test_py_size(ray_start_regular_shared):
    b = SimpleBlockBuilder()
    assert b.get_estimated_memory_usage() == 0
    b.add(SMALL_VALUE)
    assert_close(b.get_estimated_memory_usage(), 111)
    b.add(SMALL_VALUE)
    assert_close(b.get_estimated_memory_usage(), 222)
    for _ in range(8):
        b.add(SMALL_VALUE)
    assert_close(b.get_estimated_memory_usage(), 1110)
    for _ in range(90):
        b.add(SMALL_VALUE)
    assert_close(b.get_estimated_memory_usage(), 11100)
    for _ in range(900):
        b.add(SMALL_VALUE)
    assert_close(b.get_estimated_memory_usage(), 111000)


def test_py_size_diff_values(ray_start_regular_shared):
    b = SimpleBlockBuilder()
    assert b.get_estimated_memory_usage() == 0
    for _ in range(10):
        b.add(LARGE_VALUE)
    assert_close(b.get_estimated_memory_usage(), 100000)
    for _ in range(100):
        b.add(SMALL_VALUE)
    assert_close(b.get_estimated_memory_usage(), 110000)
    for _ in range(100):
        b.add(LARGE_VALUE)
    assert_close(b.get_estimated_memory_usage(), 1110000)
    for _ in range(100):
        b.add(LARGE_VALUE)
    assert_close(b.get_estimated_memory_usage(), 2110000)
    for _ in range(1000):
        b.add(SMALL_VALUE)
    assert_close(b.get_estimated_memory_usage(), 2210000)


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
