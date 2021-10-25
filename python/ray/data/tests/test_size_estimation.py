import pytest

from ray.tests.conftest import *  # noqa
from ray.data.impl.simple_block import SimpleBlockBuilder
from ray.data.impl.arrow_block import ArrowBlockBuilder

SMALL_VALUE = "a" * 100
LARGE_VALUE = "a" * 10000
ARROW_SMALL_VALUE = {"value": "a" * 100}
ARROW_LARGE_VALUE = {"value": "a" * 10000}


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
    b.add_block([SMALL_VALUE] * 900)
    assert_close(b.get_estimated_memory_usage(), 111000)
    assert len(b.build()) == 1000


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
    b.add_block([SMALL_VALUE] * 10000)
    assert_close(b.get_estimated_memory_usage(), 2210000)
    assert len(b.build()) == 10310


def test_arrow_size(ray_start_regular_shared):
    b = ArrowBlockBuilder()
    assert b.get_estimated_memory_usage() == 0
    b.add(ARROW_SMALL_VALUE)
    assert_close(b.get_estimated_memory_usage(), 111)
    b.add(ARROW_SMALL_VALUE)
    assert_close(b.get_estimated_memory_usage(), 222)
    for _ in range(8):
        b.add(ARROW_SMALL_VALUE)
    assert_close(b.get_estimated_memory_usage(), 1110)
    for _ in range(90):
        b.add(ARROW_SMALL_VALUE)
    assert_close(b.get_estimated_memory_usage(), 11100)
    for _ in range(900):
        b.add(ARROW_SMALL_VALUE)
    assert_close(b.get_estimated_memory_usage(), 111000)
    assert b.build().num_rows == 1000


def test_arrow_size_diff_values(ray_start_regular_shared):
    b = ArrowBlockBuilder()
    assert b.get_estimated_memory_usage() == 0
    b.add(ARROW_LARGE_VALUE)
    assert b._num_compactions == 0
    assert_close(b.get_estimated_memory_usage(), 10000)
    b.add(ARROW_LARGE_VALUE)
    assert b._num_compactions == 0
    assert_close(b.get_estimated_memory_usage(), 20000)
    for _ in range(10):
        b.add(ARROW_SMALL_VALUE)
    assert_close(b.get_estimated_memory_usage(), 20000)
    for _ in range(100):
        b.add(ARROW_SMALL_VALUE)
    assert b._num_compactions == 0
    assert_close(b.get_estimated_memory_usage(), 30000)
    for _ in range(13000):
        b.add(ARROW_LARGE_VALUE)
    assert_close(b.get_estimated_memory_usage(), 130200000)
    assert b._num_compactions == 2
    for _ in range(4000):
        b.add(ARROW_LARGE_VALUE)
    assert_close(b.get_estimated_memory_usage(), 170200000)
    assert b._num_compactions == 3
    assert b.build().num_rows == 17112


def test_arrow_size_add_block(ray_start_regular_shared):
    b = ArrowBlockBuilder()
    for _ in range(2000):
        b.add(ARROW_LARGE_VALUE)
    block = b.build()
    b2 = ArrowBlockBuilder()
    for _ in range(5):
        b2.add_block(block)
    assert b2._num_compactions == 0
    assert_close(b2.get_estimated_memory_usage(), 100000000)
    assert b2.build().num_rows == 10000


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
