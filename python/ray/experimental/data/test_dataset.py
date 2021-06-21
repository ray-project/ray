import pytest

import ray


def test_basic(ray_start_regular_shared):
    ds = ray.experimental.data.range(5)
    assert ds.map(lambda x: x + 1).collect() == [1, 2, 3, 4, 5]


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
