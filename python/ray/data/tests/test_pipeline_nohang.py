import pytest

import ray
from ray.tests.conftest import *  # noqa


def test_basic_actors(shutdown_only):
    for _ in range(10):
        ray.init(num_cpus=2)
        n = 10
        ds = ray.data.range(n)
        ds = ds.window(blocks_per_window=1)
        assert sorted(ds.map(lambda x: x + 1,
                             compute="actors").take()) == list(
                                 range(1, n + 1))


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
