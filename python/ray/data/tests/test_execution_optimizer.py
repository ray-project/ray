import pytest

import ray
from ray.data.context import DatasetContext


def test_e2e_optimizer_sanity(ray_start_cluster_enabled):
    ctx = DatasetContext.get_current()
    ctx.new_execution_backend = True
    ctx.optimizer_enabled = True
    ds = ray.data.range(5).map_batches(lambda x: x)
    assert ds.take_all() == [0, 1, 2, 3, 4], ds


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
