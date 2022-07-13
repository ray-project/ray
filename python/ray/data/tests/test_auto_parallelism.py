import pytest

import ray
from ray.data.context import DatasetContext
from ray.tests.conftest import *  # noqa


def test_auto_parallelism_basic(shutdown_only):
    ray.init(num_cpus=8)
    context = DatasetContext.get_current()
    context.min_parallelism = 1
    # Datasource bound.
    ds = ray.data.range_tensor(5, shape=(100,), parallelism=-1)
    assert ds.num_blocks() == 5, ds
    # CPU bound. TODO(ekl) we should fix range datasource to respect parallelism more
    # properly, currently it can go a little over.
    ds = ray.data.range_tensor(10000, shape=(100,), parallelism=-1)
    assert ds.num_blocks() == 16, ds
    # Block size bound.
    ds = ray.data.range_tensor(100000000, shape=(100,), parallelism=-1)
    assert ds.num_blocks() == 150, ds


def test_auto_parallelism_placement_group(shutdown_only):
    ray.init(num_cpus=16, num_gpus=8)

    @ray.remote
    def run():
        context = DatasetContext.get_current()
        context.min_parallelism = 1
        ds = ray.data.range_tensor(10000, shape=(100,), parallelism=-1)
        return ds.num_blocks()

    # 1/16 * 4 * 16 = 4
    pg = ray.util.placement_group([{"CPU": 1}])
    num_blocks = ray.get(run.options(placement_group=pg).remote())
    assert num_blocks == 4, num_blocks

    # 2/16 * 4 * 16 = 8
    pg = ray.util.placement_group([{"CPU": 2}])
    num_blocks = ray.get(run.options(placement_group=pg).remote())
    assert num_blocks == 8, num_blocks

    # 1/8 * 4 * 16 = 8
    pg = ray.util.placement_group([{"CPU": 1, "GPU": 1}])
    num_blocks = ray.get(run.options(placement_group=pg).remote())
    assert num_blocks == 8, num_blocks


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
