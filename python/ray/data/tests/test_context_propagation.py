import pytest

import ray
from ray.tests.conftest import *  # noqa
from ray.data.block import BlockMetadata
from ray.data.context import DatasetContext
from ray.data.datasource import Datasource, ReadTask


def test_read(ray_start_regular_shared):
    class CustomDatasource(Datasource):
        def prepare_read(self, parallelism: int):
            value = DatasetContext.get_current().foo
            meta = BlockMetadata(
                num_rows=1, size_bytes=8, schema=None, input_files=None, exec_stats=None
            )
            return [ReadTask(lambda: [[value]], meta)]

    context = DatasetContext.get_current()
    context.foo = 12345
    assert ray.data.read_datasource(CustomDatasource()).take_all()[0] == 12345


def test_map(ray_start_regular_shared):
    context = DatasetContext.get_current()
    context.foo = 70001
    ds = ray.data.range(1).map(lambda x: DatasetContext.get_current().foo)
    assert ds.take_all()[0] == 70001


def test_map_pipeline(ray_start_regular_shared):
    context = DatasetContext.get_current()
    context.foo = 8
    pipe = ray.data.range(2).repeat(2)
    pipe = pipe.map(lambda x: DatasetContext.get_current().foo)
    [a, b] = pipe.split(2)

    @ray.remote
    def fetch(shard):
        return shard.take_all()

    assert ray.get([fetch.remote(a), fetch.remote(b)]) == [[8, 8], [8, 8]]


def test_flat_map(ray_start_regular_shared):
    context = DatasetContext.get_current()
    context.foo = 70002
    ds = ray.data.range(1).flat_map(lambda x: [DatasetContext.get_current().foo])
    assert ds.take_all()[0] == 70002


def test_map_batches(ray_start_regular_shared):
    context = DatasetContext.get_current()
    context.foo = 70003
    ds = ray.data.range(1).map_batches(lambda x: [DatasetContext.get_current().foo])
    assert ds.take_all()[0] == 70003


def test_filter(ray_start_regular_shared):
    context = DatasetContext.get_current()
    context.foo = 70004
    ds = ray.data.from_items([70004]).filter(
        lambda x: x == DatasetContext.get_current().foo
    )
    assert ds.take_all()[0] == 70004


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
