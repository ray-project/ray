import pytest
import pandas as pd

import ray
from ray.tests.conftest import *  # noqa
from ray.data.block import BlockMetadata
from ray.data.context import DataContext
from ray.data.datasource import Datasource, ReadTask
from ray.data.tests.util import extract_values
from ray._private.test_utils import run_string_as_driver


def test_read(ray_start_regular_shared):
    class CustomDatasource(Datasource):
        def prepare_read(self, parallelism: int):
            value = DataContext.get_current().foo
            meta = BlockMetadata(
                num_rows=1, size_bytes=8, schema=None, input_files=None, exec_stats=None
            )
            return [ReadTask(lambda: [pd.DataFrame({"id": [value]})], meta)]

    context = DataContext.get_current()
    context.foo = 12345
    assert ray.data.read_datasource(CustomDatasource()).take_all()[0]["id"] == 12345


def test_map(ray_start_regular_shared):
    context = DataContext.get_current()
    context.foo = 70001
    ds = ray.data.range(1).map(lambda x: {"id": DataContext.get_current().foo})
    assert ds.take_all()[0]["id"] == 70001


def test_map_pipeline(ray_start_regular_shared):
    context = DataContext.get_current()
    context.foo = 8
    pipe = ray.data.range(2).repeat(2)
    pipe = pipe.map(lambda x: {"id": DataContext.get_current().foo})
    [a, b] = pipe.split(2)

    @ray.remote
    def fetch(shard):
        return extract_values("id", shard.take_all())

    assert ray.get([fetch.remote(a), fetch.remote(b)]) == [[8, 8], [8, 8]]


def test_flat_map(ray_start_regular_shared):
    context = DataContext.get_current()
    context.foo = 70002
    ds = ray.data.range(1).flat_map(lambda x: [{"id": DataContext.get_current().foo}])
    assert ds.take_all()[0]["id"] == 70002


def test_map_batches(ray_start_regular_shared):
    context = DataContext.get_current()
    context.foo = 70003
    ds = ray.data.range(1).map_batches(
        lambda x: {"id": [DataContext.get_current().foo]}
    )
    assert ds.take_all()[0]["id"] == 70003


def test_filter(shutdown_only):
    context = DataContext.get_current()
    context.foo = 70004
    ds = ray.data.from_items([70004]).filter(
        lambda x: x["item"] == DataContext.get_current().foo
    )
    assert ds.take_all()[0]["item"] == 70004


def test_context_placement_group():
    driver_code = """
import ray
from ray.data.context import DataContext
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy
from ray._private.test_utils import placement_group_assert_no_leak

ray.init(num_cpus=1)

context = DataContext.get_current()
# This placement group will take up all cores of the local cluster.
placement_group = ray.util.placement_group(
    name="core_hog",
    strategy="SPREAD",
    bundles=[
        {"CPU": 1},
    ],
)
ray.get(placement_group.ready())
context.scheduling_strategy = PlacementGroupSchedulingStrategy(placement_group)
pipe = ray.data.range(100, parallelism=2) \
    .window(blocks_per_window=1) \
    .map(lambda x: {"id": x["id"] + 1})
assert pipe.take_all() == [{"id": x} for x in range(1, 101)]
placement_group_assert_no_leak([placement_group])
ray.shutdown()
    """

    # Successful exit is sufficient to verify this test.
    run_string_as_driver(driver_code)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
