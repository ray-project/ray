import pandas as pd
import pytest

import ray
from ray._private.test_utils import run_string_as_driver
from ray.data.block import BlockMetadata
from ray.data.context import DataContext
from ray.data.datasource import Datasource, ReadTask
from ray.tests.conftest import *  # noqa


def test_context_saved_when_dataset_created(ray_start_regular_shared):
    ctx = DataContext.get_current()
    d1 = ray.data.range(10)
    d2 = ray.data.range(10)
    assert ctx.eager_free
    assert d1.context.eager_free
    assert d2.context.eager_free

    d1.context.eager_free = False
    assert not d1.context.eager_free
    assert d2.context.eager_free
    assert ctx.eager_free

    @ray.remote(num_cpus=0)
    def check(d1, d2):
        assert not d1.context.eager_free
        assert d2.context.eager_free

    ray.get(check.remote(d1, d2))

    @ray.remote(num_cpus=0)
    def check2(d):
        d.take()

    @ray.remote(num_cpus=0)
    def check3(d):
        list(d.streaming_split(1)[0].iter_batches())

    d1.context.execution_options.resource_limits.cpu = 0.1
    ray.get(check2.remote(d2))
    ray.get(check3.remote(d2))


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
ds = ray.data.range(100, parallelism=2).map(lambda x: {"id": x["id"] + 1})
assert ds.take_all() == [{"id": x} for x in range(1, 101)]
placement_group_assert_no_leak([placement_group])
ray.shutdown()
    """

    # Successful exit is sufficient to verify this test.
    run_string_as_driver(driver_code)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
