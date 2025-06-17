import random
import time

import numpy as np
import pandas as pd
import pytest

import ray
from ray.data._internal.execution.interfaces.ref_bundle import (
    _ref_bundles_iterator_to_block_refs_list,
)
from ray.data.context import DataContext
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.util import named_values
from ray.tests.conftest import *  # noqa

RANDOM_SEED = 123


def test_empty_shuffle(
    ray_start_regular_shared_2_cpus, disable_fallback_to_object_extension
):
    ds = ray.data.range(100, override_num_blocks=100)
    ds = ds.filter(lambda x: x)
    ds = ds.map_batches(lambda x: x)
    ds = ds.random_shuffle()  # Would prev. crash with AssertionError: pyarrow.Table.
    ds.show()


@pytest.mark.parametrize("num_parts", [1, 30])
@pytest.mark.parametrize("ds_format", ["arrow", "pandas"])
def test_global_tabular_sum(
    ray_start_regular_shared_2_cpus,
    ds_format,
    num_parts,
    configure_shuffle_method,
    disable_fallback_to_object_extension,
):
    seed = int(time.time())
    print(f"Seeding RNG for test_global_arrow_sum with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)

    def _to_pandas(ds):
        return ds.map_batches(lambda x: x, batch_size=None, batch_format="pandas")

    # Test built-in global sum aggregation
    ds = ray.data.from_items([{"A": x} for x in xs]).repartition(num_parts)
    if ds_format == "pandas":
        ds = _to_pandas(ds)
    assert ds.sum("A") == 4950

    # Test empty dataset
    ds = ray.data.range(10)
    if ds_format == "pandas":
        ds = _to_pandas(ds)
    assert ds.filter(lambda r: r["id"] > 10).sum("id") is None

    # Test built-in global sum aggregation with nans
    nan_ds = ray.data.from_items([{"A": x} for x in xs] + [{"A": None}]).repartition(
        num_parts
    )
    if ds_format == "pandas":
        nan_ds = _to_pandas(nan_ds)
    assert nan_ds.sum("A") == 4950
    # Test ignore_nulls=False
    assert pd.isnull(nan_ds.sum("A", ignore_nulls=False))
    # Test all nans
    nan_ds = ray.data.from_items([{"A": None}] * len(xs)).repartition(num_parts)
    if ds_format == "pandas":
        nan_ds = _to_pandas(nan_ds)
    assert nan_ds.sum("A") is None
    assert pd.isnull(nan_ds.sum("A", ignore_nulls=False))


def test_random_block_order_schema(
    ray_start_regular_shared_2_cpus, disable_fallback_to_object_extension
):
    df = pd.DataFrame({"a": np.random.rand(10), "b": np.random.rand(10)})
    ds = ray.data.from_pandas(df).randomize_block_order()
    ds.schema().names == ["a", "b"]


def test_random_block_order(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    disable_fallback_to_object_extension,
):
    ctx = DataContext.get_current()
    ctx.execution_options.preserve_order = True

    # Test BlockList.randomize_block_order.
    ds = ray.data.range(12).repartition(4)
    ds = ds.randomize_block_order(seed=0)

    results = ds.take()
    expected = named_values("id", [6, 7, 8, 0, 1, 2, 3, 4, 5, 9, 10, 11])
    assert results == expected

    # Test LazyBlockList.randomize_block_order.
    lazy_blocklist_ds = ray.data.range(12, override_num_blocks=4)
    lazy_blocklist_ds = lazy_blocklist_ds.randomize_block_order(seed=0)
    lazy_blocklist_results = lazy_blocklist_ds.take()
    lazy_blocklist_expected = named_values("id", [6, 7, 8, 0, 1, 2, 3, 4, 5, 9, 10, 11])
    assert lazy_blocklist_results == lazy_blocklist_expected


# NOTE: All tests above share a Ray cluster, while the tests below do not. These
# tests should only be carefully reordered to retain this invariant!


def test_random_shuffle(
    shutdown_only, configure_shuffle_method, disable_fallback_to_object_extension
):
    # Assert random 2 distinct random-shuffle pipelines yield different orders
    r1 = ray.data.range(100).random_shuffle().take(999)
    r2 = ray.data.range(100).random_shuffle().take(999)
    assert r1 != r2, (r1, r2)

    # Assert same random-shuffle pipeline yielding 2 different orders,
    # when executed
    ds = ray.data.range(100).random_shuffle()
    r1 = ds.take(999)
    r2 = ds.take(999)
    assert r1 != r2, (r1, r2)

    r1 = ray.data.range(100, override_num_blocks=1).random_shuffle().take(999)
    r2 = ray.data.range(100, override_num_blocks=1).random_shuffle().take(999)
    assert r1 != r2, (r1, r2)

    assert (
        ray.data.range(100).random_shuffle().repartition(1)._plan.initial_num_blocks()
        == 1
    )
    r1 = ray.data.range(100).random_shuffle().repartition(1).take(999)
    r2 = ray.data.range(100).random_shuffle().repartition(1).take(999)
    assert r1 != r2, (r1, r2)

    r0 = ray.data.range(100, override_num_blocks=5).take(999)
    r1 = ray.data.range(100, override_num_blocks=5).random_shuffle(seed=0).take(999)
    r2 = ray.data.range(100, override_num_blocks=5).random_shuffle(seed=0).take(999)
    r3 = ray.data.range(100, override_num_blocks=5).random_shuffle(seed=12345).take(999)
    assert r1 == r2, (r1, r2)
    assert r1 != r0, (r1, r0)
    assert r1 != r3, (r1, r3)

    r0 = ray.data.range(100, override_num_blocks=5).take(999)
    r1 = ray.data.range(100, override_num_blocks=5).random_shuffle(seed=0).take(999)
    r2 = ray.data.range(100, override_num_blocks=5).random_shuffle(seed=0).take(999)
    assert r1 == r2, (r1, r2)
    assert r1 != r0, (r1, r0)

    # Test move.
    ds = ray.data.range(100, override_num_blocks=2)
    r1 = ds.random_shuffle().take(999)
    ds = ds.map(lambda x: x).take(999)
    r2 = ray.data.range(100).random_shuffle().take(999)
    assert r1 != r2, (r1, r2)

    # Test empty dataset.
    ds = ray.data.from_items([])
    r1 = ds.random_shuffle()
    assert r1.count() == 0
    assert r1.take() == ds.take()


def test_random_shuffle_check_random(
    shutdown_only, disable_fallback_to_object_extension
):
    # Rows from the same input should not be contiguous in the final output.
    num_files = 10
    num_rows = 100
    items = [i for i in range(num_files) for _ in range(num_rows)]
    ds = ray.data.from_items(items, override_num_blocks=num_files)
    out = ds.random_shuffle().take(num_files * num_rows)
    for i in range(num_files):
        part = out[i * num_rows : (i + 1) * num_rows]
        seen = set()
        num_contiguous = 1
        prev = -1
        for x in part:
            x = x["item"]
            if prev != x:
                prev = x
                num_contiguous = 1
            else:
                num_contiguous += 1
                assert num_contiguous < (
                    num_rows / num_files
                ), f"{part} contains too many contiguous rows from same input block"
            seen.add(x)
        assert (
            set(range(num_files)) == seen
        ), f"{part} does not contain elements from all input blocks"

    # Rows from the same input should appear in a different order in the
    # output.
    num_files = 10
    num_rows = 100
    items = [j for i in range(num_files) for j in range(num_rows)]
    ds = ray.data.from_items(items, override_num_blocks=num_files)
    out = ds.random_shuffle().take(num_files * num_rows)
    for i in range(num_files):
        part = out[i * num_rows : (i + 1) * num_rows]
        num_increasing = 0
        prev = -1
        for x in part:
            x = x["item"]
            if x >= prev:
                num_increasing += 1
            else:
                assert num_increasing < (
                    num_rows / num_files
                ), f"{part} contains non-shuffled rows from input blocks"
                num_increasing = 0
            prev = x


def test_random_shuffle_with_custom_resource(
    ray_start_cluster, configure_shuffle_method, disable_fallback_to_object_extension
):
    cluster = ray_start_cluster
    # Create two nodes which have different custom resources.
    cluster.add_node(
        resources={"foo": 100},
        num_cpus=1,
    )
    cluster.add_node(resources={"bar": 100}, num_cpus=1)

    ray.init(cluster.address)

    # Run dataset in "bar" nodes.
    ds = ray.data.read_parquet(
        "example://parquet_images_mini",
        override_num_blocks=2,
        ray_remote_args={"resources": {"bar": 1}},
    )
    ds = ds.random_shuffle(resources={"bar": 1}).materialize()
    assert "1 nodes used" in ds.stats()
    assert "2 nodes used" not in ds.stats()


def test_random_shuffle_spread(
    ray_start_cluster, configure_shuffle_method, disable_fallback_to_object_extension
):
    cluster = ray_start_cluster
    cluster.add_node(
        resources={"bar:1": 100},
        num_cpus=10,
        _system_config={"max_direct_call_object_size": 0},
    )
    cluster.add_node(resources={"bar:2": 100}, num_cpus=10)
    cluster.add_node(resources={"bar:3": 100}, num_cpus=0)

    ray.init(cluster.address)

    @ray.remote
    def get_node_id():
        return ray.get_runtime_context().get_node_id()

    node1_id = ray.get(get_node_id.options(resources={"bar:1": 1}).remote())
    node2_id = ray.get(get_node_id.options(resources={"bar:2": 1}).remote())

    ds = ray.data.range(100, override_num_blocks=2).random_shuffle()
    bundles = ds.iter_internal_ref_bundles()
    blocks = _ref_bundles_iterator_to_block_refs_list(bundles)
    ray.wait(blocks, num_returns=len(blocks), fetch_local=False)
    location_data = ray.experimental.get_object_locations(blocks)
    locations = []
    for block in blocks:
        locations.extend(location_data[block]["node_ids"])
    assert "2 nodes used" in ds.stats()

    if not configure_shuffle_method:
        # We don't check this for push-based shuffle since it will try to
        # colocate reduce tasks to improve locality.
        assert set(locations) == {node1_id, node2_id}


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
