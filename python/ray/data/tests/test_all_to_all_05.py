import itertools
import random
import time
from typing import Optional

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
from packaging.version import parse as parse_version

import ray
from ray._private.arrow_utils import get_pyarrow_version
from ray.data._internal.arrow_ops.transform_pyarrow import (
    MIN_PYARROW_VERSION_TYPE_PROMOTION,
    combine_chunks,
)
from ray.data._internal.execution.interfaces.ref_bundle import (
    _ref_bundles_iterator_to_block_refs_list,
)
from ray.data._internal.planner.exchange.sort_task_spec import SortKey
from ray.data._internal.util import is_nan
from ray.data.aggregate import (
    AbsMax,
    AggregateFn,
    Count,
    Max,
    Mean,
    Min,
    Quantile,
    Std,
    Sum,
    Unique,
)
from ray.data.block import BlockAccessor
from ray.data.context import DataContext, ShuffleStrategy
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.util import named_values
from ray.tests.conftest import *  # noqa

RANDOM_SEED = 123


def test_groupby_map_groups_with_different_types(
    ray_start_regular_shared_2_cpus,
    configure_shuffle_method,
    disable_fallback_to_object_extension,
):
    ds = ray.data.from_items(
        [
            {"group": 1, "value": 1},
            {"group": 1, "value": 2},
            {"group": 2, "value": 3},
            {"group": 2, "value": 4},
        ]
    )

    def func(batch):
        # Test output type is Python list, different from input type.
        return {"group": [batch["group"][0]], "out": [min(batch["value"])]}

    ds = ds.groupby("group").map_groups(func)

    assert [x["out"] for x in ds.sort("group").take_all()] == [1, 3]


@pytest.mark.parametrize("num_parts", [1, 30])
def test_groupby_map_groups_multiple_batch_formats(
    ray_start_regular_shared_2_cpus,
    num_parts,
    configure_shuffle_method,
    disable_fallback_to_object_extension,
):
    # Reproduces https://github.com/ray-project/ray/issues/39206
    def identity(batch):
        return batch

    xs = list(range(100))
    ds = ray.data.from_items([{"A": (x % 3), "B": x} for x in xs]).repartition(
        num_parts
    )
    grouped_ds = (
        ds.groupby("A")
        .map_groups(identity)
        .map_batches(identity, batch_format="pandas")
    )
    agg_ds = grouped_ds.groupby("A").max("B")
    assert agg_ds.count() == 3
    assert list(agg_ds.sort("A").iter_rows()) == [
        {"A": 0, "max(B)": 99},
        {"A": 1, "max(B)": 97},
        {"A": 2, "max(B)": 98},
    ]


def test_groupby_map_groups_ray_remote_args_fn(
    ray_start_regular_shared_2_cpus, configure_shuffle_method
):
    ds = ray.data.from_items(
        [
            {"group": 1, "value": 1},
            {"group": 1, "value": 2},
            {"group": 2, "value": 3},
            {"group": 2, "value": 4},
        ]
    )

    def func(df):
        import os

        df["value"] = int(os.environ["__MY_TEST__"])
        return df

    ds = ds.groupby("group").map_groups(
        func,
        ray_remote_args_fn=lambda: {"runtime_env": {"env_vars": {"__MY_TEST__": "69"}}},
    )
    assert sorted([x["value"] for x in ds.take()]) == [69, 69, 69, 69]


def test_groupby_map_groups_extra_args(
    ray_start_regular_shared_2_cpus,
    configure_shuffle_method,
    disable_fallback_to_object_extension,
):
    ds = ray.data.from_items(
        [
            {"group": 1, "value": 1},
            {"group": 1, "value": 2},
            {"group": 2, "value": 3},
            {"group": 2, "value": 4},
        ]
    )

    def func(df, a, b, c):
        df["value"] = df["value"] * a + b + c
        return df

    ds = ds.groupby("group").map_groups(
        func,
        fn_args=(2, 1),
        fn_kwargs={"c": 3},
    )
    assert sorted([x["value"] for x in ds.take()]) == [6, 8, 10, 12]


_NEED_UNWRAP_ARROW_SCALAR = get_pyarrow_version() <= parse_version("9.0.0")


@pytest.mark.parametrize("num_parts", [1, 30])
@pytest.mark.parametrize("ds_format", ["pyarrow", "pandas", "numpy"])
def test_groupby_map_groups_multicolumn(
    ray_start_regular_shared_2_cpus,
    ds_format,
    num_parts,
    configure_shuffle_method,
    disable_fallback_to_object_extension,
):
    # Test built-in count aggregation
    random.seed(RANDOM_SEED)
    xs = list(range(100))
    random.shuffle(xs)

    ds = ray.data.from_items([{"A": (x % 2), "B": (x % 3)} for x in xs]).repartition(
        num_parts
    )

    should_unwrap_pa_scalars = ds_format == "pyarrow" and _NEED_UNWRAP_ARROW_SCALAR

    def _map_group(df):
        # NOTE: Since we're grouping by A and B, these columns will be bearing
        #       the same values.
        a = df["A"][0]
        b = df["B"][0]
        return {
            # NOTE: PA 9.0 requires explicit unwrapping into Python objects
            "A": [a.as_py() if should_unwrap_pa_scalars else a],
            "B": [b.as_py() if should_unwrap_pa_scalars else b],
            "count": [len(df["A"])],
        }

    agg_ds = ds.groupby(["A", "B"]).map_groups(
        _map_group,
        batch_format=ds_format,
    )

    assert agg_ds.sort(["A", "B"]).take_all() == [
        {"A": 0, "B": 0, "count": 17},
        {"A": 0, "B": 1, "count": 16},
        {"A": 0, "B": 2, "count": 17},
        {"A": 1, "B": 0, "count": 17},
        {"A": 1, "B": 1, "count": 17},
        {"A": 1, "B": 2, "count": 16},
    ]


@pytest.mark.parametrize("num_parts", [1, 30])
@pytest.mark.parametrize("ds_format", ["pyarrow", "pandas", "numpy"])
def test_groupby_map_groups_multicolumn_with_nan(
    ray_start_regular_shared_2_cpus,
    ds_format,
    num_parts,
    configure_shuffle_method,
    disable_fallback_to_object_extension,
):
    # Test with some NaN values
    rng = np.random.default_rng(RANDOM_SEED)
    xs = np.arange(100, dtype=np.float64)
    xs[-5:] = np.nan
    rng.shuffle(xs)

    ds = ray.data.from_items(
        [
            {
                "A": (x % 2) if np.isfinite(x) else x,
                "B": (x % 3) if np.isfinite(x) else x,
            }
            for x in xs
        ]
    ).repartition(num_parts)

    should_unwrap_pa_scalars = ds_format == "pyarrow" and _NEED_UNWRAP_ARROW_SCALAR

    def _map_group(df):
        # NOTE: Since we're grouping by A and B, these columns will be bearing
        #       the same values
        a = df["A"][0]
        b = df["B"][0]
        return {
            # NOTE: PA 9.0 requires explicit unwrapping into Python objects
            "A": [a.as_py() if should_unwrap_pa_scalars else a],
            "B": [b.as_py() if should_unwrap_pa_scalars else b],
            "count": [len(df["A"])],
        }

    agg_ds = ds.groupby(["A", "B"]).map_groups(
        _map_group,
        batch_format=ds_format,
    )

    rows = agg_ds.sort(["A", "B"]).take_all()

    # NOTE: Nans are not comparable directly, hence
    #       we have to split the assertion in 2
    assert rows[:-1] == [
        {"A": 0.0, "B": 0.0, "count": 16},
        {"A": 0.0, "B": 1.0, "count": 16},
        {"A": 0.0, "B": 2.0, "count": 16},
        {"A": 1.0, "B": 0.0, "count": 16},
        {"A": 1.0, "B": 1.0, "count": 16},
        {"A": 1.0, "B": 2.0, "count": 15},
    ]

    assert (
        np.isnan(rows[-1]["A"]) and np.isnan(rows[-1]["B"]) and rows[-1]["count"] == 5
    )


def test_groupby_map_groups_with_partial(disable_fallback_to_object_extension):
    """
    The partial function name should show up as
    +- Sort
       +- MapBatches(func)
    """
    from functools import partial

    def func(x, y):
        return {f"x_add_{y}": [len(x["id"]) + y]}

    df = pd.DataFrame({"id": list(range(100))})
    df["key"] = df["id"] % 5

    ds = ray.data.from_pandas(df).groupby("key").map_groups(partial(func, y=5))
    result = ds.take_all()

    assert result == [
        {"x_add_5": 25},
        {"x_add_5": 25},
        {"x_add_5": 25},
        {"x_add_5": 25},
        {"x_add_5": 25},
    ]
    assert "MapBatches(func)" in ds.__repr__()


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
