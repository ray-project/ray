import random

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.data._internal.planner.exchange.sort_task_spec import SortKey, SortTaskSpec
from ray.data.block import BlockAccessor
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.util import extract_values
from ray.tests.conftest import *  # noqa


@pytest.mark.parametrize(
    "descending,boundaries",
    [
        (True, list(range(100, 1000, 200))),
        (False, list(range(100, 1000, 200))),
        (True, [1, 998]),
        (False, [1, 998]),
        # Test float.
        (True, [501.5]),
        (False, [501.5]),
    ],
)
def test_sort_with_specified_boundaries(ray_start_regular, descending, boundaries):
    num_items = 1000
    ds = ray.data.range(num_items)
    ds = ds.sort("id", descending, boundaries).materialize()

    items = range(num_items)
    boundaries = [0] + sorted([round(b) for b in boundaries]) + [num_items]
    expected_blocks = [
        items[boundaries[i] : boundaries[i + 1]] for i in range(len(boundaries) - 1)
    ]
    if descending:
        expected_blocks = [list(reversed(block)) for block in reversed(expected_blocks)]

    blocks = list(ds.iter_batches(batch_size=None))
    assert len(blocks) == len(expected_blocks)
    for block, expected_block in zip(blocks, expected_blocks):
        assert np.all(block["id"] == expected_block)


def test_sort_multiple_keys_produces_equally_sized_blocks(ray_start_regular):
    # Test for https://github.com/ray-project/ray/issues/45303.
    ds = ray.data.from_items(
        [{"a": i, "b": j} for i in range(2) for j in range(5)], override_num_blocks=5
    )

    ds_sorted = ds.sort(["a", "b"], descending=[False, True])

    num_rows_per_block = [
        bundle.num_rows() for bundle in ds_sorted.iter_internal_ref_bundles()
    ]
    # Number of output blocks should be equal to the number of input blocks.
    assert len(num_rows_per_block) == 5, len(num_rows_per_block)
    # Ideally we should have 10 rows / 5 blocks = 2 rows per block, but to make this
    # test less fragile we allow for a small deviation.
    assert all(
        1 <= num_rows <= 3 for num_rows in num_rows_per_block
    ), num_rows_per_block


def test_sort_simple(ray_start_regular, configure_shuffle_method):
    num_items = 100
    parallelism = 4
    xs = list(range(num_items))
    random.shuffle(xs)
    ds = ray.data.from_items(xs, override_num_blocks=parallelism)
    assert extract_values("item", ds.sort("item").take(num_items)) == list(
        range(num_items)
    )
    # Make sure we have rows in each block.
    assert len([n for n in ds.sort("item")._block_num_rows() if n > 0]) == parallelism

    assert extract_values(
        "item", ds.sort("item", descending=True).take(num_items)
    ) == list(reversed(range(num_items)))

    # Test empty dataset.
    ds = ray.data.from_items([])
    s1 = ds.sort("item")
    assert s1.count() == 0
    assert s1.take() == ds.take()
    ds = ray.data.range(10).filter(lambda r: r["id"] > 10).sort("id")
    assert ds.count() == 0


def test_sort_partition_same_key_to_same_block(
    ray_start_regular, configure_shuffle_method
):
    num_items = 100
    xs = [1] * num_items
    ds = ray.data.from_items(xs)
    sorted_ds = ds.repartition(num_items).sort("item")

    # We still have 100 blocks
    assert len(sorted_ds._block_num_rows()) == num_items
    # Only one of them is non-empty
    count = sum(1 for x in sorted_ds._block_num_rows() if x > 0)
    assert count == 1
    # That non-empty block contains all rows
    total = sum(x for x in sorted_ds._block_num_rows() if x > 0)
    assert total == num_items


@pytest.mark.parametrize("num_items,parallelism", [(100, 1), (1000, 4)])
def test_sort_arrow(
    ray_start_regular,
    num_items,
    parallelism,
    configure_shuffle_method,
    use_polars_sort,
):
    ctx = ray.data.context.DataContext.get_current()

    try:
        original_use_polars = ctx.use_polars_sort
        ctx.use_polars_sort = use_polars_sort

        a = list(reversed(range(num_items)))
        b = [f"{x:03}" for x in range(num_items)]
        shard = int(np.ceil(num_items / parallelism))
        offset = 0
        dfs = []
        while offset < num_items:
            dfs.append(
                pd.DataFrame(
                    {"a": a[offset : offset + shard], "b": b[offset : offset + shard]}
                )
            )
            offset += shard
        if offset < num_items:
            dfs.append(pd.DataFrame({"a": a[offset:], "b": b[offset:]}))
        ds = ray.data.from_blocks(dfs).map_batches(
            lambda t: t, batch_format="pyarrow", batch_size=None
        )

        def assert_sorted(sorted_ds, expected_rows):
            assert [tuple(row.values()) for row in sorted_ds.iter_rows()] == list(
                expected_rows
            )

        assert_sorted(ds.sort(key="a"), zip(reversed(a), reversed(b)))
        # Make sure we have rows in each block.
        assert (
            len([n for n in ds.sort(key="a")._block_num_rows() if n > 0]) == parallelism
        )
        assert_sorted(ds.sort(key="b"), zip(a, b))
        assert_sorted(ds.sort(key="a", descending=True), zip(a, b))
    finally:
        ctx.use_polars_sort = original_use_polars


def test_sort(ray_start_regular, use_polars_sort):
    import random

    import pyarrow as pa

    N = 100
    r = random.Random(0xDEED)

    ints = [r.randint(0, 10) for _ in range(N)]
    floats = [r.normalvariate(0, 5) for _ in range(N)]
    t = pa.Table.from_pydict({"ints": ints, "floats": floats})

    sorted_block = BlockAccessor.for_block(t).sort(SortKey(["ints", "floats"]))

    sorted_tuples = list(zip(*sorted(zip(ints, floats))))

    assert sorted_block == pa.Table.from_pydict(
        {"ints": sorted_tuples[0], "floats": sorted_tuples[1]}
    )


def test_sort_arrow_with_empty_blocks(
    ray_start_regular, configure_shuffle_method, use_polars_sort
):
    ctx = ray.data.context.DataContext.get_current()

    try:
        original_use_polars = ctx.use_polars_sort
        ctx.use_polars_sort = use_polars_sort

        assert (
            BlockAccessor.for_block(pa.Table.from_pydict({}))
            .sample(10, SortKey("A"))
            .num_rows
            == 0
        )

        partitions = BlockAccessor.for_block(
            pa.Table.from_pydict({})
        ).sort_and_partition([1, 5, 10], SortKey("A"))
        assert len(partitions) == 4
        for partition in partitions:
            assert partition.num_rows == 0

        assert (
            BlockAccessor.for_block(pa.Table.from_pydict({}))
            .merge_sorted_blocks([pa.Table.from_pydict({})], SortKey("A"))[1]
            .metadata.num_rows
            == 0
        )

        ds = ray.data.from_items(
            [{"A": (x % 3), "B": x} for x in range(3)], override_num_blocks=3
        )
        ds = ds.filter(lambda r: r["A"] == 0)
        assert list(ds.sort("A").iter_rows()) == [{"A": 0, "B": 0}]

        # Test empty dataset.
        ds = ray.data.range(10).filter(lambda r: r["id"] > 10)
        assert (
            len(
                SortTaskSpec.sample_boundaries(
                    ds._plan.execute().block_refs, SortKey("id"), 3
                )
            )
            == 2
        )
        assert ds.sort("id").count() == 0
    finally:
        ctx.use_polars_sort = original_use_polars


@pytest.mark.parametrize("descending", [False, True])
@pytest.mark.parametrize("batch_format", ["pyarrow", "pandas"])
def test_sort_with_multiple_keys(ray_start_regular, descending, batch_format):
    num_items = 1000
    num_blocks = 100
    df = pd.DataFrame(
        {
            "a": [random.choice("ABCD") for _ in range(num_items)],
            "b": [x % 3 for x in range(num_items)],
            "c": [bool(random.getrandbits(1)) for _ in range(num_items)],
        }
    )
    ds = ray.data.from_pandas(df).map_batches(
        lambda t: t,
        batch_format=batch_format,
        batch_size=None,
    )
    df.sort_values(
        ["a", "b", "c"],
        inplace=True,
        ascending=[not descending, descending, not descending],
    )
    sorted_ds = ds.repartition(num_blocks).sort(
        ["a", "b", "c"], descending=[descending, not descending, descending]
    )

    # Number of blocks is preserved
    assert len(sorted_ds._block_num_rows()) == num_blocks
    # Rows are sorted over the dimensions
    assert [tuple(row.values()) for row in sorted_ds.iter_rows()] == list(
        zip(df["a"], df["b"], df["c"])
    )


@pytest.mark.parametrize("num_items,parallelism", [(100, 1), (1000, 4)])
def test_sort_pandas(
    ray_start_regular, num_items, parallelism, configure_shuffle_method
):
    a = list(reversed(range(num_items)))
    b = [f"{x:03}" for x in range(num_items)]
    shard = int(np.ceil(num_items / parallelism))
    offset = 0
    dfs = []
    while offset < num_items:
        dfs.append(
            pd.DataFrame(
                {"a": a[offset : offset + shard], "b": b[offset : offset + shard]}
            )
        )
        offset += shard
    if offset < num_items:
        dfs.append(pd.DataFrame({"a": a[offset:], "b": b[offset:]}))
    ds = ray.data.from_blocks(dfs)

    def assert_sorted(sorted_ds, expected_rows):
        assert [tuple(row.values()) for row in sorted_ds.iter_rows()] == list(
            expected_rows
        )

    assert_sorted(ds.sort(key="a"), zip(reversed(a), reversed(b)))
    # Make sure we have rows in each block.
    assert len([n for n in ds.sort(key="a")._block_num_rows() if n > 0]) == parallelism
    assert_sorted(ds.sort(key="b"), zip(a, b))
    assert_sorted(ds.sort(key="a", descending=True), zip(a, b))


def test_sort_pandas_with_empty_blocks(ray_start_regular, configure_shuffle_method):
    assert (
        BlockAccessor.for_block(pa.Table.from_pydict({}))
        .sample(10, SortKey("A"))
        .num_rows
        == 0
    )

    partitions = BlockAccessor.for_block(pa.Table.from_pydict({})).sort_and_partition(
        [1, 5, 10], SortKey("A")
    )
    assert len(partitions) == 4
    for partition in partitions:
        assert partition.num_rows == 0

    assert (
        BlockAccessor.for_block(pa.Table.from_pydict({}))
        .merge_sorted_blocks([pa.Table.from_pydict({})], SortKey("A"))[1]
        .metadata.num_rows
        == 0
    )

    ds = ray.data.from_items(
        [{"A": (x % 3), "B": x} for x in range(3)], override_num_blocks=3
    )
    ds = ds.filter(lambda r: r["A"] == 0)
    assert list(ds.sort("A").iter_rows()) == [{"A": 0, "B": 0}]

    # Test empty dataset.
    ds = ray.data.range(10).filter(lambda r: r["id"] > 10)
    assert (
        len(
            SortTaskSpec.sample_boundaries(
                ds._plan.execute().block_refs, SortKey("id"), 3
            )
        )
        == 2
    )
    assert ds.sort("id").count() == 0


def test_sort_with_one_block(shutdown_only, configure_shuffle_method):
    ray.init(num_cpus=8)
    ctx = ray.data.DataContext.get_current()
    ctx.execution_options.verbose_progress = True
    ctx.use_push_based_shuffle = True

    # Use a dataset that will produce only one block to sort.
    ray.data.range(1024).map_batches(
        lambda _: pa.table([pa.array([1])], ["token_counts"])
    ).sum("token_counts")


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
