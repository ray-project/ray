import itertools
import math
import random
import time

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.data.aggregate import AggregateFn, Count, Max, Mean, Min, Std, Sum, Quantile
from ray.data.context import DataContext
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.util import column_udf, named_values, STRICT_MODE
from ray.tests.conftest import *  # noqa


def test_zip(ray_start_regular_shared):
    ds1 = ray.data.range(5, parallelism=5)
    ds2 = ray.data.range(5, parallelism=5).map(column_udf("id", lambda x: x + 1))
    ds = ds1.zip(ds2)
    assert ds.schema().names == ["id", "id_1"]
    assert ds.take() == named_values(
        ["id", "id_1"], [(0, 1), (1, 2), (2, 3), (3, 4), (4, 5)]
    )
    with pytest.raises(ValueError):
        ds.zip(ray.data.range(3)).materialize()


@pytest.mark.parametrize(
    "num_blocks1,num_blocks2",
    list(itertools.combinations_with_replacement(range(1, 12), 2)),
)
def test_zip_different_num_blocks_combinations(
    ray_start_regular_shared, num_blocks1, num_blocks2
):
    n = 12
    ds1 = ray.data.range(n, parallelism=num_blocks1)
    ds2 = ray.data.range(n, parallelism=num_blocks2).map(
        column_udf("id", lambda x: x + 1)
    )
    ds = ds1.zip(ds2)
    assert ds.schema().names == ["id", "id_1"]
    assert ds.take() == named_values(
        ["id", "id_1"], list(zip(range(n), range(1, n + 1)))
    )


@pytest.mark.parametrize(
    "num_cols1,num_cols2,should_invert",
    [
        (1, 1, False),
        (4, 1, False),
        (1, 4, True),
        (1, 10, True),
        (10, 10, False),
    ],
)
def test_zip_different_num_blocks_split_smallest(
    ray_start_regular_shared,
    num_cols1,
    num_cols2,
    should_invert,
):
    n = 12
    num_blocks1 = 4
    num_blocks2 = 2
    ds1 = ray.data.from_items(
        [{str(i): i for i in range(num_cols1)}] * n, parallelism=num_blocks1
    )
    ds2 = ray.data.from_items(
        [{str(i): i for i in range(num_cols1, num_cols1 + num_cols2)}] * n,
        parallelism=num_blocks2,
    )
    ds = ds1.zip(ds2).materialize()
    num_blocks = ds._plan._snapshot_blocks.executed_num_blocks()
    assert ds.take() == [{str(i): i for i in range(num_cols1 + num_cols2)}] * n
    if should_invert:
        assert num_blocks == num_blocks2
    else:
        assert num_blocks == num_blocks1


def test_zip_pandas(ray_start_regular_shared):
    ds1 = ray.data.from_pandas(pd.DataFrame({"col1": [1, 2], "col2": [4, 5]}))
    ds2 = ray.data.from_pandas(pd.DataFrame({"col3": ["a", "b"], "col4": ["d", "e"]}))
    ds = ds1.zip(ds2)
    assert ds.count() == 2
    assert "{col1: int64, col2: int64, col3: object, col4: object}" in str(ds)
    result = list(ds.take())
    assert result[0] == {"col1": 1, "col2": 4, "col3": "a", "col4": "d"}

    ds3 = ray.data.from_pandas(pd.DataFrame({"col2": ["a", "b"], "col4": ["d", "e"]}))
    ds = ds1.zip(ds3)
    assert ds.count() == 2
    assert "{col1: int64, col2: int64, col2_1: object, col4: object}" in str(ds)
    result = list(ds.take())
    assert result[0] == {"col1": 1, "col2": 4, "col2_1": "a", "col4": "d"}


def test_zip_arrow(ray_start_regular_shared):
    ds1 = ray.data.range(5).map(lambda r: {"id": r["id"]})
    ds2 = ray.data.range(5).map(lambda r: {"a": r["id"] + 1, "b": r["id"] + 2})
    ds = ds1.zip(ds2)
    assert ds.count() == 5
    assert "{id: int64, a: int64, b: int64}" in str(ds)
    result = list(ds.take())
    assert result[0] == {"id": 0, "a": 1, "b": 2}

    # Test duplicate column names.
    ds = ds1.zip(ds1).zip(ds1)
    assert ds.count() == 5
    assert "{id: int64, id_1: int64, id_2: int64}" in str(ds)
    result = list(ds.take())
    assert result[0] == {"id": 0, "id_1": 0, "id_2": 0}


def test_zip_preserve_order(ray_start_regular_shared):
    def foo(x):
        import time

        if x["item"] < 5:
            time.sleep(1)
        return x

    num_items = 10
    items = list(range(num_items))
    ds1 = ray.data.from_items(items, parallelism=num_items)
    ds2 = ray.data.from_items(items, parallelism=num_items)
    ds2 = ds2.map_batches(foo, batch_size=1)
    result = ds1.zip(ds2).take_all()
    assert result == named_values(
        ["item", "item_1"], list(zip(range(num_items), range(num_items)))
    ), result


def test_empty_shuffle(ray_start_regular_shared):
    ds = ray.data.range(100, parallelism=100)
    ds = ds.filter(lambda x: x)
    ds = ds.map_batches(lambda x: x)
    ds = ds.random_shuffle()  # Would prev. crash with AssertionError: pyarrow.Table.
    ds.show()


def test_repartition_shuffle(ray_start_regular_shared):
    ds = ray.data.range(20, parallelism=10)
    assert ds.num_blocks() == 10
    assert ds.sum() == 190
    assert ds._block_num_rows() == [2] * 10

    ds2 = ds.repartition(5, shuffle=True)
    assert ds2.num_blocks() == 5
    assert ds2.sum() == 190
    assert ds2._block_num_rows() == [10, 10, 0, 0, 0]

    ds3 = ds2.repartition(20, shuffle=True)
    assert ds3.num_blocks() == 20
    assert ds3.sum() == 190
    assert ds3._block_num_rows() == [2] * 10 + [0] * 10

    large = ray.data.range(10000, parallelism=10)
    large = large.repartition(20, shuffle=True)
    assert large._block_num_rows() == [500] * 20


def test_repartition_noshuffle(ray_start_regular_shared):
    ds = ray.data.range(20, parallelism=10)
    assert ds.num_blocks() == 10
    assert ds.sum() == 190
    assert ds._block_num_rows() == [2] * 10

    ds2 = ds.repartition(5, shuffle=False)
    assert ds2.num_blocks() == 5
    assert ds2.sum() == 190
    assert ds2._block_num_rows() == [4, 4, 4, 4, 4]

    ds3 = ds2.repartition(20, shuffle=False)
    assert ds3.num_blocks() == 20
    assert ds3.sum() == 190
    assert ds3._block_num_rows() == [1] * 20

    # Test num_partitions > num_rows
    ds4 = ds.repartition(40, shuffle=False)
    assert ds4.num_blocks() == 40

    assert ds4.sum() == 190
    assert ds4._block_num_rows() == [1] * 20 + [0] * 20

    ds5 = ray.data.range(22).repartition(4)
    assert ds5.num_blocks() == 4
    assert ds5._block_num_rows() == [5, 6, 5, 6]

    large = ray.data.range(10000, parallelism=10)
    large = large.repartition(20)
    assert large._block_num_rows() == [500] * 20


def test_repartition_shuffle_arrow(ray_start_regular_shared):
    ds = ray.data.range(20, parallelism=10)
    assert ds.num_blocks() == 10
    assert ds.count() == 20
    assert ds._block_num_rows() == [2] * 10

    ds2 = ds.repartition(5, shuffle=True)
    assert ds2.num_blocks() == 5
    assert ds2.count() == 20
    assert ds2._block_num_rows() == [10, 10, 0, 0, 0]

    ds3 = ds2.repartition(20, shuffle=True)
    assert ds3.num_blocks() == 20
    assert ds3.count() == 20
    assert ds3._block_num_rows() == [2] * 10 + [0] * 10

    large = ray.data.range(10000, parallelism=10)
    large = large.repartition(20, shuffle=True)
    assert large._block_num_rows() == [500] * 20


def test_grouped_datastream_repr(ray_start_regular_shared):
    ds = ray.data.from_items([{"key": "spam"}, {"key": "ham"}, {"key": "spam"}])
    assert repr(ds.groupby("key")) == f"GroupedData(datastream={ds!r}, key='key')"


def test_groupby_arrow(ray_start_regular_shared, use_push_based_shuffle):
    # Test empty datastream.
    agg_ds = ray.data.range(10).filter(lambda r: r["id"] > 10).groupby("value").count()
    assert agg_ds.count() == 0


def test_groupby_errors(ray_start_regular_shared):
    ds = ray.data.range(100)
    ds.groupby(None).count().show()  # OK
    with pytest.raises(ValueError):
        ds.groupby(lambda x: x % 2).count().show()
    with pytest.raises(ValueError):
        ds.groupby("foo").count().show()


def test_agg_errors(ray_start_regular_shared):
    from ray.data.aggregate import Max

    ds = ray.data.range(100)
    ds.aggregate(Max("id"))  # OK
    with pytest.raises(ValueError):
        ds.aggregate(Max())
    with pytest.raises(ValueError):
        ds.aggregate(Max(lambda x: x))
    with pytest.raises(ValueError):
        ds.aggregate(Max("bad_field"))


@pytest.mark.parametrize("num_parts", [1, 30])
def test_groupby_agg_name_conflict(ray_start_regular_shared, num_parts):
    # Test aggregation name conflict.
    xs = list(range(100))
    grouped_ds = (
        ray.data.from_items([{"A": (x % 3), "B": x} for x in xs])
        .repartition(num_parts)
        .groupby("A")
    )
    agg_ds = grouped_ds.aggregate(
        AggregateFn(
            init=lambda k: [0, 0],
            accumulate_row=lambda a, r: [a[0] + r["B"], a[1] + 1],
            merge=lambda a1, a2: [a1[0] + a2[0], a1[1] + a2[1]],
            finalize=lambda a: a[0] / a[1],
            name="foo",
        ),
        AggregateFn(
            init=lambda k: [0, 0],
            accumulate_row=lambda a, r: [a[0] + r["B"], a[1] + 1],
            merge=lambda a1, a2: [a1[0] + a2[0], a1[1] + a2[1]],
            finalize=lambda a: a[0] / a[1],
            name="foo",
        ),
    )
    assert agg_ds.count() == 3
    assert list(agg_ds.sort("A").iter_rows()) == [
        {"A": 0, "foo": 49.5, "foo_2": 49.5},
        {"A": 1, "foo": 49.0, "foo_2": 49.0},
        {"A": 2, "foo": 50.0, "foo_2": 50.0},
    ]


@pytest.mark.parametrize("num_parts", [1, 30])
@pytest.mark.parametrize("ds_format", ["arrow", "pandas"])
def test_groupby_tabular_count(
    ray_start_regular_shared, ds_format, num_parts, use_push_based_shuffle
):
    # Test built-in count aggregation
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_arrow_count with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)

    def _to_pandas(ds):
        return ds.map_batches(lambda x: x, batch_size=None, batch_format="pandas")

    ds = ray.data.from_items([{"A": (x % 3), "B": x} for x in xs]).repartition(
        num_parts
    )
    if ds_format == "pandas":
        ds = _to_pandas(ds)
    agg_ds = ds.groupby("A").count()
    assert agg_ds.count() == 3
    assert list(agg_ds.sort("A").iter_rows()) == [
        {"A": 0, "count()": 34},
        {"A": 1, "count()": 33},
        {"A": 2, "count()": 33},
    ]


@pytest.mark.parametrize("num_parts", [1, 30])
@pytest.mark.parametrize("ds_format", ["arrow", "pandas"])
def test_groupby_tabular_sum(
    ray_start_regular_shared, ds_format, num_parts, use_push_based_shuffle
):
    # Test built-in sum aggregation
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_tabular_sum with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)

    def _to_pandas(ds):
        return ds.map_batches(lambda x: x, batch_size=None, batch_format="pandas")

    ds = ray.data.from_items([{"A": (x % 3), "B": x} for x in xs]).repartition(
        num_parts
    )
    if ds_format == "pandas":
        ds = _to_pandas(ds)

    agg_ds = ds.groupby("A").sum("B")
    assert agg_ds.count() == 3
    assert list(agg_ds.sort("A").iter_rows()) == [
        {"A": 0, "sum(B)": 1683},
        {"A": 1, "sum(B)": 1617},
        {"A": 2, "sum(B)": 1650},
    ]

    # Test built-in sum aggregation with nans
    ds = ray.data.from_items(
        [{"A": (x % 3), "B": x} for x in xs] + [{"A": 0, "B": None}]
    ).repartition(num_parts)
    if ds_format == "pandas":
        ds = _to_pandas(ds)
    nan_grouped_ds = ds.groupby("A")
    nan_agg_ds = nan_grouped_ds.sum("B")
    assert nan_agg_ds.count() == 3
    assert list(nan_agg_ds.sort("A").iter_rows()) == [
        {"A": 0, "sum(B)": 1683},
        {"A": 1, "sum(B)": 1617},
        {"A": 2, "sum(B)": 1650},
    ]
    # Test ignore_nulls=False
    nan_agg_ds = nan_grouped_ds.sum("B", ignore_nulls=False)
    assert nan_agg_ds.count() == 3
    pd.testing.assert_frame_equal(
        nan_agg_ds.sort("A").to_pandas(),
        pd.DataFrame(
            {
                "A": [0, 1, 2],
                "sum(B)": [None, 1617, 1650],
            }
        ),
        check_dtype=False,
    )
    # Test all nans
    ds = ray.data.from_items([{"A": (x % 3), "B": None} for x in xs]).repartition(
        num_parts
    )
    if ds_format == "pandas":
        ds = _to_pandas(ds)
    nan_agg_ds = ds.groupby("A").sum("B")
    assert nan_agg_ds.count() == 3
    pd.testing.assert_frame_equal(
        nan_agg_ds.sort("A").to_pandas(),
        pd.DataFrame(
            {
                "A": [0, 1, 2],
                "sum(B)": [None, None, None],
            }
        ),
    )


@pytest.mark.parametrize("num_parts", [1, 30])
@pytest.mark.parametrize("ds_format", ["arrow", "pandas"])
def test_global_tabular_sum(ray_start_regular_shared, ds_format, num_parts):
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

    # Test empty datastream
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
    assert nan_ds.sum("A", ignore_nulls=False) is None
    # Test all nans
    nan_ds = ray.data.from_items([{"A": None}] * len(xs)).repartition(num_parts)
    if ds_format == "pandas":
        nan_ds = _to_pandas(nan_ds)
    assert nan_ds.sum("A") is None
    assert nan_ds.sum("A", ignore_nulls=False) is None


@pytest.mark.parametrize("num_parts", [1, 30])
@pytest.mark.parametrize("ds_format", ["arrow", "pandas"])
def test_groupby_tabular_min(ray_start_regular_shared, ds_format, num_parts):
    # Test built-in min aggregation
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_tabular_min with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)

    def _to_pandas(ds):
        return ds.map_batches(lambda x: x, batch_size=None, batch_format="pandas")

    ds = ray.data.from_items([{"A": (x % 3), "B": x} for x in xs]).repartition(
        num_parts
    )
    if ds_format == "pandas":
        ds = _to_pandas(ds)

    agg_ds = ds.groupby("A").min("B")
    assert agg_ds.count() == 3
    assert list(agg_ds.sort("A").iter_rows()) == [
        {"A": 0, "min(B)": 0},
        {"A": 1, "min(B)": 1},
        {"A": 2, "min(B)": 2},
    ]

    # Test built-in min aggregation with nans
    ds = ray.data.from_items(
        [{"A": (x % 3), "B": x} for x in xs] + [{"A": 0, "B": None}]
    ).repartition(num_parts)
    if ds_format == "pandas":
        ds = _to_pandas(ds)
    nan_grouped_ds = ds.groupby("A")
    nan_agg_ds = nan_grouped_ds.min("B")
    assert nan_agg_ds.count() == 3
    assert list(nan_agg_ds.sort("A").iter_rows()) == [
        {"A": 0, "min(B)": 0},
        {"A": 1, "min(B)": 1},
        {"A": 2, "min(B)": 2},
    ]
    # Test ignore_nulls=False
    nan_agg_ds = nan_grouped_ds.min("B", ignore_nulls=False)
    assert nan_agg_ds.count() == 3
    pd.testing.assert_frame_equal(
        nan_agg_ds.sort("A").to_pandas(),
        pd.DataFrame(
            {
                "A": [0, 1, 2],
                "min(B)": [None, 1, 2],
            }
        ),
        check_dtype=False,
    )
    # Test all nans
    ds = ray.data.from_items([{"A": (x % 3), "B": None} for x in xs]).repartition(
        num_parts
    )
    if ds_format == "pandas":
        ds = _to_pandas(ds)
    nan_agg_ds = ds.groupby("A").min("B")
    assert nan_agg_ds.count() == 3
    pd.testing.assert_frame_equal(
        nan_agg_ds.sort("A").to_pandas(),
        pd.DataFrame(
            {
                "A": [0, 1, 2],
                "min(B)": [None, None, None],
            }
        ),
        check_dtype=False,
    )


@pytest.mark.parametrize("num_parts", [1, 30])
@pytest.mark.parametrize("ds_format", ["arrow", "pandas"])
def test_groupby_tabular_max(ray_start_regular_shared, ds_format, num_parts):
    # Test built-in max aggregation
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_tabular_max with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)

    def _to_pandas(ds):
        return ds.map_batches(lambda x: x, batch_size=None, batch_format="pandas")

    ds = ray.data.from_items([{"A": (x % 3), "B": x} for x in xs]).repartition(
        num_parts
    )
    if ds_format == "pandas":
        ds = _to_pandas(ds)

    agg_ds = ds.groupby("A").max("B")
    assert agg_ds.count() == 3
    assert list(agg_ds.sort("A").iter_rows()) == [
        {"A": 0, "max(B)": 99},
        {"A": 1, "max(B)": 97},
        {"A": 2, "max(B)": 98},
    ]

    # Test built-in min aggregation with nans
    ds = ray.data.from_items(
        [{"A": (x % 3), "B": x} for x in xs] + [{"A": 0, "B": None}]
    ).repartition(num_parts)
    if ds_format == "pandas":
        ds = _to_pandas(ds)
    nan_grouped_ds = ds.groupby("A")
    nan_agg_ds = nan_grouped_ds.max("B")
    assert nan_agg_ds.count() == 3
    assert list(nan_agg_ds.sort("A").iter_rows()) == [
        {"A": 0, "max(B)": 99},
        {"A": 1, "max(B)": 97},
        {"A": 2, "max(B)": 98},
    ]
    # Test ignore_nulls=False
    nan_agg_ds = nan_grouped_ds.max("B", ignore_nulls=False)
    assert nan_agg_ds.count() == 3
    pd.testing.assert_frame_equal(
        nan_agg_ds.sort("A").to_pandas(),
        pd.DataFrame(
            {
                "A": [0, 1, 2],
                "max(B)": [None, 97, 98],
            }
        ),
        check_dtype=False,
    )
    # Test all nans
    ds = ray.data.from_items([{"A": (x % 3), "B": None} for x in xs]).repartition(
        num_parts
    )
    if ds_format == "pandas":
        ds = _to_pandas(ds)
    nan_agg_ds = ds.groupby("A").max("B")
    assert nan_agg_ds.count() == 3
    pd.testing.assert_frame_equal(
        nan_agg_ds.sort("A").to_pandas(),
        pd.DataFrame(
            {
                "A": [0, 1, 2],
                "max(B)": [None, None, None],
            }
        ),
        check_dtype=False,
    )


@pytest.mark.parametrize("num_parts", [1, 30])
@pytest.mark.parametrize("ds_format", ["arrow", "pandas"])
def test_groupby_tabular_mean(ray_start_regular_shared, ds_format, num_parts):
    # Test built-in mean aggregation
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_tabular_mean with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)

    def _to_pandas(ds):
        return ds.map_batches(lambda x: x, batch_size=None, batch_format="pandas")

    ds = ray.data.from_items([{"A": (x % 3), "B": x} for x in xs]).repartition(
        num_parts
    )
    if ds_format == "pandas":
        ds = _to_pandas(ds)

    agg_ds = ds.groupby("A").mean("B")
    assert agg_ds.count() == 3
    assert list(agg_ds.sort("A").iter_rows()) == [
        {"A": 0, "mean(B)": 49.5},
        {"A": 1, "mean(B)": 49.0},
        {"A": 2, "mean(B)": 50.0},
    ]

    # Test built-in mean aggregation with nans
    ds = ray.data.from_items(
        [{"A": (x % 3), "B": x} for x in xs] + [{"A": 0, "B": None}]
    ).repartition(num_parts)
    if ds_format == "pandas":
        ds = _to_pandas(ds)
    nan_grouped_ds = ds.groupby("A")
    nan_agg_ds = nan_grouped_ds.mean("B")
    assert nan_agg_ds.count() == 3
    assert list(nan_agg_ds.sort("A").iter_rows()) == [
        {"A": 0, "mean(B)": 49.5},
        {"A": 1, "mean(B)": 49.0},
        {"A": 2, "mean(B)": 50.0},
    ]
    # Test ignore_nulls=False
    nan_agg_ds = nan_grouped_ds.mean("B", ignore_nulls=False)
    assert nan_agg_ds.count() == 3
    pd.testing.assert_frame_equal(
        nan_agg_ds.sort("A").to_pandas(),
        pd.DataFrame(
            {
                "A": [0, 1, 2],
                "mean(B)": [None, 49.0, 50.0],
            }
        ),
        check_dtype=False,
    )
    # Test all nans
    ds = ray.data.from_items([{"A": (x % 3), "B": None} for x in xs]).repartition(
        num_parts
    )
    if ds_format == "pandas":
        ds = _to_pandas(ds)
    nan_agg_ds = ds.groupby("A").mean("B")
    assert nan_agg_ds.count() == 3
    pd.testing.assert_frame_equal(
        nan_agg_ds.sort("A").to_pandas(),
        pd.DataFrame(
            {
                "A": [0, 1, 2],
                "mean(B)": [None, None, None],
            }
        ),
        check_dtype=False,
    )


@pytest.mark.parametrize("num_parts", [1, 30])
@pytest.mark.parametrize("ds_format", ["arrow", "pandas"])
def test_groupby_tabular_std(ray_start_regular_shared, ds_format, num_parts):
    # Test built-in std aggregation
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_tabular_std with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)

    def _to_arrow(ds):
        return ds.map_batches(lambda x: x, batch_size=None, batch_format="pyarrow")

    df = pd.DataFrame({"A": [x % 3 for x in xs], "B": xs})
    ds = ray.data.from_pandas(df).repartition(num_parts)
    if ds_format == "arrow":
        ds = _to_arrow(ds)
    agg_ds = ds.groupby("A").std("B")
    assert agg_ds.count() == 3
    result = agg_ds.to_pandas()["std(B)"].to_numpy()
    expected = df.groupby("A")["B"].std().to_numpy()
    np.testing.assert_array_almost_equal(result, expected)
    # ddof of 0
    ds = ray.data.from_pandas(df).repartition(num_parts)
    if ds_format == "arrow":
        ds = _to_arrow(ds)
    agg_ds = ds.groupby("A").std("B", ddof=0)
    assert agg_ds.count() == 3
    result = agg_ds.to_pandas()["std(B)"].to_numpy()
    expected = df.groupby("A")["B"].std(ddof=0).to_numpy()
    np.testing.assert_array_almost_equal(result, expected)

    # Test built-in std aggregation with nans
    nan_df = pd.DataFrame({"A": [x % 3 for x in xs] + [0], "B": xs + [None]})
    ds = ray.data.from_pandas(nan_df).repartition(num_parts)
    if ds_format == "arrow":
        ds = _to_arrow(ds)
    nan_grouped_ds = ds.groupby("A")
    nan_agg_ds = nan_grouped_ds.std("B")
    assert nan_agg_ds.count() == 3
    result = nan_agg_ds.to_pandas()["std(B)"].to_numpy()
    expected = nan_df.groupby("A")["B"].std().to_numpy()
    np.testing.assert_array_almost_equal(result, expected)
    # Test ignore_nulls=False
    nan_agg_ds = nan_grouped_ds.std("B", ignore_nulls=False)
    assert nan_agg_ds.count() == 3
    result = nan_agg_ds.to_pandas()["std(B)"].to_numpy()
    expected = nan_df.groupby("A")["B"].std()
    expected[0] = None
    np.testing.assert_array_almost_equal(result, expected)
    # Test all nans
    nan_df = pd.DataFrame({"A": [x % 3 for x in xs], "B": [None] * len(xs)})
    ds = ray.data.from_pandas(nan_df).repartition(num_parts)
    if ds_format == "arrow":
        ds = _to_arrow(ds)
    nan_agg_ds = ds.groupby("A").std("B", ignore_nulls=False)
    assert nan_agg_ds.count() == 3
    result = nan_agg_ds.to_pandas()["std(B)"].to_numpy()
    expected = pd.Series([None] * 3)
    np.testing.assert_array_equal(result, expected)


@pytest.mark.parametrize("num_parts", [1, 30])
def test_groupby_arrow_multicolumn(ray_start_regular_shared, num_parts):
    # Test built-in mean aggregation on multiple columns
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_arrow_multicolumn with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)
    df = pd.DataFrame({"A": [x % 3 for x in xs], "B": xs, "C": [2 * x for x in xs]})
    agg_ds = (
        ray.data.from_pandas(df).repartition(num_parts).groupby("A").mean(["B", "C"])
    )
    assert agg_ds.count() == 3
    assert list(agg_ds.sort("A").iter_rows()) == [
        {"A": 0, "mean(B)": 49.5, "mean(C)": 99.0},
        {"A": 1, "mean(B)": 49.0, "mean(C)": 98.0},
        {"A": 2, "mean(B)": 50.0, "mean(C)": 100.0},
    ]

    # Test that unspecified agg column ==> agg on all columns except for
    # groupby keys.
    agg_ds = ray.data.from_pandas(df).repartition(num_parts).groupby("A").mean()
    assert agg_ds.count() == 3
    assert list(agg_ds.sort("A").iter_rows()) == [
        {"A": 0, "mean(B)": 49.5, "mean(C)": 99.0},
        {"A": 1, "mean(B)": 49.0, "mean(C)": 98.0},
        {"A": 2, "mean(B)": 50.0, "mean(C)": 100.0},
    ]

    # Test built-in global mean aggregation
    df = pd.DataFrame({"A": xs, "B": [2 * x for x in xs]})
    result_row = ray.data.from_pandas(df).repartition(num_parts).mean(["A", "B"])
    assert result_row["mean(A)"] == df["A"].mean()
    assert result_row["mean(B)"] == df["B"].mean()


def test_groupby_agg_bad_on(ray_start_regular_shared):
    # Test bad on for groupby aggregation
    xs = list(range(100))
    df = pd.DataFrame({"A": [x % 3 for x in xs], "B": xs, "C": [2 * x for x in xs]})
    # Wrong type.
    with pytest.raises(Exception):
        ray.data.from_pandas(df).groupby("A").mean(5).materialize()
    with pytest.raises(Exception):
        ray.data.from_pandas(df).groupby("A").mean([5]).materialize()
    # Empty list.
    with pytest.raises(ValueError):
        ray.data.from_pandas(df).groupby("A").mean([]).materialize()
    # Nonexistent column.
    with pytest.raises(ValueError):
        ray.data.from_pandas(df).groupby("A").mean("D").materialize()
    with pytest.raises(ValueError):
        ray.data.from_pandas(df).groupby("A").mean(["B", "D"]).materialize()
    # Columns for simple Datastream.
    with pytest.raises(ValueError):
        ray.data.from_items(xs).groupby(lambda x: x % 3 == 0).mean("A").materialize()

    # Test bad on for global aggregation
    # Wrong type.
    with pytest.raises(Exception):
        ray.data.from_pandas(df).mean(5).materialize()
    with pytest.raises(Exception):
        ray.data.from_pandas(df).mean([5]).materialize()
    # Empty list.
    with pytest.raises(ValueError):
        ray.data.from_pandas(df).mean([]).materialize()
    # Nonexistent column.
    with pytest.raises(ValueError):
        ray.data.from_pandas(df).mean("D").materialize()
    with pytest.raises(ValueError):
        ray.data.from_pandas(df).mean(["B", "D"]).materialize()
    # Columns for simple Datastream.
    with pytest.raises(ValueError):
        ray.data.from_items(xs).mean("A").materialize()


@pytest.mark.parametrize("num_parts", [1, 30])
def test_groupby_arrow_multi_agg(ray_start_regular_shared, num_parts):
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_arrow_multi_agg with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)
    df = pd.DataFrame({"A": [x % 3 for x in xs], "B": xs})
    agg_ds = (
        ray.data.from_pandas(df)
        .repartition(num_parts)
        .groupby("A")
        .aggregate(
            Count(),
            Sum("B"),
            Min("B"),
            Max("B"),
            Mean("B"),
            Std("B"),
            Quantile("B"),
        )
    )
    assert agg_ds.count() == 3
    agg_df = agg_ds.to_pandas()
    expected_grouped = df.groupby("A")["B"]
    np.testing.assert_array_equal(agg_df["count()"].to_numpy(), [34, 33, 33])
    for agg in ["sum", "min", "max", "mean", "quantile", "std"]:
        result = agg_df[f"{agg}(B)"].to_numpy()
        expected = getattr(expected_grouped, agg)().to_numpy()
        if agg == "std":
            np.testing.assert_array_almost_equal(result, expected)
        else:
            np.testing.assert_array_equal(result, expected)
    # Test built-in global std aggregation
    df = pd.DataFrame({"A": xs})

    result_row = (
        ray.data.from_pandas(df)
        .repartition(num_parts)
        .aggregate(
            Sum("A"),
            Min("A"),
            Max("A"),
            Mean("A"),
            Std("A"),
            Quantile("A"),
        )
    )
    for agg in ["sum", "min", "max", "mean", "quantile", "std"]:
        result = result_row[f"{agg}(A)"]
        expected = getattr(df["A"], agg)()
        if agg == "std":
            assert math.isclose(result, expected)
        else:
            assert result == expected


@pytest.mark.parametrize("num_parts", [1, 30])
def test_groupby_arrow_multi_agg_alias(ray_start_regular_shared, num_parts):
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_arrow_multi_agg with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)
    df = pd.DataFrame({"A": [x % 3 for x in xs], "B": xs})
    agg_ds = (
        ray.data.from_pandas(df)
        .repartition(num_parts)
        .groupby("A")
        .aggregate(
            Sum("B", alias_name="sum_b"),
            Min("B", alias_name="min_b"),
            Max("B", alias_name="max_b"),
            Mean("B", alias_name="mean_b"),
            Std("B", alias_name="std_b"),
            Quantile("B", alias_name="quantile_b"),
        )
    )

    agg_df = agg_ds.to_pandas()
    expected_grouped = df.groupby("A")["B"]
    for agg in ["sum", "min", "max", "mean", "quantile", "std"]:
        result = agg_df[f"{agg}_b"].to_numpy()
        print(agg)
        print(result)
        expected = getattr(expected_grouped, agg)().to_numpy()
        print(expected)
        if agg == "std":
            np.testing.assert_array_almost_equal(result, expected)
        else:
            np.testing.assert_array_equal(result, expected)
    # Test built-in global std aggregation
    df = pd.DataFrame({"A": xs})
    result_row = (
        ray.data.from_pandas(df)
        .repartition(num_parts)
        .aggregate(
            Sum("A", alias_name="sum_b"),
            Min("A", alias_name="min_b"),
            Max("A", alias_name="max_b"),
            Mean("A", alias_name="mean_b"),
            Std("A", alias_name="std_b"),
            Quantile("A", alias_name="quantile_b"),
        )
    )
    for agg in ["sum", "min", "max", "mean", "quantile", "std"]:
        result = result_row[f"{agg}_b"]
        print(result)
        expected = getattr(df["A"], agg)()
        print(expected)
        if agg == "std":
            assert math.isclose(result, expected)
        else:
            assert result == expected


@pytest.mark.skipif(STRICT_MODE, reason="Deprecated in strict mode")
def test_groupby_simple(ray_start_regular_shared):
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_simple with: {seed}")
    random.seed(seed)
    parallelism = 3
    xs = [
        ("A", 2),
        ("A", 4),
        ("A", 9),
        ("B", 10),
        ("B", 20),
        ("C", 3),
        ("C", 5),
        ("C", 8),
        ("C", 12),
    ]
    random.shuffle(xs)
    ds = ray.data.from_items(xs, parallelism=parallelism)

    # Mean aggregation
    agg_ds = ds.groupby(lambda r: r[0]).aggregate(
        AggregateFn(
            init=lambda k: (0, 0),
            accumulate_row=lambda a, r: (a[0] + r[1], a[1] + 1),
            merge=lambda a1, a2: (a1[0] + a2[0], a1[1] + a2[1]),
            finalize=lambda a: a[0] / a[1],
        )
    )
    assert agg_ds.count() == 3
    assert agg_ds.sort(key=lambda r: r[0]).take(3) == [("A", 5), ("B", 15), ("C", 7)]

    # Test None row
    parallelism = 2
    xs = ["A", "A", "A", None, None, None, "B"]
    random.shuffle(xs)
    ds = ray.data.from_items(xs, parallelism=parallelism)
    # Count aggregation
    agg_ds = ds.groupby(lambda r: str(r)).aggregate(
        AggregateFn(
            init=lambda k: 0,
            accumulate_row=lambda a, r: a + 1,
            merge=lambda a1, a2: a1 + a2,
        )
    )
    assert agg_ds.count() == 3
    assert agg_ds.sort(key=lambda r: str(r[0])).take(3) == [
        ("A", 3),
        ("B", 1),
        ("None", 3),
    ]

    # Test empty datastream.
    ds = ray.data.from_items([])
    agg_ds = ds.groupby(lambda r: r[0]).aggregate(
        AggregateFn(
            init=lambda k: 1 / 0,  # should never reach here
            accumulate_row=lambda a, r: 1 / 0,
            merge=lambda a1, a2: 1 / 0,
            finalize=lambda a: 1 / 0,
        )
    )
    assert agg_ds.count() == 0
    assert agg_ds.take() == ds.take()
    agg_ds = ray.data.range(10).filter(lambda r: r > 10).groupby(lambda r: r).count()
    assert agg_ds.count() == 0


@pytest.mark.skipif(STRICT_MODE, reason="Deprecated in strict mode")
@pytest.mark.parametrize("num_parts", [1, 30])
def test_groupby_simple_count(ray_start_regular_shared, num_parts):
    # Test built-in count aggregation
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_simple_count with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)
    agg_ds = (
        ray.data.from_items(xs).repartition(num_parts).groupby(lambda x: x % 3).count()
    )
    assert agg_ds.count() == 3
    assert agg_ds.sort(key=lambda r: r[0]).take(3) == [(0, 34), (1, 33), (2, 33)]


@pytest.mark.skipif(STRICT_MODE, reason="Deprecated in strict mode")
@pytest.mark.parametrize("num_parts", [1, 30])
def test_groupby_simple_sum(ray_start_regular_shared, num_parts):
    # Test built-in sum aggregation
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_simple_sum with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)
    agg_ds = (
        ray.data.from_items(xs).repartition(num_parts).groupby(lambda x: x % 3).sum()
    )
    assert agg_ds.count() == 3
    assert agg_ds.sort(key=lambda r: r[0]).take(3) == [(0, 1683), (1, 1617), (2, 1650)]

    # Test built-in sum aggregation with nans
    nan_grouped_ds = (
        ray.data.from_items(xs + [None])
        .repartition(num_parts)
        .groupby(lambda x: int(x or 0) % 3)
    )
    nan_agg_ds = nan_grouped_ds.sum()
    assert nan_agg_ds.count() == 3
    assert nan_agg_ds.sort(key=lambda r: r[0]).take(3) == [
        (0, 1683),
        (1, 1617),
        (2, 1650),
    ]
    # Test ignore_nulls=False
    nan_agg_ds = nan_grouped_ds.sum(ignore_nulls=False)
    assert nan_agg_ds.count() == 3
    assert nan_agg_ds.sort(key=lambda r: r[0]).take(3) == [
        (0, None),
        (1, 1617),
        (2, 1650),
    ]
    # Test all nans
    nan_agg_ds = (
        ray.data.from_items([None] * len(xs))
        .repartition(num_parts)
        .groupby(lambda x: 0)
        .sum()
    )
    assert nan_agg_ds.count() == 1
    assert nan_agg_ds.sort(key=lambda r: r[0]).take(1) == [(0, None)]

    # Test built-in global sum aggregation
    assert ray.data.from_items(xs).repartition(num_parts).sum() == 4950
    assert ray.data.range(10).filter(lambda r: r > 10).sum() is None

    # Test built-in global sum aggregation with nans
    nan_ds = ray.data.from_items(xs + [None]).repartition(num_parts)
    assert nan_ds.sum() == 4950
    # Test ignore_nulls=False
    assert nan_ds.sum(ignore_nulls=False) is None
    # Test all nans
    nan_ds = ray.data.from_items([None] * len(xs)).repartition(num_parts)
    assert nan_ds.sum() is None


@pytest.mark.skipif(STRICT_MODE, reason="Deprecated in strict mode")
def test_groupby_map_groups_for_empty_datastream(ray_start_regular_shared):
    ds = ray.data.from_items([])
    mapped = ds.groupby(lambda x: x % 3).map_groups(lambda x: [min(x) * min(x)])
    assert mapped.count() == 0
    assert mapped.take_all() == []


@pytest.mark.skipif(STRICT_MODE, reason="Deprecated in strict mode")
def test_groupby_map_groups_merging_empty_result(ray_start_regular_shared):
    ds = ray.data.from_items([1, 2, 3])
    # This needs to merge empty and non-empty results from different groups.
    mapped = ds.groupby(lambda x: x).map_groups(lambda x: [] if x == [1] else x)
    assert mapped.count() == 2
    assert mapped.take_all() == [2, 3]


@pytest.mark.skipif(STRICT_MODE, reason="Deprecated in strict mode")
def test_groupby_map_groups_merging_invalid_result(ray_start_regular_shared):
    ds = ray.data.from_items([1, 2, 3])
    grouped = ds.groupby(lambda x: x)

    # The UDF returns None, which is invalid.
    with pytest.raises(TypeError):
        grouped.map_groups(lambda x: None if x == [1] else x).materialize()


@pytest.mark.parametrize("num_parts", [1, 2, 30])
def test_groupby_map_groups_for_none_groupkey(ray_start_regular_shared, num_parts):
    ds = ray.data.from_items(list(range(100)))
    mapped = (
        ds.repartition(num_parts)
        .groupby(None)
        .map_groups(lambda x: {"out": np.array([min(x["item"]) + max(x["item"])])})
    )
    assert mapped.count() == 1
    assert mapped.take_all() == named_values("out", [99])


@pytest.mark.skipif(STRICT_MODE, reason="Deprecated in strict mode")
@pytest.mark.parametrize("num_parts", [1, 2, 30])
def test_groupby_map_groups_returning_empty_result(ray_start_regular_shared, num_parts):
    xs = list(range(100))
    mapped = (
        ray.data.from_items(xs)
        .repartition(num_parts)
        .groupby(lambda x: x % 3)
        .map_groups(lambda x: [])
    )
    assert mapped.count() == 0
    assert mapped.take_all() == []


def test_groupby_map_groups_perf(ray_start_regular_shared):
    data_list = [x % 100 for x in range(5000000)]
    ds = ray.data.from_pandas(pd.DataFrame({"A": data_list}))
    start = time.perf_counter()
    ds.groupby("A").map_groups(lambda df: df)
    end = time.perf_counter()
    # On a t3.2xlarge instance, it ran in about 5 seconds, so expecting it has to
    # finish within about 10x of that time, unless something went wrong.
    assert end - start < 60


@pytest.mark.skipif(STRICT_MODE, reason="Deprecated in strict mode")
@pytest.mark.parametrize("num_parts", [1, 2, 3, 30])
def test_groupby_map_groups_for_list(ray_start_regular_shared, num_parts):
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_simple_count with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)
    mapped = (
        ray.data.from_items(xs)
        .repartition(num_parts)
        .groupby(lambda x: x % 3)
        .map_groups(lambda x: [min(x) * min(x)])
    )
    assert mapped.count() == 3
    assert mapped.take_all() == [0, 1, 4]


@pytest.mark.parametrize("num_parts", [1, 2, 3, 30])
def test_groupby_map_groups_for_pandas(ray_start_regular_shared, num_parts):
    df = pd.DataFrame({"A": "a a b".split(), "B": [1, 1, 3], "C": [4, 6, 5]})
    grouped = ray.data.from_pandas(df).repartition(num_parts).groupby("A")

    # Normalize the numeric columns (i.e. B and C) for each group.
    mapped = grouped.map_groups(
        lambda g: g.apply(
            lambda col: col / g[col.name].sum() if col.name in ["B", "C"] else col
        )
    )

    # The function (i.e. the normalization) performed on each group doesn't
    # aggregate rows, so we still have 3 rows.
    assert mapped.count() == 3
    expected = pd.DataFrame(
        {"A": ["a", "a", "b"], "B": [0.5, 0.5, 1.000000], "C": [0.4, 0.6, 1.0]}
    )
    assert mapped.to_pandas().equals(expected)


@pytest.mark.parametrize("num_parts", [1, 2, 3, 30])
def test_groupby_map_groups_for_arrow(ray_start_regular_shared, num_parts):
    at = pa.Table.from_pydict({"A": "a a b".split(), "B": [1, 1, 3], "C": [4, 6, 5]})
    grouped = ray.data.from_arrow(at).repartition(num_parts).groupby("A")

    # Normalize the numeric columns (i.e. B and C) for each group.
    def normalize(at: pa.Table):
        r = at.select("A")
        sb = pa.compute.sum(at.column("B")).cast(pa.float64())
        r = r.append_column("B", pa.compute.divide(at.column("B"), sb))
        sc = pa.compute.sum(at.column("C")).cast(pa.float64())
        r = r.append_column("C", pa.compute.divide(at.column("C"), sc))
        return r

    mapped = grouped.map_groups(normalize, batch_format="pyarrow")

    # The function (i.e. the normalization) performed on each group doesn't
    # aggregate rows, so we still have 3 rows.
    assert mapped.count() == 3
    expected = pa.Table.from_pydict(
        {"A": ["a", "a", "b"], "B": [0.5, 0.5, 1], "C": [0.4, 0.6, 1]}
    )
    result = pa.Table.from_pandas(mapped.to_pandas())
    assert result.equals(expected)


def test_groupby_map_groups_for_numpy(ray_start_regular_shared):
    ds = ray.data.from_items(
        [
            {"group": 1, "value": 1},
            {"group": 1, "value": 2},
            {"group": 2, "value": 3},
            {"group": 2, "value": 4},
        ]
    )

    def func(group):
        # Test output type is NumPy format.
        return {"group": group["group"] + 1, "value": group["value"] + 1}

    ds = ds.groupby("group").map_groups(func, batch_format="numpy")
    expected = pa.Table.from_pydict({"group": [2, 2, 3, 3], "value": [2, 3, 4, 5]})
    result = pa.Table.from_pandas(ds.to_pandas())
    assert result.equals(expected)


def test_groupby_map_groups_with_different_types(ray_start_regular_shared):
    ds = ray.data.from_items(
        [
            {"group": 1, "value": 1},
            {"group": 1, "value": 2},
            {"group": 2, "value": 3},
            {"group": 2, "value": 4},
        ]
    )

    def func(group):
        # Test output type is Python list, different from input type.
        value = int(group["value"][0])
        return {"out": np.array([value])}

    ds = ds.groupby("group").map_groups(func)
    assert sorted([x["out"] for x in ds.take()]) == [1, 3]


@pytest.mark.skipif(STRICT_MODE, reason="Deprecated in strict mode")
@pytest.mark.parametrize("num_parts", [1, 30])
def test_groupby_simple_min(ray_start_regular_shared, num_parts):
    # Test built-in min aggregation
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_simple_min with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)
    agg_ds = (
        ray.data.from_items(xs).repartition(num_parts).groupby(lambda x: x % 3).min()
    )
    assert agg_ds.count() == 3
    assert agg_ds.sort(key=lambda r: r[0]).take(3) == [(0, 0), (1, 1), (2, 2)]

    # Test built-in min aggregation with nans
    nan_grouped_ds = (
        ray.data.from_items(xs + [None])
        .repartition(num_parts)
        .groupby(lambda x: int(x or 0) % 3)
    )
    nan_agg_ds = nan_grouped_ds.min()
    assert nan_agg_ds.count() == 3
    assert nan_agg_ds.sort(key=lambda r: r[0]).take(3) == [(0, 0), (1, 1), (2, 2)]
    # Test ignore_nulls=False
    nan_agg_ds = nan_grouped_ds.min(ignore_nulls=False)
    assert nan_agg_ds.count() == 3
    assert nan_agg_ds.sort(key=lambda r: r[0]).take(3) == [(0, None), (1, 1), (2, 2)]
    # Test all nans
    nan_agg_ds = (
        ray.data.from_items([None] * len(xs))
        .repartition(num_parts)
        .groupby(lambda x: 0)
        .min()
    )
    assert nan_agg_ds.count() == 1
    assert nan_agg_ds.sort(key=lambda r: r[0]).take(1) == [(0, None)]

    # Test built-in global min aggregation
    assert ray.data.from_items(xs).repartition(num_parts).min() == 0
    assert ray.data.range(10).filter(lambda r: r > 10).min() is None

    # Test built-in global min aggregation with nans
    nan_ds = ray.data.from_items(xs + [None]).repartition(num_parts)
    assert nan_ds.min() == 0
    # Test ignore_nulls=False
    assert nan_ds.min(ignore_nulls=False) is None
    # Test all nans
    nan_ds = ray.data.from_items([None] * len(xs)).repartition(num_parts)
    assert nan_ds.min() is None


@pytest.mark.skipif(STRICT_MODE, reason="Deprecated in strict mode")
@pytest.mark.parametrize("num_parts", [1, 30])
def test_groupby_simple_max(ray_start_regular_shared, num_parts):
    # Test built-in max aggregation
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_simple_max with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)
    agg_ds = (
        ray.data.from_items(xs).repartition(num_parts).groupby(lambda x: x % 3).max()
    )
    assert agg_ds.count() == 3
    assert agg_ds.sort(key=lambda r: r[0]).take(3) == [(0, 99), (1, 97), (2, 98)]

    # Test built-in max aggregation with nans
    nan_grouped_ds = (
        ray.data.from_items(xs + [None])
        .repartition(num_parts)
        .groupby(lambda x: int(x or 0) % 3)
    )
    nan_agg_ds = nan_grouped_ds.max()
    assert nan_agg_ds.count() == 3
    assert nan_agg_ds.sort(key=lambda r: r[0]).take(3) == [(0, 99), (1, 97), (2, 98)]
    # Test ignore_nulls=False
    nan_agg_ds = nan_grouped_ds.max(ignore_nulls=False)
    assert nan_agg_ds.count() == 3
    assert nan_agg_ds.sort(key=lambda r: r[0]).take(3) == [(0, None), (1, 97), (2, 98)]
    # Test all nans
    nan_agg_ds = (
        ray.data.from_items([None] * len(xs))
        .repartition(num_parts)
        .groupby(lambda x: 0)
        .max()
    )
    assert nan_agg_ds.count() == 1
    assert nan_agg_ds.sort(key=lambda r: r[0]).take(1) == [(0, None)]

    # Test built-in global max aggregation
    assert ray.data.from_items(xs).repartition(num_parts).max() == 99
    assert ray.data.range(10).filter(lambda r: r > 10).max() is None

    # Test built-in global max aggregation with nans
    nan_ds = ray.data.from_items(xs + [None]).repartition(num_parts)
    assert nan_ds.max() == 99
    # Test ignore_nulls=False
    assert nan_ds.max(ignore_nulls=False) is None
    # Test all nans
    nan_ds = ray.data.from_items([None] * len(xs)).repartition(num_parts)
    assert nan_ds.max() is None


@pytest.mark.skipif(STRICT_MODE, reason="Deprecated in strict mode")
@pytest.mark.parametrize("num_parts", [1, 30])
def test_groupby_simple_mean(ray_start_regular_shared, num_parts):
    # Test built-in mean aggregation
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_simple_mean with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)
    agg_ds = (
        ray.data.from_items(xs).repartition(num_parts).groupby(lambda x: x % 3).mean()
    )
    assert agg_ds.count() == 3
    assert agg_ds.sort(key=lambda r: r[0]).take(3) == [(0, 49.5), (1, 49.0), (2, 50.0)]

    # Test built-in mean aggregation with nans
    nan_grouped_ds = (
        ray.data.from_items(xs + [None])
        .repartition(num_parts)
        .groupby(lambda x: int(x or 0) % 3)
    )
    nan_agg_ds = nan_grouped_ds.mean()
    assert nan_agg_ds.count() == 3
    assert nan_agg_ds.sort(key=lambda r: r[0]).take(3) == [
        (0, 49.5),
        (1, 49.0),
        (2, 50.0),
    ]
    # Test ignore_nulls=False
    nan_agg_ds = nan_grouped_ds.mean(ignore_nulls=False)
    assert nan_agg_ds.count() == 3
    assert nan_agg_ds.sort(key=lambda r: r[0]).take(3) == [
        (0, None),
        (1, 49.0),
        (2, 50.0),
    ]
    # Test all nans
    nan_agg_ds = (
        ray.data.from_items([None] * len(xs))
        .repartition(num_parts)
        .groupby(lambda x: 0)
        .mean()
    )
    assert nan_agg_ds.count() == 1
    assert nan_agg_ds.sort(key=lambda r: r[0]).take(1) == [(0, None)]

    # Test built-in global mean aggregation
    assert ray.data.from_items(xs).repartition(num_parts).mean() == 49.5
    # Test empty datastream
    assert ray.data.range(10).filter(lambda r: r > 10).mean() is None

    # Test built-in global mean aggregation with nans
    nan_ds = ray.data.from_items(xs + [None]).repartition(num_parts)
    assert nan_ds.mean() == 49.5
    # Test ignore_nulls=False
    assert nan_ds.mean(ignore_nulls=False) is None
    # Test all nans
    nan_ds = ray.data.from_items([None] * len(xs)).repartition(num_parts)
    assert nan_ds.mean() is None


@pytest.mark.skipif(STRICT_MODE, reason="Deprecated in strict mode")
@pytest.mark.parametrize("num_parts", [1, 30])
def test_groupby_simple_std(ray_start_regular_shared, num_parts):
    # Test built-in std aggregation
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_simple_std with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)
    agg_ds = (
        ray.data.from_items(xs).repartition(num_parts).groupby(lambda x: x % 3).std()
    )
    assert agg_ds.count() == 3
    df = pd.DataFrame({"A": [x % 3 for x in xs], "B": xs})
    expected = df.groupby("A")["B"].std()
    result = agg_ds.sort(key=lambda r: r[0]).take(3)
    groups, stds = zip(*result)
    result_df = pd.DataFrame({"A": list(groups), "B": list(stds)})
    result_df = result_df.set_index("A")
    pd.testing.assert_series_equal(result_df["B"], expected)
    # ddof of 0
    agg_ds = (
        ray.data.from_items(xs)
        .repartition(num_parts)
        .groupby(lambda x: x % 3)
        .std(ddof=0)
    )
    assert agg_ds.count() == 3
    df = pd.DataFrame({"A": [x % 3 for x in xs], "B": xs})
    expected = df.groupby("A")["B"].std(ddof=0)
    result = agg_ds.sort(key=lambda r: r[0]).take(3)
    groups, stds = zip(*result)
    result_df = pd.DataFrame({"A": list(groups), "B": list(stds)})
    result_df = result_df.set_index("A")
    pd.testing.assert_series_equal(result_df["B"], expected)

    # Test built-in std aggregation with nans
    nan_grouped_ds = (
        ray.data.from_items(xs + [None])
        .repartition(num_parts)
        .groupby(lambda x: int(x or 0) % 3)
    )
    nan_agg_ds = nan_grouped_ds.std()
    assert nan_agg_ds.count() == 3
    nan_df = pd.DataFrame({"A": [x % 3 for x in xs] + [0], "B": xs + [None]})
    expected = nan_df.groupby("A")["B"].std()
    result = nan_agg_ds.sort(key=lambda r: r[0]).take(3)
    groups, stds = zip(*result)
    result_df = pd.DataFrame({"A": list(groups), "B": list(stds)})
    result_df = result_df.set_index("A")
    pd.testing.assert_series_equal(result_df["B"], expected)
    # Test ignore_nulls=False
    nan_agg_ds = nan_grouped_ds.std(ignore_nulls=False)
    assert nan_agg_ds.count() == 3
    expected = nan_df.groupby("A")["B"].std()
    expected[0] = None
    result = nan_agg_ds.sort(key=lambda r: r[0]).take(3)
    groups, stds = zip(*result)
    result_df = pd.DataFrame({"A": list(groups), "B": list(stds)})
    result_df = result_df.set_index("A")
    pd.testing.assert_series_equal(result_df["B"], expected)
    # Test all nans
    nan_agg_ds = (
        ray.data.from_items([None] * len(xs))
        .repartition(num_parts)
        .groupby(lambda x: 0)
        .std(ignore_nulls=False)
    )
    assert nan_agg_ds.count() == 1
    expected = pd.Series([None], name="B")
    expected.index.rename("A", inplace=True)
    result = nan_agg_ds.sort(key=lambda r: r[0]).take(1)
    groups, stds = zip(*result)
    result_df = pd.DataFrame({"A": list(groups), "B": list(stds)})
    result_df = result_df.set_index("A")
    pd.testing.assert_series_equal(result_df["B"], expected)

    # Test built-in global std aggregation
    assert math.isclose(
        ray.data.from_items(xs).repartition(num_parts).std(), pd.Series(xs).std()
    )
    # ddof of 0
    assert math.isclose(
        ray.data.from_items(xs).repartition(num_parts).std(ddof=0),
        pd.Series(xs).std(ddof=0),
    )

    # Test empty datastream
    assert ray.data.from_items([]).std() is None
    # Test edge cases
    assert ray.data.from_items([3]).std() == 0

    # Test built-in global std aggregation with nans
    nan_ds = ray.data.from_items(xs + [None]).repartition(num_parts)
    assert math.isclose(nan_ds.std(), pd.Series(xs).std())
    # Test ignore_nulls=False
    assert nan_ds.std(ignore_nulls=False) is None
    # Test all nans
    nan_ds = ray.data.from_items([None] * len(xs)).repartition(num_parts)
    assert nan_ds.std() is None


@pytest.mark.skipif(STRICT_MODE, reason="Deprecated in strict mode")
@pytest.mark.parametrize("num_parts", [1, 30])
def test_groupby_simple_multilambda(ray_start_regular_shared, num_parts):
    # Test built-in mean aggregation
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_simple_multilambda with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)
    agg_ds = (
        ray.data.from_items([[x, 2 * x] for x in xs])
        .repartition(num_parts)
        .groupby(lambda x: x[0] % 3)
        .mean([lambda x: x[0], lambda x: x[1]])
    )
    assert agg_ds.count() == 3
    assert agg_ds.sort(key=lambda r: r[0]).take(3) == [
        (0, 49.5, 99.0),
        (1, 49.0, 98.0),
        (2, 50.0, 100.0),
    ]
    # Test built-in global mean aggregation
    assert ray.data.from_items([[x, 2 * x] for x in xs]).repartition(num_parts).mean(
        [lambda x: x[0], lambda x: x[1]]
    ) == (49.5, 99.0)
    assert ray.data.from_items([[x, 2 * x] for x in range(10)]).filter(
        lambda r: r[0] > 10
    ).mean([lambda x: x[0], lambda x: x[1]]) == (None, None)


@pytest.mark.skipif(STRICT_MODE, reason="Deprecated in strict mode")
@pytest.mark.parametrize("num_parts", [1, 30])
def test_groupby_simple_multi_agg(ray_start_regular_shared, num_parts):
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_simple_multi_agg with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)
    df = pd.DataFrame({"A": [x % 3 for x in xs], "B": xs})
    agg_ds = (
        ray.data.from_items(xs)
        .repartition(num_parts)
        .groupby(lambda x: x % 3)
        .aggregate(
            Count(),
            Sum(),
            Min(),
            Max(),
            Mean(),
            Std(),
        )
    )
    assert agg_ds.count() == 3
    result = agg_ds.sort(key=lambda r: r[0]).take(3)
    groups, counts, sums, mins, maxs, means, stds = zip(*result)
    agg_df = pd.DataFrame(
        {
            "groups": list(groups),
            "count": list(counts),
            "sum": list(sums),
            "min": list(mins),
            "max": list(maxs),
            "mean": list(means),
            "std": list(stds),
        }
    )
    agg_df = agg_df.set_index("groups")
    df = pd.DataFrame({"groups": [x % 3 for x in xs], "B": xs})
    expected_grouped = df.groupby("groups")["B"]
    np.testing.assert_array_equal(agg_df["count"].to_numpy(), [34, 33, 33])
    for agg in ["sum", "min", "max", "mean", "std"]:
        result = agg_df[agg].to_numpy()
        expected = getattr(expected_grouped, agg)().to_numpy()
        if agg == "std":
            np.testing.assert_array_almost_equal(result, expected)
        else:
            np.testing.assert_array_equal(result, expected)
    # Test built-in global multi-aggregation
    result_row = (
        ray.data.from_items(xs)
        .repartition(num_parts)
        .aggregate(
            Sum(),
            Min(),
            Max(),
            Mean(),
            Std(),
        )
    )
    series = pd.Series(xs)
    for idx, agg in enumerate(["sum", "min", "max", "mean", "std"]):
        result = result_row[idx]
        expected = getattr(series, agg)()
        if agg == "std":
            assert math.isclose(result, expected)
        else:
            assert result == expected


def test_random_block_order_schema(ray_start_regular_shared):
    df = pd.DataFrame({"a": np.random.rand(10), "b": np.random.rand(10)})
    ds = ray.data.from_pandas(df).randomize_block_order()
    ds.schema().names == ["a", "b"]


def test_random_block_order(ray_start_regular_shared, restore_data_context):
    ctx = DataContext.get_current()
    ctx.execution_options.preserve_order = True

    # Test BlockList.randomize_block_order.
    ds = ray.data.range(12).repartition(4)
    ds = ds.randomize_block_order(seed=0)

    results = ds.take()
    expected = named_values("id", [6, 7, 8, 0, 1, 2, 3, 4, 5, 9, 10, 11])
    assert results == expected

    # Test LazyBlockList.randomize_block_order.
    context = DataContext.get_current()
    try:
        original_optimize_fuse_read_stages = context.optimize_fuse_read_stages
        context.optimize_fuse_read_stages = False

        lazy_blocklist_ds = ray.data.range(12, parallelism=4)
        lazy_blocklist_ds = lazy_blocklist_ds.randomize_block_order(seed=0)
        lazy_blocklist_results = lazy_blocklist_ds.take()
        lazy_blocklist_expected = named_values(
            "id", [6, 7, 8, 0, 1, 2, 3, 4, 5, 9, 10, 11]
        )
        assert lazy_blocklist_results == lazy_blocklist_expected
    finally:
        context.optimize_fuse_read_stages = original_optimize_fuse_read_stages


# NOTE: All tests above share a Ray cluster, while the tests below do not. These
# tests should only be carefully reordered to retain this invariant!


@pytest.mark.parametrize("pipelined", [False, True])
def test_random_shuffle(shutdown_only, pipelined, use_push_based_shuffle):
    def range(n, parallelism=200):
        ds = ray.data.range(n, parallelism=parallelism)
        if pipelined:
            pipe = ds.repeat(2)
            pipe.random_shuffle = pipe.random_shuffle_each_window
            return pipe
        else:
            return ds

    r1 = range(100).random_shuffle().take(999)
    r2 = range(100).random_shuffle().take(999)
    assert r1 != r2, (r1, r2)

    r1 = range(100, parallelism=1).random_shuffle().take(999)
    r2 = range(100, parallelism=1).random_shuffle().take(999)
    assert r1 != r2, (r1, r2)

    # TODO(swang): fix this
    if not use_push_based_shuffle:
        if not pipelined:
            assert range(100).random_shuffle(num_blocks=1).num_blocks() == 1
        r1 = range(100).random_shuffle(num_blocks=1).take(999)
        r2 = range(100).random_shuffle(num_blocks=1).take(999)
        assert r1 != r2, (r1, r2)

    r0 = range(100, parallelism=5).take(999)
    r1 = range(100, parallelism=5).random_shuffle(seed=0).take(999)
    r2 = range(100, parallelism=5).random_shuffle(seed=0).take(999)
    r3 = range(100, parallelism=5).random_shuffle(seed=12345).take(999)
    assert r1 == r2, (r1, r2)
    assert r1 != r0, (r1, r0)
    assert r1 != r3, (r1, r3)

    r0 = ray.data.range(100, parallelism=5).take(999)
    r1 = ray.data.range(100, parallelism=5).random_shuffle(seed=0).take(999)
    r2 = ray.data.range(100, parallelism=5).random_shuffle(seed=0).take(999)
    assert r1 == r2, (r1, r2)
    assert r1 != r0, (r1, r0)

    # Test move.
    ds = range(100, parallelism=2)
    r1 = ds.random_shuffle().take(999)
    if pipelined:
        with pytest.raises(RuntimeError):
            ds = ds.map(lambda x: x).take(999)
    else:
        ds = ds.map(lambda x: x).take(999)
    r2 = range(100).random_shuffle().take(999)
    assert r1 != r2, (r1, r2)

    # Test empty datastream.
    ds = ray.data.from_items([])
    r1 = ds.random_shuffle()
    assert r1.count() == 0
    assert r1.take() == ds.take()


def test_random_shuffle_check_random(shutdown_only):
    # Rows from the same input should not be contiguous in the final output.
    num_files = 10
    num_rows = 100
    items = [i for i in range(num_files) for _ in range(num_rows)]
    ds = ray.data.from_items(items, parallelism=num_files)
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
    ds = ray.data.from_items(items, parallelism=num_files)
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


def test_random_shuffle_with_custom_resource(ray_start_cluster):
    cluster = ray_start_cluster
    # Create two nodes which have different custom resources.
    cluster.add_node(
        resources={"foo": 100},
        num_cpus=1,
    )
    cluster.add_node(resources={"bar": 100}, num_cpus=1)

    ray.init(cluster.address)

    # Run datastream in "bar" nodes.
    ds = ray.data.read_parquet(
        "example://parquet_images_mini",
        parallelism=2,
        ray_remote_args={"resources": {"bar": 1}},
    )
    ds = ds.random_shuffle(resources={"bar": 1}).materialize()
    assert "1 nodes used" in ds.stats()
    assert "2 nodes used" not in ds.stats()


def test_random_shuffle_spread(ray_start_cluster, use_push_based_shuffle):
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

    ds = ray.data.range(100, parallelism=2).random_shuffle()
    blocks = ds.get_internal_block_refs()
    ray.wait(blocks, num_returns=len(blocks), fetch_local=False)
    location_data = ray.experimental.get_object_locations(blocks)
    locations = []
    for block in blocks:
        locations.extend(location_data[block]["node_ids"])
    assert "2 nodes used" in ds.stats()

    if not use_push_based_shuffle:
        # We don't check this for push-based shuffle since it will try to
        # colocate reduce tasks to improve locality.
        assert set(locations) == {node1_id, node2_id}


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
