import itertools
import random
import time
from typing import Optional

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

import ray
from packaging.version import parse as parse_version
from ray._private.arrow_utils import get_pyarrow_version
from ray.data._internal.arrow_ops.transform_pyarrow import (
    combine_chunks,
    MIN_PYARROW_VERSION_TYPE_PROMOTION,
)
from ray.data._internal.planner.exchange.sort_task_spec import SortKey
from ray.data._internal.table_block import TableBlockAccessor
from ray.data._internal.util import is_nan
from ray.data._internal.execution.interfaces.ref_bundle import (
    _ref_bundles_iterator_to_block_refs_list,
)
from ray.data.aggregate import AggregateFn, Count, Max, Mean, Min, Quantile, Std, Sum
from ray.data.context import DataContext
from ray.data.block import BlockAccessor
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.util import named_values
from ray.tests.conftest import *  # noqa

RANDOM_SEED = 123


def test_empty_shuffle(ray_start_regular_shared_2_cpus):
    ds = ray.data.range(100, override_num_blocks=100)
    ds = ds.filter(lambda x: x)
    ds = ds.map_batches(lambda x: x)
    ds = ds.random_shuffle()  # Would prev. crash with AssertionError: pyarrow.Table.
    ds.show()


def test_repartition_shuffle(ray_start_regular_shared_2_cpus):
    ds = ray.data.range(20, override_num_blocks=10)
    assert ds._plan.initial_num_blocks() == 10
    assert ds.sum() == 190
    assert ds._block_num_rows() == [2] * 10

    ds2 = ds.repartition(5, shuffle=True)
    assert ds2._plan.initial_num_blocks() == 5
    assert ds2.sum() == 190
    assert ds2._block_num_rows() == [10, 10, 0, 0, 0]

    ds3 = ds2.repartition(20, shuffle=True)
    assert ds3._plan.initial_num_blocks() == 20
    assert ds3.sum() == 190
    assert ds3._block_num_rows() == [2] * 10 + [0] * 10

    large = ray.data.range(10000, override_num_blocks=10)
    large = large.repartition(20, shuffle=True)
    assert large._block_num_rows() == [500] * 20


def test_repartition_noshuffle(ray_start_regular_shared_2_cpus):
    ds = ray.data.range(20, override_num_blocks=10)
    assert ds._plan.initial_num_blocks() == 10
    assert ds.sum() == 190
    assert ds._block_num_rows() == [2] * 10

    ds2 = ds.repartition(5, shuffle=False)
    assert ds2._plan.initial_num_blocks() == 5
    assert ds2.sum() == 190
    assert ds2._block_num_rows() == [4, 4, 4, 4, 4]

    ds3 = ds2.repartition(20, shuffle=False)
    assert ds3._plan.initial_num_blocks() == 20
    assert ds3.sum() == 190
    assert ds3._block_num_rows() == [1] * 20

    # Test num_partitions > num_rows
    ds4 = ds.repartition(40, shuffle=False)
    assert ds4._plan.initial_num_blocks() == 40

    assert ds4.sum() == 190
    assert ds4._block_num_rows() == [1] * 20 + [0] * 20

    ds5 = ray.data.range(22).repartition(4)
    assert ds5._plan.initial_num_blocks() == 4
    assert ds5._block_num_rows() == [5, 6, 5, 6]

    large = ray.data.range(10000, override_num_blocks=10)
    large = large.repartition(20)
    assert large._block_num_rows() == [500] * 20


def test_repartition_shuffle_arrow(ray_start_regular_shared_2_cpus):
    ds = ray.data.range(20, override_num_blocks=10)
    assert ds._plan.initial_num_blocks() == 10
    assert ds.count() == 20
    assert ds._block_num_rows() == [2] * 10

    ds2 = ds.repartition(5, shuffle=True)
    assert ds2._plan.initial_num_blocks() == 5
    assert ds2.count() == 20
    assert ds2._block_num_rows() == [10, 10, 0, 0, 0]

    ds3 = ds2.repartition(20, shuffle=True)
    assert ds3._plan.initial_num_blocks() == 20
    assert ds3.count() == 20
    assert ds3._block_num_rows() == [2] * 10 + [0] * 10

    large = ray.data.range(10000, override_num_blocks=10)
    large = large.repartition(20, shuffle=True)
    assert large._block_num_rows() == [500] * 20


@pytest.mark.parametrize(
    "total_rows,target_num_rows_per_block",
    [
        (128, 1),
        (128, 2),
        (128, 4),
        (128, 8),
        (128, 128),
    ],
)
def test_repartition_target_num_rows_per_block(
    ray_start_regular_shared_2_cpus,
    total_rows,
    target_num_rows_per_block,
):
    ds = ray.data.range(total_rows).repartition(
        target_num_rows_per_block=target_num_rows_per_block,
    )
    rows_count = 0
    all_data = []
    for ref_bundle in ds.iter_internal_ref_bundles():
        block, block_metadata = (
            ray.get(ref_bundle.blocks[0][0]),
            ref_bundle.blocks[0][1],
        )
        assert block_metadata.num_rows <= target_num_rows_per_block
        rows_count += block_metadata.num_rows
        block_data = (
            BlockAccessor.for_block(block).to_pandas().to_dict(orient="records")
        )
        all_data.extend(block_data)

    assert rows_count == total_rows

    # Verify total rows match
    assert rows_count == total_rows

    # Verify data consistency
    all_values = [row["id"] for row in all_data]
    assert sorted(all_values) == list(range(total_rows))


@pytest.mark.parametrize(
    "num_blocks, target_num_rows_per_block, shuffle, expected_exception_msg",
    [
        (
            4,
            10,
            False,
            "Only one of `num_blocks` or `target_num_rows_per_block` must be set, but not both.",
        ),
        (
            None,
            None,
            False,
            "Either `num_blocks` or `target_num_rows_per_block` must be set",
        ),
        (
            None,
            10,
            True,
            "`shuffle` must be False when `target_num_rows_per_block` is set.",
        ),
    ],
)
def test_repartition_invalid_inputs(
    ray_start_regular_shared_2_cpus,
    num_blocks,
    target_num_rows_per_block,
    shuffle,
    expected_exception_msg,
):
    with pytest.raises(ValueError, match=expected_exception_msg):
        ray.data.range(10).repartition(
            num_blocks=num_blocks,
            target_num_rows_per_block=target_num_rows_per_block,
            shuffle=shuffle,
        )


def test_unique(ray_start_regular_shared_2_cpus):
    ds = ray.data.from_items([3, 2, 3, 1, 2, 3])
    assert set(ds.unique("item")) == {1, 2, 3}

    ds = ray.data.from_items(
        [
            {"a": 1, "b": 1},
            {"a": 1, "b": 2},
        ]
    )
    assert set(ds.unique("a")) == {1}


@pytest.mark.parametrize("batch_format", ["pandas", "pyarrow"])
def test_unique_with_nulls(ray_start_regular_shared_2_cpus, batch_format):
    ds = ray.data.from_items([3, 2, 3, 1, 2, 3, None])
    assert set(ds.unique("item")) == {1, 2, 3, None}
    assert len(ds.unique("item")) == 4

    ds = ray.data.from_items(
        [
            {"a": 1, "b": 1},
            {"a": 1, "b": 2},
            {"a": 1, "b": None},
            {"a": None, "b": 3},
            {"a": None, "b": 4},
        ]
    )
    assert set(ds.unique("a")) == {1, None}
    assert len(ds.unique("a")) == 2
    assert set(ds.unique("b")) == {1, 2, 3, 4, None}
    assert len(ds.unique("b")) == 5

    # Check with 3 columns
    df = pd.DataFrame(
        {
            "col1": [1, 2, None, 3, None, 3, 2],
            "col2": [None, 2, 2, 3, None, 3, 2],
            "col3": [1, None, 2, None, None, None, 2],
        }
    )
    # df["col"].unique() works fine, as expected
    ds2 = ray.data.from_pandas(df)
    ds2 = ds2.map_batches(lambda x: x, batch_format=batch_format)
    assert set(ds2.unique("col1")) == {1, 2, 3, None}
    assert len(ds2.unique("col1")) == 4
    assert set(ds2.unique("col2")) == {2, 3, None}
    assert len(ds2.unique("col2")) == 3
    assert set(ds2.unique("col3")) == {1, 2, None}
    assert len(ds2.unique("col3")) == 3

    # Check with 3 columns and different dtypes
    df = pd.DataFrame(
        {
            "col1": [1, 2, None, 3, None, 3, 2],
            "col2": [None, 2, 2, 3, None, 3, 2],
            "col3": [1, None, 2, None, None, None, 2],
        }
    )
    df["col1"] = df["col1"].astype("Int64")
    df["col2"] = df["col2"].astype("Float64")
    df["col3"] = df["col3"].astype("string")
    ds3 = ray.data.from_pandas(df)
    ds3 = ds3.map_batches(lambda x: x, batch_format=batch_format)
    assert set(ds3.unique("col1")) == {1, 2, 3, None}
    assert len(ds3.unique("col1")) == 4
    assert set(ds3.unique("col2")) == {2, 3, None}
    assert len(ds3.unique("col2")) == 3
    assert set(ds3.unique("col3")) == {"1.0", "2.0", None}
    assert len(ds3.unique("col3")) == 3


def test_grouped_dataset_repr(ray_start_regular_shared_2_cpus):
    ds = ray.data.from_items([{"key": "spam"}, {"key": "ham"}, {"key": "spam"}])
    assert repr(ds.groupby("key")) == f"GroupedData(dataset={ds!r}, key='key')"


def test_groupby_arrow(ray_start_regular_shared_2_cpus, configure_shuffle_method):
    # Test empty dataset.
    agg_ds = ray.data.range(10).filter(lambda r: r["id"] > 10).groupby("value").count()
    assert agg_ds.count() == 0


def test_groupby_none(ray_start_regular_shared_2_cpus, configure_shuffle_method):
    ds = ray.data.range(10)
    assert ds.groupby(None).min().take_all() == [{"min(id)": 0}]
    assert ds.groupby(None).max().take_all() == [{"max(id)": 9}]


def test_groupby_errors(ray_start_regular_shared_2_cpus):
    ds = ray.data.range(100)
    ds.groupby(None).count().show()  # OK
    with pytest.raises(ValueError):
        ds.groupby(lambda x: x % 2).count().show()
    with pytest.raises(ValueError):
        ds.groupby("foo").count().show()


def test_map_groups_with_gpus(shutdown_only, configure_shuffle_method):
    ray.shutdown()
    ray.init(num_gpus=1)

    rows = (
        ray.data.range(1).groupby("id").map_groups(lambda x: x, num_gpus=1).take_all()
    )

    assert rows == [{"id": 0}]


def test_map_groups_with_actors(
    ray_start_regular_shared_2_cpus, configure_shuffle_method
):
    class Identity:
        def __call__(self, batch):
            return batch

    rows = (
        ray.data.range(1).groupby("id").map_groups(Identity, concurrency=1).take_all()
    )

    assert rows == [{"id": 0}]


def test_map_groups_with_actors_and_args(
    ray_start_regular_shared_2_cpus, configure_shuffle_method
):
    class Fn:
        def __init__(self, x: int, y: Optional[int] = None):
            self.x = x
            self.y = y

        def __call__(self, batch, q: int, r: Optional[int] = None):
            return {"x": [self.x], "y": [self.y], "q": [q], "r": [r]}

    rows = (
        ray.data.range(1)
        .groupby("id")
        .map_groups(
            Fn,
            concurrency=1,
            fn_constructor_args=[0],
            fn_constructor_kwargs={"y": 1},
            fn_args=[2],
            fn_kwargs={"r": 3},
        )
        .take_all()
    )

    assert rows == [{"x": 0, "y": 1, "q": 2, "r": 3}]


def test_groupby_large_udf_returns(
    ray_start_regular_shared_2_cpus, configure_shuffle_method
):
    # Test for https://github.com/ray-project/ray/issues/44861.

    # Each UDF return is 128 MiB. If Ray Data doesn't incrementally yield outputs, the
    # combined output size is 128 MiB * 1024 = 128 GiB and Arrow errors.
    def create_large_data(group):
        return {"item": np.zeros((1, 128 * 1024 * 1024), dtype=np.uint8)}

    ds = (
        ray.data.range(1024, override_num_blocks=1)
        .groupby(key="id")
        .map_groups(create_large_data)
    )
    ds.take(1)


@pytest.mark.parametrize("keys", ["A", ["A", "B"]])
def test_agg_inputs(ray_start_regular_shared_2_cpus, keys, configure_shuffle_method):
    xs = list(range(100))
    ds = ray.data.from_items([{"A": (x % 3), "B": x, "C": (x % 2)} for x in xs])

    def check_init(k):
        if len(keys) == 2:
            assert isinstance(k, tuple), k
            assert len(k) == 2
        elif len(keys) == 1:
            assert isinstance(k, int)
        return 1

    def check_finalize(v):
        assert v == 1

    def check_accumulate_merge(a, r):
        assert a == 1
        if isinstance(r, int):
            return 1
        elif len(r) == 3:
            assert all(x in r for x in ["A", "B", "C"])
        else:
            assert False, r
        return 1

    output = ds.groupby(keys).aggregate(
        AggregateFn(
            init=check_init,
            accumulate_row=check_accumulate_merge,
            merge=check_accumulate_merge,
            finalize=check_finalize,
            name="foo",
        )
    )
    output.take_all()


def test_agg_errors(ray_start_regular_shared_2_cpus, configure_shuffle_method):
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
def test_groupby_agg_name_conflict(
    ray_start_regular_shared_2_cpus, num_parts, configure_shuffle_method
):
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


@pytest.mark.parametrize("ds_format", ["pyarrow", "numpy", "pandas"])
def test_groupby_nans(
    ray_start_regular_shared_2_cpus, ds_format, configure_shuffle_method
):
    ds = ray.data.from_items(
        [
            1.0,
            1.0,
            2.0,
            np.nan,
            np.nan,
        ]
    )
    ds = ds.map_batches(lambda x: x, batch_format=ds_format)
    ds = ds.groupby("item").count()

    # NOTE: Hash-based shuffling will convert the block to Arrow, which
    #       in turn convert NaNs into Nones
    ds = ds.filter(lambda v: v["item"] is None or is_nan(v["item"]))

    result = ds.take_all()
    assert result[0]["count()"] == 2


@pytest.mark.parametrize("num_parts", [1, 30])
@pytest.mark.parametrize("ds_format", ["pyarrow", "pandas"])
def test_groupby_tabular_count(
    ray_start_regular_shared_2_cpus,
    ds_format,
    num_parts,
    configure_shuffle_method,
):
    # Test built-in count aggregation
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_arrow_count with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)

    ds = ray.data.from_items([{"A": (x % 3), "B": x} for x in xs]).repartition(
        num_parts
    )

    ds = ds.map_batches(lambda x: x, batch_size=None, batch_format=ds_format)

    agg_ds = ds.groupby("A").count()
    assert agg_ds.count() == 3
    assert list(agg_ds.sort("A").iter_rows()) == [
        {"A": 0, "count()": 34},
        {"A": 1, "count()": 33},
        {"A": 2, "count()": 33},
    ]


@pytest.mark.parametrize("num_parts", [1, 30])
@pytest.mark.parametrize("ds_format", ["pyarrow", "pandas"])
def test_groupby_multiple_keys_tabular_count(
    ray_start_regular_shared_2_cpus,
    ds_format,
    num_parts,
    configure_shuffle_method,
):
    # Test built-in count aggregation
    print(f"Seeding RNG for test_groupby_arrow_count with: {RANDOM_SEED}")
    random.seed(RANDOM_SEED)
    xs = list(range(100))
    random.shuffle(xs)

    ds = ray.data.from_items([{"A": (x % 2), "B": (x % 3)} for x in xs]).repartition(
        num_parts
    )
    ds = ds.map_batches(lambda x: x, batch_size=None, batch_format=ds_format)

    agg_ds = ds.groupby(["A", "B"]).count()
    assert agg_ds.count() == 6
    assert list(agg_ds.sort(["A", "B"]).iter_rows()) == [
        {"A": 0, "B": 0, "count()": 17},
        {"A": 0, "B": 1, "count()": 16},
        {"A": 0, "B": 2, "count()": 17},
        {"A": 1, "B": 0, "count()": 17},
        {"A": 1, "B": 1, "count()": 17},
        {"A": 1, "B": 2, "count()": 16},
    ]


@pytest.mark.parametrize("num_parts", [1, 30])
@pytest.mark.parametrize("ds_format", ["pyarrow", "pandas"])
def test_groupby_tabular_sum(
    ray_start_regular_shared_2_cpus,
    ds_format,
    num_parts,
    configure_shuffle_method,
):
    # Test built-in sum aggregation
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_tabular_sum with: {seed}")
    random.seed(seed)
    xs = list(range(100))
    random.shuffle(xs)

    def _to_batch_format(ds):
        return ds.map_batches(lambda x: x, batch_size=None, batch_format=ds_format)

    ds = ray.data.from_items([{"A": (x % 3), "B": x} for x in xs]).repartition(
        num_parts
    )
    ds = _to_batch_format(ds)

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
    ds = _to_batch_format(ds)
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
    ds = _to_batch_format(ds)
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
def test_global_tabular_sum(
    ray_start_regular_shared_2_cpus, ds_format, num_parts, configure_shuffle_method
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


@pytest.mark.parametrize("num_parts", [1, 30])
@pytest.mark.parametrize("ds_format", ["arrow", "pandas"])
def test_groupby_tabular_min(
    ray_start_regular_shared_2_cpus, ds_format, num_parts, configure_shuffle_method
):
    # NOTE: Do not change the seed
    seed = int(1739959110)

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
        [{"A": (x % 3), "B": x} for x in xs]
        + [{"A": 0, "B": None}, {"A": 3, "B": None}]
    ).repartition(num_parts)

    if ds_format == "pandas":
        ds = _to_pandas(ds)

    nan_grouped_ds = ds.groupby("A")
    nan_agg_ds = nan_grouped_ds.min("B")

    pd.testing.assert_frame_equal(
        nan_agg_ds.sort("A").to_pandas(),
        pd.DataFrame(
            {
                "A": [0, 1, 2, 3],
                "min(B)": [0, 1, 2, np.nan],
            }
        ),
        # NOTE: We're disabling the check due to lossy conversion from
        #       Pandas to Arrow when all of the values in the partition
        #       are nans/Nones
        check_dtype=False,
    )

    # Test ignore_nulls=False
    nan_agg_ds = nan_grouped_ds.min("B", ignore_nulls=False)

    pd.testing.assert_frame_equal(
        nan_agg_ds.sort("A").to_pandas(),
        pd.DataFrame(
            {
                "A": [0, 1, 2, 3],
                "min(B)": [np.nan, 1, 2, np.nan],
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
def test_groupby_tabular_max(
    ray_start_regular_shared_2_cpus, ds_format, num_parts, configure_shuffle_method
):
    # Test built-in max aggregation
    random.seed(1738727165)
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
@pytest.mark.parametrize("ds_format", ["pyarrow", "pandas"])
def test_groupby_tabular_mean(
    ray_start_regular_shared_2_cpus, ds_format, num_parts, configure_shuffle_method
):
    # Test built-in mean aggregation
    seed = int(1739950448)

    random.seed(seed)

    xs = list(range(100))
    random.shuffle(xs)

    def _convert_to_format(ds):
        return ds.map_batches(lambda x: x, batch_size=None, batch_format=ds_format)

    ds = ray.data.from_items([{"A": (x % 3), "B": x} for x in xs]).repartition(
        num_parts
    )

    ds = _convert_to_format(ds)

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

    ds = _convert_to_format(ds)

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

    ds = _convert_to_format(ds)

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
@pytest.mark.parametrize("ds_format", ["pyarrow", "pandas"])
def test_groupby_tabular_std(
    ray_start_regular_shared_2_cpus, ds_format, num_parts, configure_shuffle_method
):
    # Test built-in std aggregation
    seed = int(time.time())
    print(f"Seeding RNG for test_groupby_tabular_std with: {seed}")
    random.seed(seed)

    xs = list(range(100))
    random.shuffle(xs)

    def _convert_to_format(ds):
        return ds.map_batches(lambda x: x, batch_size=None, batch_format="pyarrow")

    df = pd.DataFrame({"A": [x % 3 for x in xs], "B": xs})
    ds = ray.data.from_pandas(df).repartition(num_parts)

    ds = _convert_to_format(ds)

    agg_ds = ds.groupby("A").std("B")
    assert agg_ds.count() == 3

    result = agg_ds.to_pandas().sort_values("A")["std(B)"].to_numpy()
    expected = df.groupby("A")["B"].std().to_numpy()

    np.testing.assert_array_almost_equal(result, expected)

    # ddof of 0
    ds = ray.data.from_pandas(df).repartition(num_parts)
    ds = _convert_to_format(ds)

    agg_ds = ds.groupby("A").std("B", ddof=0)
    assert agg_ds.count() == 3

    result = agg_ds.to_pandas().sort_values("A")["std(B)"].to_numpy()
    expected = df.groupby("A")["B"].std(ddof=0).to_numpy()

    np.testing.assert_array_almost_equal(result, expected)

    # Test built-in std aggregation with nans
    nan_df = pd.DataFrame({"A": [x % 3 for x in xs] + [0], "B": xs + [None]})
    ds = ray.data.from_pandas(nan_df).repartition(num_parts)

    ds = _convert_to_format(ds)

    nan_grouped_ds = ds.groupby("A")
    nan_agg_ds = nan_grouped_ds.std("B")
    assert nan_agg_ds.count() == 3

    result = nan_agg_ds.to_pandas().sort_values("A")["std(B)"].to_numpy()
    expected = nan_df.groupby("A")["B"].std().to_numpy()

    np.testing.assert_array_almost_equal(result, expected)

    # Test ignore_nulls=False
    nan_agg_ds = nan_grouped_ds.std("B", ignore_nulls=False)
    assert nan_agg_ds.count() == 3

    result = nan_agg_ds.to_pandas().sort_values("A")["std(B)"].to_numpy()
    expected = nan_df.groupby("A")["B"].std().to_numpy()

    assert result[0] is None or np.isnan(result[0])

    np.testing.assert_array_almost_equal(result[1:], expected[1:])

    # Test all nans
    nan_df = pd.DataFrame({"A": [x % 3 for x in xs], "B": [None] * len(xs)})
    ds = ray.data.from_pandas(nan_df).repartition(num_parts)

    ds = _convert_to_format(ds)

    nan_agg_ds = ds.groupby("A").std("B", ignore_nulls=False)
    assert nan_agg_ds.count() == 3

    result = nan_agg_ds.to_pandas().sort_values("A")["std(B)"].to_numpy()
    expected = pd.Series([None] * 3)

    np.testing.assert_array_equal(result, expected)


@pytest.mark.parametrize("num_parts", [1, 30])
def test_groupby_arrow_multicolumn(
    ray_start_regular_shared_2_cpus, num_parts, configure_shuffle_method
):
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


def test_groupby_agg_bad_on(ray_start_regular_shared_2_cpus, configure_shuffle_method):
    # Test bad on for groupby aggregation
    xs = list(range(100))
    df = pd.DataFrame(
        np.array([[x % 3 for x in xs], xs, [2 * x for x in xs]]).T,
        columns=["A", "B", "C"],
    )

    # Wrong type.
    with pytest.raises(Exception) as exc_info:
        ray.data.from_pandas(df).groupby("A").mean(5).materialize()

    assert "Key must be a string or a list of strings, but got 5." in str(
        exc_info.value
    )

    with pytest.raises(Exception) as exc_info:
        ray.data.from_pandas(df).groupby("A").mean([5]).materialize()

    assert "Key must be a string or a list of strings, but got 5." in str(
        exc_info.value
    )

    # Empty list.
    with pytest.raises(ValueError) as exc_info:
        ray.data.from_pandas(df).groupby("A").mean([]).materialize()

    assert "At least 1 column to aggregate on has to be provided" in str(exc_info.value)

    # Nonexistent column.
    with pytest.raises(ValueError) as exc_info:
        ray.data.from_pandas(df).groupby("A").mean("D").materialize()

    assert (
        "You specified the column 'D', but there's no such column in the dataset. The dataset has columns: ['A', 'B', 'C']"
        in str(exc_info.value)
    )

    with pytest.raises(ValueError) as exc_info:
        ray.data.from_pandas(df).groupby("A").mean(["B", "D"]).materialize()

    assert (
        "You specified the column 'D', but there's no such column in the dataset. The dataset has columns: ['A', 'B', 'C']"
        in str(exc_info.value)
    )

    # Columns for simple Dataset.
    with pytest.raises(ValueError) as exc_info:
        ray.data.from_items(xs).groupby(lambda x: x % 3 == 0).mean("A").materialize()

    assert "Key must be a string or a list of strings, but got <function" in str(
        exc_info.value
    )

    # Test bad on for global aggregation
    # Wrong type.
    with pytest.raises(Exception) as exc_info:
        ray.data.from_pandas(df).mean(5).materialize()

    assert "Key must be a string or a list of strings, but got 5." in str(
        exc_info.value
    )

    with pytest.raises(Exception) as exc_info:
        ray.data.from_pandas(df).mean([5]).materialize()

    assert "Key must be a string or a list of strings, but got 5." in str(
        exc_info.value
    )

    # Empty list.
    with pytest.raises(ValueError) as exc_info:
        ray.data.from_pandas(df).mean([]).materialize()

    assert "At least 1 column to aggregate on has to be provided" in str(exc_info.value)

    # Nonexistent column.
    with pytest.raises(ValueError) as exc_info:
        ray.data.from_pandas(df).mean("D").materialize()

    assert (
        "You specified the column 'D', but there's no such column in the dataset. The dataset has columns: ['A', 'B', 'C']"
        in str(exc_info.value)
    )

    with pytest.raises(ValueError) as exc_info:
        ray.data.from_pandas(df).mean(["B", "D"]).materialize()

    assert (
        "You specified the column 'D', but there's no such column in the dataset. The dataset has columns: ['A', 'B', 'C']"
        in str(exc_info.value)
    )

    # Columns for simple Dataset.
    with pytest.raises(ValueError) as exc_info:
        ray.data.from_items(xs).mean("A").materialize()

    assert (
        "You specified the column 'A', but there's no such column in the dataset. The dataset has columns: ['item']"
        in str(exc_info.value)
    )


@pytest.mark.parametrize("num_parts", [1, 30])
@pytest.mark.parametrize("ds_format", ["pandas", "pyarrow"])
def test_groupby_arrow_multi_agg(
    ray_start_regular_shared_2_cpus, num_parts, configure_shuffle_method, ds_format
):
    using_pyarrow = ds_format == "pyarrow"

    if using_pyarrow and get_pyarrow_version() < MIN_PYARROW_VERSION_TYPE_PROMOTION:
        pytest.skip(
            "Pyarrow < 14.0 doesn't support type promotions (hence fails "
            "promoting from int64 to double)"
        )

    # NOTE: Do not change the seed
    random.seed(1738379113)

    xs = list(range(100))
    random.shuffle(xs)
    df = pd.DataFrame({"A": [x % 3 for x in xs], "B": xs})
    agg_ds = (
        ray.data.from_pandas(df)
        .map_batches(lambda df: df, batch_size=None, batch_format=ds_format)
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

    agg_df = agg_ds.to_pandas().sort_values(by="A").reset_index(drop=True)

    grouped_df = df.groupby("A", as_index=False).agg(
        {
            "B": ["count", "sum", "min", "max", "mean", "std", "quantile"],
        }
    )

    grouped_df.columns = [
        "A",
        "count()",
        "sum(B)",
        "min(B)",
        "max(B)",
        "mean(B)",
        "std(B)",
        "quantile(B)",
    ]

    expected_df = grouped_df.sort_values(by="A").reset_index(drop=True)

    print(f"Expected: {expected_df}")
    print(f"Result: {agg_df}")

    pd.testing.assert_frame_equal(expected_df, agg_df)

    # Test built-in global std aggregation
    df = pd.DataFrame({"A": xs})

    result_row = (
        ray.data.from_pandas(df)
        .map_batches(lambda df: df, batch_size=None, batch_format=ds_format)
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

    expected_row = {
        f"{agg}(A)": getattr(df["A"], agg)()
        for agg in ["sum", "min", "max", "mean", "std", "quantile"]
    }

    def _round_to_14_digits(row):
        return {
            # NOTE: Pandas and Arrow diverge on 14th digit (due to different formula
            #       used with diverging FP numerical stability), hence we round it up
            k: round(v, 14)
            for k, v in row.items()
        }

    assert _round_to_14_digits(expected_row) == _round_to_14_digits(result_row)


@pytest.mark.parametrize("num_parts", [1, 30])
@pytest.mark.parametrize("ds_format", ["pandas", "pyarrow"])
@pytest.mark.parametrize("ignore_nulls", [True, False])
def test_groupby_multi_agg_with_nans(
    ray_start_regular_shared_2_cpus,
    num_parts,
    configure_shuffle_method,
    ds_format,
    ignore_nulls,
):
    using_pyarrow = ds_format == "pyarrow"

    if using_pyarrow and get_pyarrow_version() < MIN_PYARROW_VERSION_TYPE_PROMOTION:
        pytest.skip(
            "Pyarrow < 14.0 doesn't support type promotions (hence fails "
            "promoting from int64 to double)"
        )

    # NOTE: Do not change the seed
    random.seed(1738379113)

    xs = list(range(100))
    random.shuffle(xs)

    df = pd.DataFrame(
        {
            "A": [x % 3 for x in xs] + [(np.nan if x % 2 == 0 else None) for x in xs],
            "B": xs + [(x if x % 2 == 1 else np.nan) for x in xs],
        }
    )

    agg_ds = (
        ray.data.from_pandas(df)
        .map_batches(lambda df: df, batch_size=None, batch_format=ds_format)
        .repartition(num_parts)
        .groupby("A")
        .aggregate(
            Sum("B", alias_name="sum_b", ignore_nulls=ignore_nulls),
            Min("B", alias_name="min_b", ignore_nulls=ignore_nulls),
            Max("B", alias_name="max_b", ignore_nulls=ignore_nulls),
            Mean("B", alias_name="mean_b", ignore_nulls=ignore_nulls),
            Std("B", alias_name="std_b", ignore_nulls=ignore_nulls),
            Quantile("B", alias_name="quantile_b", ignore_nulls=ignore_nulls),
        )
    )

    agg_df = agg_ds.to_pandas().sort_values(by="A").reset_index(drop=True)

    grouped_df = df.groupby("A", as_index=False, dropna=False).agg(
        {
            "B": [
                ("sum", lambda s: s.sum(skipna=ignore_nulls)),
                ("min", lambda s: s.min(skipna=ignore_nulls)),
                ("max", lambda s: s.max(skipna=ignore_nulls)),
                ("mean", lambda s: s.mean(skipna=ignore_nulls)),
                ("std", lambda s: s.std(skipna=ignore_nulls)),
                (
                    "quantile",
                    lambda s: s.quantile() if ignore_nulls or not s.hasnans else np.nan,
                ),
            ]
        },
    )

    grouped_df.columns = [
        "A",
        "sum_b",
        "min_b",
        "max_b",
        "mean_b",
        "std_b",
        "quantile_b",
    ]

    expected_df = grouped_df.sort_values(by="A").reset_index(drop=True)

    print(f"Expected: {expected_df}")
    print(f"Result: {agg_df}")

    pd.testing.assert_frame_equal(expected_df, agg_df)

    # Test built-in global std aggregation
    df = pd.DataFrame({"A": xs})

    result_row = (
        ray.data.from_pandas(df)
        .map_batches(lambda df: df, batch_size=None, batch_format=ds_format)
        .repartition(num_parts)
        .aggregate(
            Sum("A", alias_name="sum_a", ignore_nulls=ignore_nulls),
            Min("A", alias_name="min_a", ignore_nulls=ignore_nulls),
            Max("A", alias_name="max_a", ignore_nulls=ignore_nulls),
            Mean("A", alias_name="mean_a", ignore_nulls=ignore_nulls),
            Std("A", alias_name="std_a", ignore_nulls=ignore_nulls),
            Quantile("A", alias_name="quantile_a", ignore_nulls=ignore_nulls),
        )
    )

    expected_row = {
        f"{agg}_a": getattr(df["A"], agg)()
        for agg in ["sum", "min", "max", "mean", "std", "quantile"]
    }

    def _round_to_14_digits(row):
        return {
            # NOTE: Pandas and Arrow diverge on 14th digit (due to different formula
            #       used with diverging FP numerical stability), hence we round it up
            k: round(v, 14)
            for k, v in row.items()
        }

    assert _round_to_14_digits(expected_row) == _round_to_14_digits(result_row)


@pytest.mark.parametrize("ds_format", ["pyarrow", "pandas"])
@pytest.mark.parametrize("ignore_nulls", [True, False])
@pytest.mark.parametrize("null", [None, np.nan])
def test_groupby_multi_agg_with_nans_v2(
    ray_start_regular_shared_2_cpus,
    configure_shuffle_method,
    ds_format,
    ignore_nulls,
    null,
):
    # NOTE: This test verifies that combining is an properly
    #       associative operation by combining all possible permutations
    #       of partially aggregated blocks

    source = pd.DataFrame(
        {
            "A": [0, 1, 2, 3],
            "B": [0, 1, 2, null],
        }
    )

    aggs = [
        Sum("B", alias_name="sum_b", ignore_nulls=ignore_nulls),
        Min("B", alias_name="min_b", ignore_nulls=ignore_nulls),
        Max("B", alias_name="max_b", ignore_nulls=ignore_nulls),
        Mean("B", alias_name="mean_b", ignore_nulls=ignore_nulls),
        Std("B", alias_name="std_b", ignore_nulls=ignore_nulls),
        Quantile("B", alias_name="quantile_b", ignore_nulls=ignore_nulls),
    ]

    # Step 0: Prepare expected output (using Pandas)
    grouped_df = source.groupby("A", as_index=False, dropna=False).agg(
        {
            "B": [
                ("sum", lambda s: s.sum(skipna=ignore_nulls, min_count=1)),
                ("min", lambda s: s.min(skipna=ignore_nulls)),
                ("max", lambda s: s.max(skipna=ignore_nulls)),
                ("mean", lambda s: s.mean(skipna=ignore_nulls)),
                ("std", lambda s: s.std(skipna=ignore_nulls)),
                (
                    "quantile",
                    lambda s: s.quantile() if ignore_nulls or not s.hasnans else np.nan,
                ),
            ]
        },
    )

    grouped_df.columns = [
        "A",
        "sum_b",
        "min_b",
        "max_b",
        "mean_b",
        "std_b",
        "quantile_b",
    ]

    expected_df = grouped_df.sort_values(by="A").reset_index(drop=True)

    # Step 1: Split individual rows into standalone blocks, then apply
    #         aggregations to it
    group_by_key = SortKey("A")
    aggregated_sub_blocks = []

    for i in range(len(source)):
        slice_ = BlockAccessor.for_block(source).slice(i, i + 1)
        if ds_format == "pyarrow":
            b = pa.Table.from_pydict(slice_)
        elif ds_format == "pandas":
            b = pd.DataFrame(slice_)
        else:
            raise ValueError(f"Unknown format: {ds_format}")

        aggregated_sub_blocks.append(
            BlockAccessor.for_block(b)._aggregate(group_by_key, tuple(aggs))
        )

    # Step 2: Aggregate all possible permutations of the partially aggregated
    #         blocks, assert against expected output
    for aggregated_blocks in itertools.permutations(aggregated_sub_blocks):
        cur = aggregated_blocks[0]
        for next_ in aggregated_blocks[1:]:
            cur, _ = TableBlockAccessor._combine_aggregated_blocks(
                [cur, next_], group_by_key, aggs, finalize=False
            )

        finalized_block, _ = TableBlockAccessor._combine_aggregated_blocks(
            [cur], group_by_key, aggs, finalize=True
        )

        if ds_format == "pyarrow":
            res = finalized_block.to_pandas()
        elif ds_format == "pandas":
            res = finalized_block
        else:
            raise ValueError(f"Unknown format: {ds_format}")

        res = res.sort_values(by="A").reset_index(drop=True)

        print(">>> Result: ", res)
        print(">>> Expected: ", expected_df)

        # NOTE: We currently ignore the underlying schema and assert only
        #       based on values, due to current aggregations implementations
        #       not handling types properly and consistently
        #
        # TODO assert on expected schema as well
        pd.testing.assert_frame_equal(expected_df, res, check_dtype=False)


@pytest.mark.parametrize("num_parts", [1, 2, 30])
def test_groupby_map_groups_for_none_groupkey(
    ray_start_regular_shared_2_cpus, num_parts, configure_shuffle_method
):
    ds = ray.data.from_items(list(range(100)))
    mapped = (
        ds.repartition(num_parts)
        .groupby(None)
        .map_groups(lambda x: {"out": np.array([min(x["item"]) + max(x["item"])])})
    )
    assert mapped.count() == 1
    assert mapped.take_all() == named_values("out", [99])


def test_groupby_map_groups_perf(
    ray_start_regular_shared_2_cpus, configure_shuffle_method
):
    data_list = [x % 100 for x in range(5000000)]
    ds = ray.data.from_pandas(pd.DataFrame({"A": data_list}))
    start = time.perf_counter()
    ds.groupby("A").map_groups(lambda df: df)
    end = time.perf_counter()
    # On a t3.2xlarge instance, it ran in about 5 seconds, so expecting it has to
    # finish within about 10x of that time, unless something went wrong.
    assert end - start < 60


@pytest.mark.parametrize("num_parts", [1, 2, 30])
def test_groupby_map_groups_for_pandas(
    ray_start_regular_shared_2_cpus, num_parts, configure_shuffle_method
):
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

    result = mapped.sort(["A", "C"]).to_pandas()

    pd.testing.assert_frame_equal(expected, result)


@pytest.mark.parametrize("num_parts", [1, 2, 30])
def test_groupby_map_groups_for_arrow(
    ray_start_regular_shared_2_cpus, num_parts, configure_shuffle_method
):
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

    result = mapped.sort(["A", "C"]).take_batch(batch_format="pyarrow")

    assert expected == combine_chunks(result)


def test_groupby_map_groups_for_numpy(
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

    def func(group):
        # Test output type is NumPy format.
        return {"group": group["group"] + 1, "value": group["value"] + 1}

    ds = ds.groupby("group").map_groups(func, batch_format="numpy")

    expected = pa.Table.from_pydict({"group": [2, 2, 3, 3], "value": [2, 3, 4, 5]})

    result = ds.sort(["group", "value"]).take_batch(batch_format="pyarrow")

    assert expected == result


def test_groupby_map_groups_with_different_types(
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

    def func(batch):
        # Test output type is Python list, different from input type.
        return {"group": [batch["group"][0]], "out": [min(batch["value"])]}

    ds = ds.groupby("group").map_groups(func)

    assert [x["out"] for x in ds.sort("group").take_all()] == [1, 3]


@pytest.mark.parametrize("num_parts", [1, 30])
def test_groupby_map_groups_multiple_batch_formats(
    ray_start_regular_shared_2_cpus, num_parts, configure_shuffle_method
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


def test_groupby_map_groups_extra_args(
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


def test_groupby_map_groups_with_partial():
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


def test_random_block_order_schema(ray_start_regular_shared_2_cpus):
    df = pd.DataFrame({"a": np.random.rand(10), "b": np.random.rand(10)})
    ds = ray.data.from_pandas(df).randomize_block_order()
    ds.schema().names == ["a", "b"]


def test_random_block_order(ray_start_regular_shared_2_cpus, restore_data_context):
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


def test_random_shuffle(shutdown_only, configure_shuffle_method):
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


def test_random_shuffle_check_random(shutdown_only):
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
    ray_start_cluster, configure_shuffle_method
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


def test_random_shuffle_spread(ray_start_cluster, configure_shuffle_method):
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
