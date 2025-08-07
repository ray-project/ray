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


def _sort_series_of_lists_elements(s: pd.Series):
    return s.apply(
        lambda l: list(
            # NOTE: We convert to Series to ensure the NaN elements will go last
            pd.Series(list(l)).sort_values()
        )
    )


def test_grouped_dataset_repr(
    ray_start_regular_shared_2_cpus,
    disable_fallback_to_object_extension,
    target_max_block_size_infinite_or_default,
):
    ds = ray.data.from_items([{"key": "spam"}, {"key": "ham"}, {"key": "spam"}])
    assert repr(ds.groupby("key")) == f"GroupedData(dataset={ds!r}, key='key')"


def test_groupby_arrow(
    ray_start_regular_shared_2_cpus,
    configure_shuffle_method,
    disable_fallback_to_object_extension,
    target_max_block_size_infinite_or_default,
):
    # Test empty dataset.
    agg_ds = ray.data.range(10).filter(lambda r: r["id"] > 10).groupby("value").count()
    assert agg_ds.count() == 0


def test_groupby_none(
    ray_start_regular_shared_2_cpus,
    configure_shuffle_method,
    disable_fallback_to_object_extension,
    target_max_block_size_infinite_or_default,
):
    ds = ray.data.range(10)
    assert ds.groupby(None).min().take_all() == [{"min(id)": 0}]
    assert ds.groupby(None).max().take_all() == [{"max(id)": 9}]


def test_groupby_errors(
    ray_start_regular_shared_2_cpus,
    disable_fallback_to_object_extension,
    target_max_block_size_infinite_or_default,
):
    ds = ray.data.range(100)
    ds.groupby(None).count().show()  # OK
    with pytest.raises(ValueError):
        ds.groupby(lambda x: x % 2).count().show()
    with pytest.raises(ValueError):
        ds.groupby("foo").count().show()


def test_map_groups_with_gpus(
    shutdown_only,
    configure_shuffle_method,
    disable_fallback_to_object_extension,
    target_max_block_size_infinite_or_default,
):
    ray.shutdown()
    ray.init(num_gpus=1)

    rows = (
        ray.data.range(1, override_num_blocks=1)
        .groupby("id")
        .map_groups(lambda x: x, num_gpus=1)
        .take_all()
    )

    assert rows == [{"id": 0}]


def test_map_groups_with_actors(
    ray_start_regular_shared_2_cpus,
    configure_shuffle_method,
    disable_fallback_to_object_extension,
    target_max_block_size_infinite_or_default,
):
    class Identity:
        def __call__(self, batch):
            return batch

    rows = (
        ray.data.range(1).groupby("id").map_groups(Identity, concurrency=1).take_all()
    )

    assert rows == [{"id": 0}]


def test_map_groups_with_actors_and_args(
    ray_start_regular_shared_2_cpus,
    configure_shuffle_method,
    disable_fallback_to_object_extension,
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
    ray_start_regular_shared_2_cpus,
    configure_shuffle_method,
    disable_fallback_to_object_extension,
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


@pytest.mark.parametrize("num_parts", [1, 30])
def test_groupby_agg_name_conflict(
    ray_start_regular_shared_2_cpus,
    num_parts,
    configure_shuffle_method,
    disable_fallback_to_object_extension,
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
    ray_start_regular_shared_2_cpus,
    ds_format,
    configure_shuffle_method,
    disable_fallback_to_object_extension,
    target_max_block_size_infinite_or_default,
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
    disable_fallback_to_object_extension,
    target_max_block_size_infinite_or_default,
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
    disable_fallback_to_object_extension,
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
    disable_fallback_to_object_extension,
):
    ctx = DataContext.get_current()

    if ctx.shuffle_strategy == ShuffleStrategy.HASH_SHUFFLE and ds_format == "pandas":
        pytest.skip(
            "Pandas derives integer columns with null as doubles, "
            "therefore deviating schemas for blocks containing nulls"
        )

    # Test built-in sum aggregation
    random.seed(1741752320)

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

    expected = pd.DataFrame(
        {
            "A": [0, 1, 2],
            "sum(B)": pd.Series([None, None, None], dtype="object"),
        },
    )
    result = nan_agg_ds.sort("A").to_pandas()

    print("Result: ", result)
    print("Expected: ", expected)

    pd.testing.assert_frame_equal(
        expected,
        result,
    )


@pytest.mark.parametrize("num_parts", [1, 30])
@pytest.mark.parametrize("ds_format", ["pandas", "pyarrow"])
def test_groupby_arrow_multi_agg(
    ray_start_regular_shared_2_cpus,
    num_parts,
    configure_shuffle_method,
    ds_format,
    disable_fallback_to_object_extension,
):
    using_pyarrow = (
        ds_format == "pyarrow"
        or
        # NOTE: Hash-shuffle internally converts to pyarrow
        (
            ds_format == "pandas"
            and configure_shuffle_method == ShuffleStrategy.HASH_SHUFFLE
        )
    )

    if using_pyarrow and get_pyarrow_version() < MIN_PYARROW_VERSION_TYPE_PROMOTION:
        pytest.skip(
            "Pyarrow < 14.0 doesn't support type promotions (hence fails "
            "promoting from int64 to double)"
        )

    # NOTE: Do not change the seed
    random.seed(1738379113)

    xs = list(range(-50, 50))
    random.shuffle(xs)

    df = pd.DataFrame({"A": [x % 3 for x in xs], "B": xs})

    agg_ds = (
        ray.data.from_pandas(df)
        .map_batches(lambda df: df, batch_size=None, batch_format=ds_format)
        .repartition(num_parts)
        .groupby("A")
        .aggregate(
            Count(),
            Count("B"),
            Sum("B"),
            Min("B"),
            Max("B"),
            AbsMax("B"),
            Mean("B"),
            Std("B"),
            Quantile("B"),
            Unique("B"),
        )
    )

    agg_df = agg_ds.to_pandas().sort_values(by="A").reset_index(drop=True)

    grouped_df = df.groupby("A", as_index=False).agg(
        {
            "B": [
                "count",
                "count",
                "sum",
                "min",
                "max",
                lambda x: x.abs().max(),
                "mean",
                "std",
                "quantile",
                "unique",
            ],
        }
    )

    grouped_df.columns = [
        "A",
        "count()",
        "count(B)",
        "sum(B)",
        "min(B)",
        "max(B)",
        "abs_max(B)",
        "mean(B)",
        "std(B)",
        "quantile(B)",
        "unique(B)",
    ]

    expected_df = grouped_df.sort_values(by="A").reset_index(drop=True)

    agg_df["unique(B)"] = _sort_series_of_lists_elements(agg_df["unique(B)"])
    expected_df["unique(B)"] = _sort_series_of_lists_elements(expected_df["unique(B)"])

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

    def _round_to_13_digits(row):
        return {
            # NOTE: Pandas and Arrow diverge on 14th digit (due to different formula
            #       used with diverging FP numerical stability), hence we round it up
            k: round(v, 13)
            for k, v in row.items()
        }

    print(f"Expected: {expected_row}, (rounded: {_round_to_13_digits(expected_row)})")
    print(f"Result: {result_row} (rounded: {_round_to_13_digits(result_row)})")

    assert _round_to_13_digits(expected_row) == _round_to_13_digits(result_row)


@pytest.mark.parametrize("num_parts", [1, 30])
@pytest.mark.parametrize("ds_format", ["pandas", "pyarrow"])
@pytest.mark.parametrize("ignore_nulls", [True, False])
def test_groupby_multi_agg_with_nans(
    ray_start_regular_shared_2_cpus,
    num_parts,
    configure_shuffle_method,
    ds_format,
    ignore_nulls,
    disable_fallback_to_object_extension,
):
    using_pyarrow = ds_format == "pyarrow"

    if using_pyarrow and get_pyarrow_version() < MIN_PYARROW_VERSION_TYPE_PROMOTION:
        pytest.skip(
            "Pyarrow < 14.0 doesn't support type promotions (hence fails "
            "promoting from int64 to double)"
        )

    # NOTE: Do not change the seed
    random.seed(1738379113)

    xs = list(range(-50, 50))
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
            Count("B", alias_name="count_b", ignore_nulls=ignore_nulls),
            Sum("B", alias_name="sum_b", ignore_nulls=ignore_nulls),
            Min("B", alias_name="min_b", ignore_nulls=ignore_nulls),
            Max("B", alias_name="max_b", ignore_nulls=ignore_nulls),
            AbsMax("B", alias_name="abs_max_b", ignore_nulls=ignore_nulls),
            Mean("B", alias_name="mean_b", ignore_nulls=ignore_nulls),
            Std("B", alias_name="std_b", ignore_nulls=ignore_nulls),
            Quantile("B", alias_name="quantile_b", ignore_nulls=ignore_nulls),
            Unique("B", alias_name="unique_b"),
        )
    )

    agg_df = agg_ds.to_pandas().sort_values(by="A").reset_index(drop=True)

    grouped_df = df.groupby("A", as_index=False, dropna=False).agg(
        {
            "B": [
                ("count_b", lambda s: s.count() if ignore_nulls else len(s)),
                ("sum_b", lambda s: s.sum(skipna=ignore_nulls)),
                ("min_b", lambda s: s.min(skipna=ignore_nulls)),
                ("max_b", lambda s: s.max(skipna=ignore_nulls)),
                ("abs_max_b", lambda s: s.abs().max(skipna=ignore_nulls)),
                ("mean_b", lambda s: s.mean(skipna=ignore_nulls)),
                ("std_b", lambda s: s.std(skipna=ignore_nulls)),
                (
                    "quantile_b",
                    lambda s: s.quantile() if ignore_nulls or not s.hasnans else np.nan,
                ),
                ("unique_b", "unique"),
            ]
        },
    )

    print(grouped_df)

    grouped_df.columns = [
        "A",
        "count_b",
        "sum_b",
        "min_b",
        "max_b",
        "abs_max_b",
        "mean_b",
        "std_b",
        "quantile_b",
        "unique_b",
    ]

    expected_df = grouped_df.sort_values(by="A").reset_index(drop=True)

    agg_df["unique_b"] = _sort_series_of_lists_elements(agg_df["unique_b"])
    expected_df["unique_b"] = _sort_series_of_lists_elements(expected_df["unique_b"])

    print(f"Expected: {expected_df}")
    print(f"Result: {agg_df}")

    pd.testing.assert_frame_equal(expected_df, agg_df, check_dtype=False)

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

    assert expected_row.keys() == result_row.keys()
    assert all(result_row[k] == pytest.approx(expected_row[k]) for k in expected_row)


@pytest.mark.parametrize("ds_format", ["pyarrow", "pandas"])
@pytest.mark.parametrize("ignore_nulls", [True, False])
@pytest.mark.parametrize("null", [None, np.nan])
def test_groupby_aggregations_are_associative(
    ray_start_regular_shared_2_cpus,
    configure_shuffle_method,
    ds_format,
    ignore_nulls,
    null,
    disable_fallback_to_object_extension,
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
        Count("B", alias_name="count_b", ignore_nulls=ignore_nulls),
        Sum("B", alias_name="sum_b", ignore_nulls=ignore_nulls),
        Min("B", alias_name="min_b", ignore_nulls=ignore_nulls),
        Max("B", alias_name="max_b", ignore_nulls=ignore_nulls),
        AbsMax("B", alias_name="abs_max_b", ignore_nulls=ignore_nulls),
        Mean("B", alias_name="mean_b", ignore_nulls=ignore_nulls),
        Std("B", alias_name="std_b", ignore_nulls=ignore_nulls),
        Quantile("B", alias_name="quantile_b", ignore_nulls=ignore_nulls),
        Unique("B", alias_name="unique_b"),
    ]

    # Step 0: Prepare expected output (using Pandas)
    grouped_df = source.groupby("A", as_index=False, dropna=False).agg(
        {
            "B": [
                ("count", lambda s: s.count() if ignore_nulls else len(s)),
                ("sum", lambda s: s.sum(skipna=ignore_nulls, min_count=1)),
                ("min", lambda s: s.min(skipna=ignore_nulls)),
                ("max", lambda s: s.max(skipna=ignore_nulls)),
                ("abs_max", lambda s: s.abs().max(skipna=ignore_nulls)),
                ("mean", lambda s: s.mean(skipna=ignore_nulls)),
                ("std", lambda s: s.std(skipna=ignore_nulls)),
                (
                    "quantile_b",
                    lambda s: s.quantile() if ignore_nulls or not s.hasnans else np.nan,
                ),
                ("unique", "unique"),
            ]
        },
    )

    print(grouped_df)

    grouped_df.columns = [
        "A",
        "count_b",
        "sum_b",
        "min_b",
        "max_b",
        "abs_max_b",
        "mean_b",
        "std_b",
        "quantile_b",
        "unique_b",
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
            cur, _ = BlockAccessor.for_block(cur)._combine_aggregated_blocks(
                [cur, next_], group_by_key, aggs, finalize=False
            )

        finalized_block, _ = BlockAccessor.for_block(cur)._combine_aggregated_blocks(
            [cur], group_by_key, aggs, finalize=True
        )

        # NOTE: _combine_aggregated_blocks could be producing
        #   - Arrow blocks when using vectorized or full Arrow-native aggregations
        #   - Pandas blocks if it falls back to default (OSS) impl (for ex for Arrow < 14.0)
        res = BlockAccessor.for_block(finalized_block).to_pandas()

        res = res.sort_values(by="A").reset_index(drop=True)

        res["unique_b"] = _sort_series_of_lists_elements(res["unique_b"])
        expected_df["unique_b"] = _sort_series_of_lists_elements(
            expected_df["unique_b"]
        )

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
    ray_start_regular_shared_2_cpus,
    num_parts,
    configure_shuffle_method,
    disable_fallback_to_object_extension,
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
    ray_start_regular_shared_2_cpus,
    configure_shuffle_method,
    disable_fallback_to_object_extension,
    target_max_block_size_infinite_or_default,
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
    ray_start_regular_shared_2_cpus,
    num_parts,
    configure_shuffle_method,
    disable_fallback_to_object_extension,
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
    ray_start_regular_shared_2_cpus,
    num_parts,
    configure_shuffle_method,
    disable_fallback_to_object_extension,
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

    def func(group):
        # Test output type is NumPy format.
        return {"group": group["group"] + 1, "value": group["value"] + 1}

    ds = ds.groupby("group").map_groups(func, batch_format="numpy")

    expected = pa.Table.from_pydict({"group": [2, 2, 3, 3], "value": [2, 3, 4, 5]})

    result = ds.sort(["group", "value"]).take_batch(batch_format="pyarrow")

    assert expected == result


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
    ray_start_regular_shared_2_cpus,
    configure_shuffle_method,
    target_max_block_size_infinite_or_default,
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
    target_max_block_size_infinite_or_default,
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
