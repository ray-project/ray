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


@pytest.mark.parametrize("num_parts", [1, 30])
@pytest.mark.parametrize("ds_format", ["pyarrow", "pandas"])
def test_groupby_tabular_std(
    ray_start_regular_shared_2_cpus,
    ds_format,
    num_parts,
    configure_shuffle_method,
    disable_fallback_to_object_extension,
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
    ray_start_regular_shared_2_cpus,
    num_parts,
    configure_shuffle_method,
    disable_fallback_to_object_extension,
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


def test_groupby_agg_bad_on(
    ray_start_regular_shared_2_cpus,
    configure_shuffle_method,
    disable_fallback_to_object_extension,
):
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


def _sort_series_of_lists_elements(s: pd.Series):
    return s.apply(
        lambda l: list(
            # NOTE: We convert to Series to ensure the NaN elements will go last
            pd.Series(list(l)).sort_values()
        )
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
