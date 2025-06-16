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


@pytest.mark.parametrize("num_parts", [1, 30])
@pytest.mark.parametrize("ds_format", ["arrow", "pandas"])
def test_groupby_tabular_min(
    ray_start_regular_shared_2_cpus,
    ds_format,
    num_parts,
    configure_shuffle_method,
    disable_fallback_to_object_extension,
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
    ray_start_regular_shared_2_cpus,
    ds_format,
    num_parts,
    configure_shuffle_method,
    disable_fallback_to_object_extension,
):
    current = DataContext.get_current()
    if (
        num_parts == 30
        and current.shuffle_strategy == ShuffleStrategy.HASH_SHUFFLE
        and get_pyarrow_version() < MIN_PYARROW_VERSION_TYPE_PROMOTION
    ):
        # NOTE: When partitioning by large number of partitions some of these
        #       will be empty, hence resulting in the type deduced as a double
        pytest.skip(
            "Pyarrow < 14.0 doesn't support type promotions (hence fails "
            "promoting from int64 to double)"
        )

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
    ray_start_regular_shared_2_cpus,
    ds_format,
    num_parts,
    configure_shuffle_method,
    disable_fallback_to_object_extension,
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
