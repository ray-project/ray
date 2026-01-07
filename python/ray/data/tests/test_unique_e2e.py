import pandas as pd
import pytest

import ray
from ray.data.aggregate import AsList
from ray.data.expressions import col
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa

RANDOM_SEED = 123


def test_unique(ray_start_regular_shared_2_cpus, disable_fallback_to_object_extension):
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
def test_unique_with_nulls(
    ray_start_regular_shared_2_cpus, batch_format, disable_fallback_to_object_extension
):
    ds = ray.data.from_items([3, 2, 3, 1, 2, 3, None])
    assert set(ds.unique("item")) == {1, 2, 3, None}
    assert set(ds.unique("item", ignore_nulls=True)) == {1, 2, 3}

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
    assert set(ds.unique("b")) == {1, 2, 3, 4, None}

    # Check with 3 columns
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

    # df["col"].unique() works fine, as expected
    ds2 = ray.data.from_pandas(df)
    ds2 = ds2.map_batches(lambda x: x, batch_format=batch_format)
    assert set(ds2.unique("col1")) == {1, 2, 3, None}
    assert set(ds2.unique("col2")) == {2, 3, None}
    assert set(ds2.unique("col3")) == {"1.0", "2.0", None}

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
    assert set(ds3.unique("col2")) == {2, 3, None}
    assert set(ds3.unique("col3")) == {"1.0", "2.0", None}


@pytest.mark.parametrize("batch_format", ["pandas", "pyarrow"])
def test_as_list_e2e(
    ray_start_regular_shared_2_cpus, batch_format, disable_fallback_to_object_extension
):
    ds = ray.data.range(10)
    ds = ds.with_column("group_key", col("id") % 3)

    # Listing all elements per group:
    result = ds.groupby("group_key").aggregate(AsList(on="id")).take_all()

    for i in range(len(result)):
        result[i]["list(id)"] = sorted(result[i]["list(id)"])

    assert sorted(result, key=lambda x: x["group_key"]) == [
        {"group_key": 0, "list(id)": [0, 3, 6, 9]},
        {"group_key": 1, "list(id)": [1, 4, 7]},
        {"group_key": 2, "list(id)": [2, 5, 8]},
    ]


@pytest.mark.parametrize("batch_format", ["pandas", "pyarrow"])
def test_as_list_with_nulls(
    ray_start_regular_shared_2_cpus, batch_format, disable_fallback_to_object_extension
):
    # Test with nulls included (default behavior: ignore_nulls=False)
    ds = ray.data.from_items(
        [
            {"group": "A", "value": 1},
            {"group": "A", "value": None},
            {"group": "A", "value": 3},
            {"group": "B", "value": None},
            {"group": "B", "value": 5},
        ]
    )

    # Default: nulls are included in the list
    result = ds.groupby("group").aggregate(AsList(on="value")).take_all()
    result_sorted = sorted(result, key=lambda x: x["group"])

    # Sort the lists for comparison (None values will be at the end in sorted order)
    for r in result_sorted:
        # Separate None and non-None values for sorting
        non_nulls = sorted([v for v in r["list(value)"] if v is not None])
        nulls = [v for v in r["list(value)"] if v is None]
        r["list(value)"] = non_nulls + nulls

    assert result_sorted == [
        {"group": "A", "list(value)": [1, 3, None]},
        {"group": "B", "list(value)": [5, None]},
    ]

    # With ignore_nulls=True: nulls are excluded from the list
    result = ds.groupby("group").aggregate(AsList(on="value", ignore_nulls=True)).take_all()
    result_sorted = sorted(result, key=lambda x: x["group"])

    for r in result_sorted:
        r["list(value)"] = sorted(r["list(value)"])

    assert result_sorted == [
        {"group": "A", "list(value)": [1, 3]},
        {"group": "B", "list(value)": [5]},
    ]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
