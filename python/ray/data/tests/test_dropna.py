import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.data.tests.conftest import *  # noqa


def test_dropna_any():
    """Test dropna with how='any' drops rows with any missing values."""
    ds = ray.data.from_items(
        [
            {"a": 1, "b": 2.0, "c": "x"},
            {"a": None, "b": 3.0, "c": "y"},
            {"a": 2, "b": np.nan, "c": "z"},
            {"a": 3, "b": 4.0, "c": None},
            {"a": 4, "b": 5.0, "c": "w"},
        ]
    )

    result = ds.dropna(how="any")
    rows = result.take_all()

    expected = [{"a": 1, "b": 2.0, "c": "x"}, {"a": 4, "b": 5.0, "c": "w"}]

    assert rows == expected


def test_dropna_all():
    """Test dropna with how='all' drops rows where all values are missing."""
    ds = ray.data.from_items(
        [
            {"a": 1, "b": 2.0, "c": "x"},
            {"a": None, "b": None, "c": None},
            {"a": 2, "b": np.nan, "c": "z"},
            {"a": None, "b": 4.0, "c": None},
            {"a": 4, "b": 5.0, "c": "w"},
        ]
    )

    result = ds.dropna(how="all")
    rows = result.take_all()

    expected = [
        {"a": 1, "b": 2.0, "c": "x"},
        {"a": 2, "b": np.nan, "c": "z"},
        {"a": None, "b": 4.0, "c": None},
        {"a": 4, "b": 5.0, "c": "w"},
    ]

    # Compare while handling NaN/None values
    assert len(rows) == len(expected)
    for i, (actual, exp) in enumerate(zip(rows, expected)):
        for key in exp.keys():
            if pd.isna(exp[key]):
                assert pd.isna(actual[key])
            else:
                assert actual[key] == exp[key]


def test_dropna_subset():
    """Test dropna with subset parameter to consider only specified columns."""
    ds = ray.data.from_items(
        [
            {"a": 1, "b": 2.0, "c": "x"},
            {"a": None, "b": 3.0, "c": "y"},
            {"a": 2, "b": np.nan, "c": "z"},
            {"a": 3, "b": 4.0, "c": None},
            {"a": 4, "b": 5.0, "c": "w"},
        ]
    )

    result = ds.dropna(subset=["a", "b"])
    rows = result.take_all()

    expected = [
        {"a": 1, "b": 2.0, "c": "x"},
        {"a": 3, "b": 4.0, "c": None},
        {"a": 4, "b": 5.0, "c": "w"},
    ]

    assert rows == expected


def test_dropna_thresh():
    """Test dropna with thresh parameter for minimum non-null values."""
    ds = ray.data.from_items(
        [
            {"a": 1, "b": 2.0, "c": "x"},  # 3 non-null
            {"a": None, "b": 3.0, "c": "y"},  # 2 non-null
            {"a": 2, "b": np.nan, "c": None},  # 1 non-null
            {"a": None, "b": None, "c": None},  # 0 non-null
            {"a": 4, "b": 5.0, "c": "w"},  # 3 non-null
        ]
    )

    result = ds.dropna(thresh=2)
    rows = result.take_all()

    expected = [
        {"a": 1, "b": 2.0, "c": "x"},
        {"a": None, "b": 3.0, "c": "y"},
        {"a": 4, "b": 5.0, "c": "w"},
    ]

    # Compare while handling None values
    assert len(rows) == len(expected)
    for i, (actual, exp) in enumerate(zip(rows, expected)):
        for key in exp.keys():
            if exp[key] is None:
                assert actual[key] is None
            else:
                assert actual[key] == exp[key]


def test_dropna_thresh_with_subset():
    """Test dropna with thresh parameter and subset."""
    ds = ray.data.from_items(
        [
            {"a": 1, "b": 2.0, "c": "x"},  # a,b: 2 non-null
            {"a": None, "b": 3.0, "c": "y"},  # a,b: 1 non-null
            {"a": 2, "b": np.nan, "c": "z"},  # a,b: 1 non-null
            {"a": None, "b": None, "c": "w"},  # a,b: 0 non-null
            {"a": 4, "b": 5.0, "c": "v"},  # a,b: 2 non-null
        ]
    )

    result = ds.dropna(thresh=2, subset=["a", "b"])
    rows = result.take_all()

    expected = [{"a": 1, "b": 2.0, "c": "x"}, {"a": 4, "b": 5.0, "c": "v"}]

    assert rows == expected


def test_dropna_empty_dataset():
    """Test dropna on empty dataset."""
    schema = pa.schema([("a", pa.int64()), ("b", pa.float64()), ("c", pa.string())])
    ds = ray.data.from_arrow(pa.table({"a": [], "b": [], "c": []}, schema=schema))

    result = ds.dropna()
    assert result.count() == 0


def test_dropna_no_missing_values():
    """Test dropna on dataset with no missing values."""
    ds = ray.data.from_items(
        [
            {"a": 1, "b": 2.0, "c": "x"},
            {"a": 2, "b": 3.0, "c": "y"},
            {"a": 3, "b": 4.0, "c": "z"},
        ]
    )

    result = ds.dropna()
    rows = result.take_all()

    expected = [
        {"a": 1, "b": 2.0, "c": "x"},
        {"a": 2, "b": 3.0, "c": "y"},
        {"a": 3, "b": 4.0, "c": "z"},
    ]

    assert rows == expected


def test_dropna_all_rows_dropped():
    """Test dropna when all rows should be dropped."""
    ds = ray.data.from_items(
        [
            {"a": None, "b": np.nan, "c": None},
            {"a": None, "b": None, "c": None},
            {"a": np.nan, "b": None, "c": np.nan},
        ]
    )

    result = ds.dropna(how="any")
    assert result.count() == 0


def test_dropna_single_column():
    """Test dropna on dataset with single column."""
    ds = ray.data.from_items([{"a": 1}, {"a": None}, {"a": 3}, {"a": None}, {"a": 5}])

    result = ds.dropna()
    rows = result.take_all()

    expected = [{"a": 1}, {"a": 3}, {"a": 5}]

    assert rows == expected


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__]))
