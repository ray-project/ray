import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.data.tests.conftest import *  # noqa


def test_fillna_scalar_value():
    """Test fillna with scalar value fills all missing values."""
    ds = ray.data.from_items(
        [
            {"a": 1, "b": 2.0, "c": "x"},
            {"a": None, "b": np.nan, "c": None},
            {"a": 3, "b": None, "c": "z"},
            {"a": 4, "b": 5.0, "c": "w"},
        ]
    )

    result = ds.fillna(0)
    rows = result.take_all()

    expected = [
        {"a": 1, "b": 2.0, "c": "x"},
        {"a": 0, "b": 0.0, "c": "0"},
        {"a": 3, "b": 0.0, "c": "z"},
        {"a": 4, "b": 5.0, "c": "w"},
    ]

    assert rows == expected


def test_fillna_dict_values():
    """Test fillna with dictionary values for column-specific filling."""
    ds = ray.data.from_items(
        [
            {"a": 1, "b": 2.0, "c": "x"},
            {"a": None, "b": np.nan, "c": None},
            {"a": 3, "b": None, "c": "z"},
            {"a": None, "b": 4.0, "c": None},
        ]
    )

    result = ds.fillna({"a": -1, "b": -2.0, "c": "missing"})
    rows = result.take_all()

    expected = [
        {"a": 1, "b": 2.0, "c": "x"},
        {"a": -1, "b": -2.0, "c": "missing"},
        {"a": 3, "b": -2.0, "c": "z"},
        {"a": -1, "b": 4.0, "c": "missing"},
    ]

    assert rows == expected


def test_fillna_with_subset():
    """Test fillna with subset parameter to fill only specified columns."""
    ds = ray.data.from_items(
        [
            {"a": 1, "b": 2.0, "c": "x"},
            {"a": None, "b": np.nan, "c": None},
            {"a": 3, "b": None, "c": "z"},
            {"a": None, "b": 4.0, "c": None},
        ]
    )

    result = ds.fillna(0, subset=["a"])
    rows = result.take_all()

    expected = [
        {"a": 1, "b": 2.0, "c": "x"},
        {"a": 0, "b": np.nan, "c": None},
        {"a": 3, "b": None, "c": "z"},
        {"a": 0, "b": 4.0, "c": None},
    ]

    # Compare while handling NaN values
    assert len(rows) == len(expected)
    for i, (actual, exp) in enumerate(zip(rows, expected)):
        assert actual["a"] == exp["a"]
        assert actual["c"] == exp["c"]
        if pd.isna(exp["b"]):
            assert pd.isna(actual["b"])
        else:
            assert actual["b"] == exp["b"]


def test_fillna_dict_with_subset():
    """Test fillna with dictionary values and subset parameter."""
    ds = ray.data.from_items(
        [
            {"a": 1, "b": 2.0, "c": "x"},
            {"a": None, "b": np.nan, "c": None},
            {"a": 3, "b": None, "c": "z"},
            {"a": None, "b": 4.0, "c": None},
        ]
    )

    result = ds.fillna({"a": -1, "b": -2.0, "c": "missing"}, subset=["a", "c"])
    rows = result.take_all()

    expected = [
        {"a": 1, "b": 2.0, "c": "x"},
        {"a": -1, "b": np.nan, "c": "missing"},
        {"a": 3, "b": None, "c": "z"},
        {"a": -1, "b": 4.0, "c": "missing"},
    ]

    # Compare while handling NaN values
    assert len(rows) == len(expected)
    for i, (actual, exp) in enumerate(zip(rows, expected)):
        assert actual["a"] == exp["a"]
        assert actual["c"] == exp["c"]
        if pd.isna(exp["b"]):
            assert pd.isna(actual["b"])
        else:
            assert actual["b"] == exp["b"]


def test_fillna_empty_dataset():
    """Test fillna on empty dataset."""
    schema = pa.schema([("a", pa.int64()), ("b", pa.float64()), ("c", pa.string())])
    ds = ray.data.from_arrow(pa.table({"a": [], "b": [], "c": []}, schema=schema))

    result = ds.fillna(0)
    assert result.count() == 0


def test_fillna_no_missing_values():
    """Test fillna on dataset with no missing values."""
    ds = ray.data.from_items(
        [
            {"a": 1, "b": 2.0, "c": "x"},
            {"a": 2, "b": 3.0, "c": "y"},
            {"a": 3, "b": 4.0, "c": "z"},
        ]
    )

    result = ds.fillna(0)
    rows = result.take_all()

    expected = [
        {"a": 1, "b": 2.0, "c": "x"},
        {"a": 2, "b": 3.0, "c": "y"},
        {"a": 3, "b": 4.0, "c": "z"},
    ]

    assert rows == expected


def test_fillna_different_dtypes():
    """Test fillna with different data types."""
    ds = ray.data.from_items(
        [
            {"int_col": 1, "float_col": 1.5, "str_col": "a", "bool_col": True},
            {"int_col": None, "float_col": np.nan, "str_col": None, "bool_col": None},
            {"int_col": 3, "float_col": None, "str_col": "c", "bool_col": False},
        ]
    )

    result = ds.fillna(
        {"int_col": 0, "float_col": 0.0, "str_col": "missing", "bool_col": False}
    )
    rows = result.take_all()

    expected = [
        {"int_col": 1, "float_col": 1.5, "str_col": "a", "bool_col": True},
        {"int_col": 0, "float_col": 0.0, "str_col": "missing", "bool_col": False},
        {"int_col": 3, "float_col": 0.0, "str_col": "c", "bool_col": False},
    ]

    assert rows == expected


def test_fillna_nonexistent_column_in_subset():
    """Test fillna with subset containing nonexistent columns."""
    ds = ray.data.from_items(
        [{"a": 1, "b": 2.0}, {"a": None, "b": np.nan}, {"a": 3, "b": None}]
    )

    # Should not raise error, just ignore nonexistent columns
    result = ds.fillna(0, subset=["a", "nonexistent"])
    rows = result.take_all()

    expected = [{"a": 1, "b": 2.0}, {"a": 0, "b": np.nan}, {"a": 3, "b": None}]

    # Compare while handling NaN values
    assert len(rows) == len(expected)
    for i, (actual, exp) in enumerate(zip(rows, expected)):
        assert actual["a"] == exp["a"]
        if pd.isna(exp["b"]):
            assert pd.isna(actual["b"])
        else:
            assert actual["b"] == exp["b"]


def test_fillna_dict_nonexistent_column():
    """Test fillna with dictionary containing nonexistent columns."""
    ds = ray.data.from_items(
        [{"a": 1, "b": 2.0}, {"a": None, "b": np.nan}, {"a": 3, "b": None}]
    )

    # Should not raise error, just ignore nonexistent columns
    result = ds.fillna({"a": 0, "nonexistent": 999})
    rows = result.take_all()

    expected = [{"a": 1, "b": 2.0}, {"a": 0, "b": np.nan}, {"a": 3, "b": None}]

    # Compare while handling NaN values
    assert len(rows) == len(expected)
    for i, (actual, exp) in enumerate(zip(rows, expected)):
        assert actual["a"] == exp["a"]
        if pd.isna(exp["b"]):
            assert pd.isna(actual["b"])
        else:
            assert actual["b"] == exp["b"]


def test_fillna_preserves_schema():
    """Test that fillna preserves the dataset schema."""
    schema = pa.schema([("a", pa.int64()), ("b", pa.float64()), ("c", pa.string())])

    ds = ray.data.from_arrow(
        pa.table(
            {"a": [1, None, 3], "b": [1.0, None, 3.0], "c": ["x", None, "z"]},
            schema=schema,
        )
    )

    result = ds.fillna({"a": 0, "b": 0.0, "c": "missing"})

    # Check that schema is preserved
    assert result.schema() == schema


def test_fillna_large_dataset():
    """Test fillna on a larger dataset to ensure it works with multiple batches."""
    data = []
    for i in range(1000):
        if i % 3 == 0:
            data.append({"a": None, "b": np.nan})
        else:
            data.append({"a": i, "b": float(i)})

    ds = ray.data.from_items(data)
    result = ds.fillna({"a": -1, "b": -1.0})

    rows = result.take_all()
    assert len(rows) == 1000

    # Check that null values were replaced
    for i, row in enumerate(rows):
        if i % 3 == 0:
            assert row["a"] == -1
            assert row["b"] == -1.0
        else:
            assert row["a"] == i
            assert row["b"] == float(i)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__]))
