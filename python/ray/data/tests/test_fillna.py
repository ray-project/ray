import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.data.tests.conftest import *  # noqa


def test_fillna_scalar_value():
    """Test fillna with scalar value fills all missing values."""
    # Set preserve_order to True to ensure consistent row ordering
    context = ray.data.DataContext.get_current()
    context.execution_options.preserve_order = True

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
    # Set preserve_order to True to ensure consistent row ordering
    context = ray.data.DataContext.get_current()
    context.execution_options.preserve_order = True

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
    # Set preserve_order to True to ensure consistent row ordering
    context = ray.data.DataContext.get_current()
    context.execution_options.preserve_order = True

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


def test_fillna_empty_dataset():
    """Test fillna on empty dataset."""
    schema = pa.schema([("a", pa.int64()), ("b", pa.float64()), ("c", pa.string())])
    ds = ray.data.from_arrow(pa.table({"a": [], "b": [], "c": []}, schema=schema))

    result = ds.fillna(0)
    assert result.count() == 0


def test_fillna_no_missing_values():
    """Test fillna on dataset with no missing values."""
    # Set preserve_order to True to ensure consistent row ordering
    context = ray.data.DataContext.get_current()
    context.execution_options.preserve_order = True

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
    # Set preserve_order to True to ensure consistent row ordering
    context = ray.data.DataContext.get_current()
    context.execution_options.preserve_order = True

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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__]))
