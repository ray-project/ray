import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.data.tests.conftest import *  # noqa


class TestFillNa:
    """Test suite for Ray Data fillna functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        # Ensure consistent ordering for deterministic tests
        context = ray.data.DataContext.get_current()
        self.original_preserve_order = context.execution_options.preserve_order
        context.execution_options.preserve_order = True

    def teardown_method(self):
        """Clean up after tests."""
        context = ray.data.DataContext.get_current()
        context.execution_options.preserve_order = self.original_preserve_order

    def test_fillna_scalar_value(self, ray_start_regular_shared):
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

    def test_fillna_dict_values(self, ray_start_regular_shared):
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

    def test_fillna_with_subset(self, ray_start_regular_shared):
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

    def test_fillna_empty_dataset(self, ray_start_regular_shared):
        """Test fillna on empty dataset."""
        schema = pa.schema([("a", pa.int64()), ("b", pa.float64()), ("c", pa.string())])
        ds = ray.data.from_arrow(pa.table({"a": [], "b": [], "c": []}, schema=schema))

        result = ds.fillna(0)
        assert result.count() == 0

    def test_fillna_no_missing_values(self, ray_start_regular_shared):
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

    def test_fillna_different_dtypes(self, ray_start_regular_shared):
        """Test fillna with different data types."""
        ds = ray.data.from_items(
            [
                {"int_col": 1, "float_col": 1.5, "str_col": "a", "bool_col": True},
                {
                    "int_col": None,
                    "float_col": np.nan,
                    "str_col": None,
                    "bool_col": None,
                },
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

    @pytest.mark.parametrize("method", ["forward", "backward"])
    def test_fillna_directional_methods(self, ray_start_regular_shared, method):
        """Test fillna with forward and backward fill methods."""
        ds = ray.data.from_items(
            [
                {"a": 1, "b": 1.0},
                {"a": None, "b": None},
                {"a": 3, "b": 3.0},
                {"a": None, "b": None},
                {"a": 5, "b": 5.0},
            ]
        )

        result = ds.fillna(method=method)
        rows = result.take_all()

        if method == "forward":
            expected = [
                {"a": 1, "b": 1.0},
                {"a": 1, "b": 1.0},
                {"a": 3, "b": 3.0},
                {"a": 3, "b": 3.0},
                {"a": 5, "b": 5.0},
            ]
        else:  # backward
            expected = [
                {"a": 1, "b": 1.0},
                {"a": 3, "b": 3.0},
                {"a": 3, "b": 3.0},
                {"a": 5, "b": 5.0},
                {"a": 5, "b": 5.0},
            ]

        assert rows == expected

    def test_fillna_interpolate_method(self, ray_start_regular_shared):
        """Test fillna with interpolate method for numeric columns."""
        ds = ray.data.from_items(
            [
                {"a": 1, "b": 1.0, "c": "x"},
                {"a": None, "b": None, "c": None},
                {"a": 5, "b": 5.0, "c": "y"},
                {"a": None, "b": None, "c": None},
                {"a": 9, "b": 9.0, "c": "z"},
            ]
        )

        result = ds.fillna(method="interpolate")
        rows = result.take_all()

        expected = [
            {"a": 1, "b": 1.0, "c": "x"},
            {"a": 3, "b": 3.0, "c": None},  # String columns not interpolated
            {"a": 5, "b": 5.0, "c": "y"},
            {"a": 7, "b": 7.0, "c": None},  # String columns not interpolated
            {"a": 9, "b": 9.0, "c": "z"},
        ]

        assert rows == expected

    def test_fillna_with_limit(self, ray_start_regular_shared):
        """Test fillna with limit parameter."""
        ds = ray.data.from_items(
            [
                {"a": 1},
                {"a": None},
                {"a": None},
                {"a": None},
                {"a": 5},
            ]
        )

        result = ds.fillna(method="forward", limit=2)
        rows = result.take_all()

        expected = [
            {"a": 1},
            {"a": 1},  # First fill
            {"a": 1},  # Second fill
            {"a": None},  # Limit reached, remains None
            {"a": 5},
        ]

        assert rows == expected

    def test_fillna_method_validation(self, ray_start_regular_shared):
        """Test fillna method validation."""
        ds = ray.data.from_items([{"a": 1}, {"a": None}])

        # Test invalid method
        with pytest.raises(ValueError, match="Unsupported method"):
            ds.fillna(method="invalid")

        # Test missing value when method="value"
        with pytest.raises(ValueError, match="'value' parameter is required"):
            ds.fillna(method="value")

    def test_fillna_with_subset_and_dict(self, ray_start_regular_shared):
        """Test fillna with subset and dictionary values."""
        ds = ray.data.from_items(
            [
                {"a": 1, "b": 2.0, "c": "x"},
                {"a": None, "b": np.nan, "c": None},
                {"a": 3, "b": None, "c": "z"},
            ]
        )

        # Only columns in subset should be affected, and only those in dict should be filled
        result = ds.fillna({"a": -1, "c": "missing"}, subset=["a", "b"])
        rows = result.take_all()

        expected = [
            {"a": 1, "b": 2.0, "c": "x"},
            {
                "a": -1,
                "b": np.nan,
                "c": None,
            },  # b is in subset but not in dict, c not in subset
            {"a": 3, "b": None, "c": "z"},  # b is in subset but not in dict
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

    def test_fillna_preserves_schema(self, ray_start_regular_shared):
        """Test that fillna preserves the original schema."""
        original_data = pa.table(
            {
                "int_col": pa.array([1, None, 3], type=pa.int64()),
                "float_col": pa.array([1.5, None, 3.5], type=pa.float64()),
                "str_col": pa.array(["a", None, "c"], type=pa.string()),
            }
        )
        ds = ray.data.from_arrow(original_data)

        result = ds.fillna({"int_col": 0, "float_col": 0.0, "str_col": "missing"})

        # Check that schema is preserved
        assert result.schema() == ds.schema()

    @pytest.mark.parametrize("num_blocks", [1, 3, 10])
    def test_fillna_multiple_blocks(self, ray_start_regular_shared, num_blocks):
        """Test fillna works correctly across multiple blocks."""
        data = []
        for i in range(30):
            if i % 3 == 1:
                data.append({"a": None, "b": None})
            else:
                data.append({"a": i, "b": float(i)})

        ds = ray.data.from_items(data, override_num_blocks=num_blocks)
        result = ds.fillna({"a": -1, "b": -1.0})
        rows = result.take_all()

        # Check that all None values were replaced
        for row in rows:
            assert row["a"] is not None
            assert row["b"] is not None
            if row["a"] == -1:
                assert row["b"] == -1.0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__]))
