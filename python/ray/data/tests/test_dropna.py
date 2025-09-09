import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.data.tests.conftest import *  # noqa


class TestDropNa:
    """Test suite for Ray Data dropna functionality."""

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

    def test_dropna_any(self, ray_start_regular_shared):
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

    def test_dropna_all(self, ray_start_regular_shared):
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

    def test_dropna_subset(self, ray_start_regular_shared):
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

    def test_dropna_thresh(self, ray_start_regular_shared):
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

    def test_dropna_thresh_with_subset(self, ray_start_regular_shared):
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

    def test_dropna_empty_dataset(self, ray_start_regular_shared):
        """Test dropna on empty dataset."""
        schema = pa.schema([("a", pa.int64()), ("b", pa.float64()), ("c", pa.string())])
        ds = ray.data.from_arrow(pa.table({"a": [], "b": [], "c": []}, schema=schema))

        result = ds.dropna()
        assert result.count() == 0

    def test_dropna_no_missing_values(self, ray_start_regular_shared):
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

    def test_dropna_all_rows_dropped(self, ray_start_regular_shared):
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

    def test_dropna_single_column(self, ray_start_regular_shared):
        """Test dropna on dataset with single column."""
        ds = ray.data.from_items([{"a": 1}, {"a": None}, {"a": 3}, {"a": None}, {"a": 5}])

        result = ds.dropna()
        rows = result.take_all()

        expected = [{"a": 1}, {"a": 3}, {"a": 5}]

        assert rows == expected

    def test_dropna_ignore_values(self, ray_start_regular_shared):
        """Test dropna with ignore_values parameter."""
        ds = ray.data.from_items(
            [
                {"a": 1, "b": "valid"},
                {"a": 0, "b": ""},  # Treat 0 and empty string as missing
                {"a": 2, "b": "valid"},
                {"a": None, "b": "valid"},
                {"a": 3, "b": "also_valid"},
            ]
        )

        result = ds.dropna(ignore_values=[0, ""])
        rows = result.take_all()

        expected = [
            {"a": 1, "b": "valid"},
            {"a": 2, "b": "valid"},
            {"a": 3, "b": "also_valid"},
        ]

        assert rows == expected

    def test_dropna_ignore_values_with_subset(self, ray_start_regular_shared):
        """Test dropna with ignore_values and subset parameters."""
        ds = ray.data.from_items(
            [
                {"a": 1, "b": "valid", "c": 0},  # c=0 should be ignored but c not in subset
                {"a": 0, "b": "valid", "c": 1},  # a=0 should be treated as missing
                {"a": 2, "b": "", "c": 2},  # b="" should be ignored but b not in subset
                {"a": 3, "b": "valid", "c": 3},
            ]
        )

        result = ds.dropna(ignore_values=[0, ""], subset=["a"])
        rows = result.take_all()

        expected = [
            {"a": 1, "b": "valid", "c": 0},
            {"a": 2, "b": "", "c": 2},
            {"a": 3, "b": "valid", "c": 3},
        ]

        assert rows == expected

    @pytest.mark.parametrize("how", ["any", "all"])
    def test_dropna_how_parameter(self, ray_start_regular_shared, how):
        """Test dropna with different 'how' parameter values."""
        ds = ray.data.from_items(
            [
                {"a": 1, "b": 2, "c": 3},  # No missing values
                {"a": None, "b": 2, "c": 3},  # One missing value
                {"a": None, "b": None, "c": None},  # All missing values
                {"a": 1, "b": None, "c": 3},  # One missing value
            ]
        )

        result = ds.dropna(how=how)
        rows = result.take_all()

        if how == "any":
            expected = [{"a": 1, "b": 2, "c": 3}]
        else:  # how == "all"
            expected = [
                {"a": 1, "b": 2, "c": 3},
                {"a": None, "b": 2, "c": 3},
                {"a": 1, "b": None, "c": 3},
            ]

        # Handle None values in comparison
        assert len(rows) == len(expected)
        for actual, exp in zip(rows, expected):
            for key in exp.keys():
                if exp[key] is None:
                    assert actual[key] is None
                else:
                    assert actual[key] == exp[key]

    def test_dropna_invalid_how(self, ray_start_regular_shared):
        """Test dropna with invalid 'how' parameter."""
        ds = ray.data.from_items([{"a": 1}, {"a": None}])

        with pytest.raises(ValueError, match="'how' must be 'any' or 'all'"):
            ds.dropna(how="invalid")

    def test_dropna_invalid_thresh(self, ray_start_regular_shared):
        """Test dropna with invalid thresh parameter."""
        ds = ray.data.from_items([{"a": 1}, {"a": None}])

        with pytest.raises(ValueError, match="'thresh' must be non-negative"):
            ds.dropna(thresh=-1)

    def test_dropna_thresh_exceeds_subset(self, ray_start_regular_shared):
        """Test dropna when thresh exceeds subset length."""
        ds = ray.data.from_items([{"a": 1, "b": 2, "c": 3}])

        with pytest.raises(ValueError, match="'thresh' cannot be greater than the number of columns"):
            ds.dropna(thresh=3, subset=["a", "b"])  # thresh=3 > len(subset)=2

    def test_dropna_preserves_schema(self, ray_start_regular_shared):
        """Test that dropna preserves the original schema."""
        original_data = pa.table({
            "int_col": pa.array([1, None, 3], type=pa.int64()),
            "float_col": pa.array([1.5, 2.5, None], type=pa.float64()),
            "str_col": pa.array(["a", "b", None], type=pa.string()),
        })
        ds = ray.data.from_arrow(original_data)

        result = ds.dropna()
        
        # Check that schema is preserved
        assert result.schema() == ds.schema()

    def test_dropna_nonexistent_subset_columns(self, ray_start_regular_shared):
        """Test dropna with nonexistent columns in subset."""
        ds = ray.data.from_items(
            [
                {"a": 1, "b": 2},
                {"a": None, "b": 3},
            ]
        )

        # Should ignore nonexistent columns and work with existing ones
        result = ds.dropna(subset=["a", "nonexistent", "b"])
        rows = result.take_all()

        expected = [{"a": 1, "b": 2}]
        assert rows == expected

        # Should return original data if no valid columns in subset
        result = ds.dropna(subset=["nonexistent1", "nonexistent2"])
        rows = result.take_all()
        assert len(rows) == 2  # All rows preserved

    @pytest.mark.parametrize("num_blocks", [1, 3, 10])
    def test_dropna_multiple_blocks(self, ray_start_regular_shared, num_blocks):
        """Test dropna works correctly across multiple blocks."""
        data = []
        for i in range(30):
            if i % 3 == 1:
                data.append({"a": None, "b": None})
            else:
                data.append({"a": i, "b": float(i)})

        ds = ray.data.from_items(data, override_num_blocks=num_blocks)
        result = ds.dropna()
        rows = result.take_all()

        # Should have 20 rows (30 - 10 with None values)
        assert len(rows) == 20
        
        # Check that no None values remain
        for row in rows:
            assert row["a"] is not None
            assert row["b"] is not None

    def test_dropna_mixed_null_types(self, ray_start_regular_shared):
        """Test dropna handles different types of null values."""
        ds = ray.data.from_items(
            [
                {"a": 1, "b": 1.0, "c": "valid"},
                {"a": None, "b": 2.0, "c": "valid"},  # Python None
                {"a": 2, "b": np.nan, "c": "valid"},  # NumPy NaN
                {"a": 3, "b": 3.0, "c": None},  # Python None in string
                {"a": 4, "b": 4.0, "c": "valid"},
            ]
        )

        result = ds.dropna()
        rows = result.take_all()

        expected = [
            {"a": 1, "b": 1.0, "c": "valid"},
            {"a": 4, "b": 4.0, "c": "valid"},
        ]

        assert rows == expected


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__]))