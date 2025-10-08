import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.data.stats import (
    DtypeAggregators,
    _dtype_aggregators_for_dataset,
)


class TestDtypeAggregatorsForDataset:
    """Test suite for _dtype_aggregators_for_dataset function."""

    def test_numerical_dtypes_detection(self):
        """Test that numerical dtypes are correctly identified and get appropriate aggregators."""
        data = [
            {"int_col": 1, "float_col": 1.5, "string_col": "a"},
            {"int_col": 2, "float_col": 2.5, "string_col": "b"},
            {"int_col": 3, "float_col": 3.5, "string_col": "c"},
        ]

        ds = ray.data.from_items(data)
        dtype_aggs = _dtype_aggregators_for_dataset(ds.schema())

        # Check that dtypes are stored
        assert "int_col" in dtype_aggs.column_to_dtype
        assert "float_col" in dtype_aggs.column_to_dtype
        assert "string_col" in dtype_aggs.column_to_dtype

        # Check dtype strings
        assert "int64" in dtype_aggs.column_to_dtype["int_col"]
        assert "double" in dtype_aggs.column_to_dtype["float_col"]
        assert "string" in dtype_aggs.column_to_dtype["string_col"]

        # Check aggregator count: 2 numerical * 7 + 1 string * 2 = 16
        assert len(dtype_aggs.aggregators) == 16

    def test_string_dtypes_detection(self):
        """Test that string dtypes are correctly identified."""
        data = [
            {"category": "A", "name": "Alice", "value": 1},
            {"category": "B", "name": "Bob", "value": 2},
        ]

        ds = ray.data.from_items(data)
        dtype_aggs = _dtype_aggregators_for_dataset(ds.schema())

        # Check that all columns have dtypes
        assert "category" in dtype_aggs.column_to_dtype
        assert "name" in dtype_aggs.column_to_dtype
        assert "value" in dtype_aggs.column_to_dtype

        # String columns should have string dtype
        assert "string" in dtype_aggs.column_to_dtype["category"]
        assert "string" in dtype_aggs.column_to_dtype["name"]

        # Check aggregator count: 1 numerical * 7 + 2 string * 2 = 11
        assert len(dtype_aggs.aggregators) == 11

    def test_list_dtypes_detection(self):
        """Test that list dtypes are correctly identified."""
        data = [
            {"vector": [1, 2, 3], "scalar": 1, "text": "hello"},
            {"vector": [4, 5, 6], "scalar": 2, "text": "world"},
        ]

        ds = ray.data.from_items(data)
        dtype_aggs = _dtype_aggregators_for_dataset(ds.schema())

        # Check dtypes
        assert "list" in dtype_aggs.column_to_dtype["vector"]
        assert "int64" in dtype_aggs.column_to_dtype["scalar"]
        assert "string" in dtype_aggs.column_to_dtype["text"]

        # Check aggregator count: 1 numerical * 7 + 1 string * 2 + 1 list * 2 = 11
        assert len(dtype_aggs.aggregators) == 11

    def test_column_filtering(self):
        """Test that only specified columns are included."""
        data = [
            {"col1": 1, "col2": "a", "col3": [1, 2], "col4": 1.5},
            {"col1": 2, "col2": "b", "col3": [3, 4], "col4": 2.5},
        ]

        ds = ray.data.from_items(data)
        dtype_aggs = _dtype_aggregators_for_dataset(
            ds.schema(), columns=["col1", "col3"]
        )

        # Should only include col1 and col3
        assert "col1" in dtype_aggs.column_to_dtype
        assert "col2" not in dtype_aggs.column_to_dtype
        assert "col3" in dtype_aggs.column_to_dtype
        assert "col4" not in dtype_aggs.column_to_dtype

        # Check aggregator count: 1 numerical * 7 + 1 list * 2 = 9
        assert len(dtype_aggs.aggregators) == 9

    def test_empty_dataset_schema(self):
        """Test behavior with empty dataset that has no schema."""
        ds = ray.data.from_items([])

        with pytest.raises(ValueError, match="Dataset must have a schema"):
            _dtype_aggregators_for_dataset(ds.schema())

    def test_invalid_columns_parameter(self):
        """Test error handling when columns parameter contains non-existent columns."""
        data = [{"col1": 1, "col2": "a"}]
        ds = ray.data.from_items(data)

        with pytest.raises(ValueError, match="Columns .* not found in dataset schema"):
            _dtype_aggregators_for_dataset(
                ds.schema(), columns=["col1", "nonexistent_col"]
            )

    def test_return_dataclass_structure(self):
        """Test that the function returns the correct DtypeAggregators dataclass."""
        data = [{"num": 1, "cat": "a"}]
        ds = ray.data.from_items(data)
        result = _dtype_aggregators_for_dataset(ds.schema())

        # Should return a DtypeAggregators dataclass
        assert isinstance(result, DtypeAggregators)
        assert isinstance(result.column_to_dtype, dict)
        assert isinstance(result.aggregators, list)

    def test_none_columns_parameter(self):
        """Test that None columns parameter includes all columns."""
        data = [{"col1": 1, "col2": "a"}]
        ds = ray.data.from_items(data)

        # Test with None (should be same as not providing columns parameter)
        result1 = _dtype_aggregators_for_dataset(ds.schema(), columns=None)
        result2 = _dtype_aggregators_for_dataset(ds.schema())

        # Compare the dataclass attributes
        assert result1.column_to_dtype == result2.column_to_dtype
        assert len(result1.aggregators) == len(result2.aggregators)

    def test_empty_columns_list(self):
        """Test behavior with empty columns list."""
        data = [{"col1": 1, "col2": "a"}]
        ds = ray.data.from_items(data)

        dtype_aggs = _dtype_aggregators_for_dataset(ds.schema(), columns=[])

        # Should have no columns and no aggregators
        assert len(dtype_aggs.column_to_dtype) == 0
        assert len(dtype_aggs.aggregators) == 0


class TestDatasetSummary:
    """Test suite for Dataset.summary() method."""

    @pytest.mark.parametrize(
        "test_case,data,expected_df,columns",
        [
            pytest.param(
                "basic_mixed_types",
                [
                    {"age": 25, "name": "Alice", "scores": [1, 2, 3]},
                    {"age": 30, "name": "Bob", "scores": [4, 5, 6]},
                    {"age": 35, "name": None, "scores": None},
                ],
                pd.DataFrame(
                    {
                        "column": ["age", "name", "scores"],
                        "count": [3, 3, 3],
                        "mean": [30.0, np.nan, np.nan],
                        "min": [25.0, np.nan, np.nan],
                        "max": [35.0, np.nan, np.nan],
                        "std": [4.08248290463863, np.nan, np.nan],
                        "missing_pct": [0.0, 33.333333, 33.333333],
                        "zero_pct": [0.0, np.nan, np.nan],
                    }
                ).astype(
                    {
                        "column": "object",
                        "count": "int64",
                        "mean": "float64",
                        "min": "float64",
                        "max": "float64",
                        "std": "float64",
                        "missing_pct": "float64",
                        "zero_pct": "float64",
                    }
                ),
                None,
                id="basic_mixed_types",
            ),
            pytest.param(
                "numerical_only",
                [{"x": 1, "y": 2.0}, {"x": 3, "y": 4.0}],
                pd.DataFrame(
                    {
                        "column": ["x", "y"],
                        "count": [2, 2],
                        "mean": [2.0, 3.0],
                        "min": [1.0, 2.0],
                        "max": [3.0, 4.0],
                        "std": [1.0, 1.0],
                        "missing_pct": [0.0, 0.0],
                        "zero_pct": [0.0, 0.0],
                    }
                ).astype(
                    {
                        "column": "object",
                        "count": "int64",
                        "mean": "float64",
                        "min": "float64",
                        "max": "float64",
                        "std": "float64",
                        "missing_pct": "float64",
                        "zero_pct": "float64",
                    }
                ),
                None,
                id="numerical_only",
            ),
            pytest.param(
                "categorical_only",
                [
                    {"city": "NYC", "country": "USA"},
                    {"city": "LA", "country": "USA"},
                ],
                pd.DataFrame(
                    {
                        "column": ["city", "country"],
                        "count": [2, 2],
                        "mean": [np.nan, np.nan],
                        "min": [np.nan, np.nan],
                        "max": [np.nan, np.nan],
                        "std": [np.nan, np.nan],
                        "missing_pct": [0.0, 0.0],
                        "zero_pct": [np.nan, np.nan],
                    }
                ).astype(
                    {
                        "column": "object",
                        "count": "int64",
                        "mean": "float64",
                        "min": "float64",
                        "max": "float64",
                        "std": "float64",
                        "missing_pct": "float64",
                        "zero_pct": "float64",
                    }
                ),
                None,
                id="categorical_only",
            ),
            pytest.param(
                "with_missing_values",
                [{"value": 10}, {"value": None}, {"value": 30}],
                pd.DataFrame(
                    {
                        "column": ["value"],
                        "count": [3],
                        "mean": [20.0],
                        "min": [10],
                        "max": [30],
                        "std": [10],
                        "missing_pct": [33.333333],
                        "zero_pct": [0.0],
                    }
                ).astype(
                    {
                        "column": "object",
                        "count": "int64",
                        "mean": "float64",
                        "min": "int64",
                        "max": "int64",
                        "std": "float64",
                        "missing_pct": "float64",
                        "zero_pct": "float64",
                    }
                ),
                None,
                id="with_missing_values",
            ),
            pytest.param(
                "with_zeros",
                [
                    {"value": 0, "text": "test1"},
                    {"value": 0, "text": "test2"},
                    {"value": 100, "text": None},
                ],
                pd.DataFrame(
                    {
                        "column": ["value", "text"],
                        "count": [3, 3],
                        "mean": [33.333333, np.nan],
                        "min": [0.0, np.nan],
                        "max": [100.0, np.nan],
                        "std": [47.140452079103168, np.nan],
                        "missing_pct": [0.0, 33.333333],
                        "zero_pct": [66.666667, np.nan],
                    }
                ).astype(
                    {
                        "column": "object",
                        "count": "int64",
                        "mean": "float64",
                        "min": "float64",
                        "max": "float64",
                        "std": "float64",
                        "missing_pct": "float64",
                        "zero_pct": "float64",
                    }
                ),
                None,
                id="with_zeros",
            ),
            pytest.param(
                "column_filtering",
                [
                    {"age": 25, "salary": 50000, "name": "Alice", "scores": [1, 2]},
                    {"age": 30, "salary": 60000, "name": "Bob", "scores": [3, 4]},
                ],
                pd.DataFrame(
                    {
                        "column": ["age", "name"],
                        "count": [2, 2],
                        "mean": [27.5, np.nan],
                        "min": [25.0, np.nan],
                        "max": [30.0, np.nan],
                        "std": [2.5, np.nan],
                        "missing_pct": [0.0, 0.0],
                        "zero_pct": [0.0, np.nan],
                    }
                ).astype(
                    {
                        "column": "object",
                        "count": "int64",
                        "mean": "float64",
                        "min": "float64",
                        "max": "float64",
                        "std": "float64",
                        "missing_pct": "float64",
                        "zero_pct": "float64",
                    }
                ),
                ["age", "name"],
                id="column_filtering",
            ),
            pytest.param(
                "single_row",
                [{"value": 42, "text": "hello"}],
                pd.DataFrame(
                    {
                        "column": ["value", "text"],
                        "count": [1, 1],
                        "mean": [42.0, np.nan],
                        "min": [42.0, np.nan],
                        "max": [42.0, np.nan],
                        "std": [0.0, np.nan],
                        "missing_pct": [0.0, 0.0],
                        "zero_pct": [0.0, np.nan],
                    }
                ).astype(
                    {
                        "column": "object",
                        "count": "int64",
                        "mean": "float64",
                        "min": "float64",
                        "max": "float64",
                        "std": "float64",
                        "missing_pct": "float64",
                        "zero_pct": "float64",
                    }
                ),
                None,
                id="single_row",
            ),
        ],
    )
    def test_summary_functionality(self, test_case, data, expected_df, columns):
        """Test summary functionality with various data scenarios."""
        ds = ray.data.from_items(data)
        summary = ds.summary(columns=columns)
        actual_df = summary.to_pandas()
        pd.testing.assert_frame_equal(actual_df, expected_df, rtol=1e-5)

    def test_summary_empty_dataset(self):
        """Test summary on empty dataset raises ValueError."""
        ds = ray.data.from_items([])

        with pytest.raises(
            ValueError,
            match="Dataset must have a schema to determine column types",
        ):
            ds.summary()

    def test_summary_dtype_organization(self):
        """Test that summary organizes stats by arrow dtype."""
        # Create arrays with specific dtypes
        int8_array = pa.array([1, 2, 3], type=pa.int8())
        int64_array = pa.array([100, 200, 300], type=pa.int64())
        float_array = pa.array([1.5, 2.5, 3.5], type=pa.float64())
        str_array = pa.array(["a", "b", "c"], type=pa.string())

        # Create table with specific schema
        table = pa.table(
            {
                "int8_col": int8_array,
                "int64_col": int64_array,
                "float_col": float_array,
                "str_col": str_array,
            }
        )

        ds = ray.data.from_arrow(table)
        summary = ds.summary()
        actual_df = summary.to_pandas()

        # Verify that each column has its correct dtype
        assert len(actual_df) == 4

    def test_summary_repr(self):
        """Test that summary has a repr."""
        data = [{"value": 1}]
        ds = ray.data.from_items(data)
        summary = ds.summary()

        # Should have a repr
        repr_str = repr(summary)
        assert repr_str is not None
        assert len(repr_str) > 0


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
