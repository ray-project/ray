import pytest
import pyarrow as pa
import ray
from ray.data import Dataset
from ray.data.stats import (
    feature_aggregators_for_dataset,
    numerical_aggregators,
    categorical_aggregators,
    vector_aggregators,
)
from ray.data.aggregate import (
    Count,
    Mean,
    Min,
    Max,
    Std,
    ZeroPercentage,
    MissingValuePercentage,
)


class TestFeatureAggregatorsForDataset:
    """Test suite for feature_aggregators_for_dataset function."""

    def test_numerical_columns_detection(self):
        """Test that numerical columns are correctly identified and get appropriate aggregators."""
        # Create a dataset with various numerical types
        data = [
            {"int_col": 1, "float_col": 1.5, "decimal_col": 2.3, "string_col": "a"},
            {"int_col": 2, "float_col": 2.5, "decimal_col": 3.3, "string_col": "b"},
            {"int_col": 3, "float_col": 3.5, "decimal_col": 4.3, "string_col": "c"},
        ]

        ds = ray.data.from_items(data)
        (
            numerical_cols,
            str_cols,
            vector_cols,
            all_aggs,
        ) = feature_aggregators_for_dataset(ds)

        # Check that numerical columns are identified
        assert "int_col" in numerical_cols
        assert "float_col" in numerical_cols
        assert "decimal_col" in numerical_cols
        assert "string_col" not in numerical_cols

        # Check that string columns are identified
        assert "string_col" in str_cols
        assert "int_col" not in str_cols

        # Check that no vector columns are identified
        assert len(vector_cols) == 0

        # Check that we have the right number of aggregators
        # 3 numerical columns * 7 aggregators each + 1 string column * 2 aggregators = 23 total
        assert len(all_aggs) == 23

    def test_categorical_columns_detection(self):
        """Test that string columns are correctly identified as categorical."""
        data = [
            {"category": "A", "name": "Alice", "value": 1},
            {"category": "B", "name": "Bob", "value": 2},
            {"category": "A", "name": "Charlie", "value": 3},
        ]

        ds = ray.data.from_items(data)
        (
            numerical_cols,
            str_cols,
            vector_cols,
            all_aggs,
        ) = feature_aggregators_for_dataset(ds)

        # Check categorical columns
        assert "category" in str_cols
        assert "name" in str_cols
        assert "value" not in str_cols

        # Check numerical columns
        assert "value" in numerical_cols
        assert "category" not in numerical_cols

        # Check aggregator count: 1 numerical * 7 + 2 categorical * 2 = 11
        assert len(all_aggs) == 11

    def test_vector_columns_detection(self):
        """Test that list columns are correctly identified as vector columns."""
        data = [
            {"vector": [1, 2, 3], "scalar": 1, "text": "hello"},
            {"vector": [4, 5, 6], "scalar": 2, "text": "world"},
            {"vector": [7, 8, 9], "scalar": 3, "text": "test"},
        ]

        ds = ray.data.from_items(data)
        (
            numerical_cols,
            str_cols,
            vector_cols,
            all_aggs,
        ) = feature_aggregators_for_dataset(ds)

        # Check vector columns
        assert "vector" in vector_cols
        assert "scalar" not in vector_cols
        assert "text" not in vector_cols

        # Check other column types
        assert "scalar" in numerical_cols
        assert "text" in str_cols

        # Check aggregator count: 1 numerical * 7 + 1 categorical * 2 + 1 vector * 2 = 11
        assert len(all_aggs) == 11

    def test_mixed_column_types(self):
        """Test dataset with all column types mixed together."""
        data = [
            {
                "int_val": 1,
                "float_val": 1.5,
                "string_val": "a",
                "vector_val": [1, 2],
                "bool_val": True,
            },
            {
                "int_val": 2,
                "float_val": 2.5,
                "string_val": "b",
                "vector_val": [3, 4],
                "bool_val": False,
            },
        ]

        ds = ray.data.from_items(data)
        (
            numerical_cols,
            str_cols,
            vector_cols,
            all_aggs,
        ) = feature_aggregators_for_dataset(ds)

        # Check column classification
        assert "int_val" in numerical_cols
        assert "float_val" in numerical_cols
        assert "string_val" in str_cols
        assert "vector_val" in vector_cols
        # bool_val should be treated as numerical (integer-like)
        assert "bool_val" in numerical_cols

        # Check aggregator count: 3 numerical * 7 + 1 categorical * 2 + 1 vector * 2 = 25
        assert len(all_aggs) == 25

    def test_column_filtering(self):
        """Test that only specified columns are included when columns parameter is provided."""
        data = [
            {"col1": 1, "col2": "a", "col3": [1, 2], "col4": 1.5},
            {"col1": 2, "col2": "b", "col3": [3, 4], "col4": 2.5},
        ]

        ds = ray.data.from_items(data)

        # Test with specific columns
        (
            numerical_cols,
            str_cols,
            vector_cols,
            all_aggs,
        ) = feature_aggregators_for_dataset(ds, columns=["col1", "col3"])

        # Should only include col1 and col3
        assert "col1" in numerical_cols
        assert "col2" not in str_cols
        assert "col3" in vector_cols
        assert "col4" not in numerical_cols

        # Check aggregator count: 1 numerical * 7 + 1 vector * 2 = 9
        assert len(all_aggs) == 9

    def test_empty_dataset_schema(self):
        """Test behavior with empty dataset that has no schema."""
        # Create an empty dataset
        ds = ray.data.from_items([])

        with pytest.raises(ValueError, match="Dataset must have a schema"):
            feature_aggregators_for_dataset(ds)

    def test_invalid_columns_parameter(self):
        """Test error handling when columns parameter contains non-existent columns."""
        data = [{"col1": 1, "col2": "a"}]
        ds = ray.data.from_items(data)

        with pytest.raises(ValueError, match="Columns .* not found in dataset schema"):
            feature_aggregators_for_dataset(ds, columns=["col1", "nonexistent_col"])

    def test_unsupported_column_types(self):
        """Test that unsupported column types are handled gracefully."""
        # Create a dataset with unsupported types by using PyArrow directly
        schema = pa.schema(
            [
                ("supported_int", pa.int64()),
                ("supported_string", pa.string()),
                ("unsupported_timestamp", pa.timestamp("us")),
                ("unsupported_binary", pa.binary()),
            ]
        )

        table = pa.table(
            {
                "supported_int": [1, 2, 3],
                "supported_string": ["a", "b", "c"],
                "unsupported_timestamp": [pa.scalar(0, type=pa.timestamp("us"))] * 3,
                "unsupported_binary": [b"data"] * 3,
            }
        )

        ds = ray.data.from_arrow(table)
        (
            numerical_cols,
            str_cols,
            vector_cols,
            all_aggs,
        ) = feature_aggregators_for_dataset(ds)

        # Only supported types should be included
        assert "supported_int" in numerical_cols
        assert "supported_string" in str_cols
        assert "unsupported_timestamp" not in numerical_cols
        assert "unsupported_timestamp" not in str_cols
        assert "unsupported_timestamp" not in vector_cols
        assert "unsupported_binary" not in numerical_cols
        assert "unsupported_binary" not in str_cols
        assert "unsupported_binary" not in vector_cols

        # Check aggregator count: 1 numerical * 7 + 1 categorical * 2 = 9
        assert len(all_aggs) == 9

    def test_aggregator_types_verification(self):
        """Test that the correct aggregator types are generated for each column type."""
        data = [
            {"num": 1, "cat": "a", "vec": [1, 2]},
            {"num": 2, "cat": "b", "vec": [3, 4]},
        ]

        ds = ray.data.from_items(data)
        (
            numerical_cols,
            str_cols,
            vector_cols,
            all_aggs,
        ) = feature_aggregators_for_dataset(ds)

        # Check that we have the right types of aggregators
        agg_names = [agg.name for agg in all_aggs]

        # Numerical aggregators should include all 7 types
        num_agg_names = [name for name in agg_names if "num" in name]
        assert len(num_agg_names) == 7
        assert any("count" in name.lower() for name in num_agg_names)
        assert any("mean" in name.lower() for name in num_agg_names)
        assert any("min" in name.lower() for name in num_agg_names)
        assert any("max" in name.lower() for name in num_agg_names)
        assert any("std" in name.lower() for name in num_agg_names)
        assert any("missing" in name.lower() for name in num_agg_names)
        assert any("zero" in name.lower() for name in num_agg_names)

        # Categorical aggregators should include count and missing percentage
        cat_agg_names = [name for name in agg_names if "cat" in name]
        assert len(cat_agg_names) == 2
        assert any("count" in name.lower() for name in cat_agg_names)
        assert any("missing" in name.lower() for name in cat_agg_names)

        # Vector aggregators should include count and missing percentage
        vec_agg_names = [name for name in agg_names if "vec" in name]
        assert len(vec_agg_names) == 2
        assert any("count" in name.lower() for name in vec_agg_names)
        assert any("missing" in name.lower() for name in vec_agg_names)

    def test_aggregator_instances_verification(self):
        """Test that the actual aggregator instances are of the correct types."""
        data = [{"num": 1, "cat": "a"}]
        ds = ray.data.from_items(data)
        (
            numerical_cols,
            str_cols,
            vector_cols,
            all_aggs,
        ) = feature_aggregators_for_dataset(ds)

        # Find aggregators for the numerical column
        num_aggs = [agg for agg in all_aggs if "num" in agg.name]
        assert len(num_aggs) == 7

        # Check that we have the right aggregator types
        agg_types = [type(agg) for agg in num_aggs]
        assert Count in agg_types
        assert Mean in agg_types
        assert Min in agg_types
        assert Max in agg_types
        assert Std in agg_types
        assert MissingValuePercentage in agg_types
        assert ZeroPercentage in agg_types

        # Find aggregators for the categorical column
        cat_aggs = [agg for agg in all_aggs if "cat" in agg.name]
        assert len(cat_aggs) == 2

        # Check that we have the right aggregator types for categorical
        cat_agg_types = [type(agg) for agg in cat_aggs]
        assert Count in cat_agg_types
        assert MissingValuePercentage in cat_agg_types
        # Should not have numerical aggregators for categorical columns
        assert Mean not in cat_agg_types
        assert Min not in cat_agg_types
        assert Max not in cat_agg_types
        assert Std not in cat_agg_types
        assert ZeroPercentage not in cat_agg_types

    def test_return_tuple_structure(self):
        """Test that the function returns the correct tuple structure."""
        data = [{"num": 1, "cat": "a", "vec": [1, 2]}]
        ds = ray.data.from_items(data)
        result = feature_aggregators_for_dataset(ds)

        # Should return a tuple with 4 elements
        assert isinstance(result, tuple)
        assert len(result) == 4

        numerical_cols, str_cols, vector_cols, all_aggs = result

        # Each should be a list
        assert isinstance(numerical_cols, list)
        assert isinstance(str_cols, list)
        assert isinstance(vector_cols, list)
        assert isinstance(all_aggs, list)

        # Check that column names are strings
        for col in numerical_cols + str_cols + vector_cols:
            assert isinstance(col, str)

        # Check that aggregators are AggregateFnV2 instances
        for agg in all_aggs:
            assert hasattr(agg, "name")
            assert hasattr(agg, "on")

    def test_none_columns_parameter(self):
        """Test that None columns parameter includes all columns."""
        data = [{"col1": 1, "col2": "a"}]
        ds = ray.data.from_items(data)

        # Test with None (should be same as not providing columns parameter)
        result1 = feature_aggregators_for_dataset(ds, columns=None)
        result2 = feature_aggregators_for_dataset(ds)

        assert result1 == result2

    def test_empty_columns_list(self):
        """Test behavior with empty columns list."""
        data = [{"col1": 1, "col2": "a"}]
        ds = ray.data.from_items(data)

        (
            numerical_cols,
            str_cols,
            vector_cols,
            all_aggs,
        ) = feature_aggregators_for_dataset(ds, columns=[])

        # Should have no columns and no aggregators
        assert len(numerical_cols) == 0
        assert len(str_cols) == 0
        assert len(vector_cols) == 0
        assert len(all_aggs) == 0

    def test_large_dataset_performance(self):
        """Test performance with a larger dataset to ensure it scales reasonably."""
        # Create a larger dataset
        data = []
        for i in range(1000):
            data.append(
                {
                    "id": i,
                    "value": i * 1.5,
                    "category": f"cat_{i % 10}",
                    "vector": [i, i + 1, i + 2],
                }
            )

        ds = ray.data.from_items(data)

        # Should complete without issues
        (
            numerical_cols,
            str_cols,
            vector_cols,
            all_aggs,
        ) = feature_aggregators_for_dataset(ds)

        # Verify results
        assert "id" in numerical_cols
        assert "value" in numerical_cols
        assert "category" in str_cols
        assert "vector" in vector_cols

        # Check aggregator count: 2 numerical * 7 + 1 categorical * 2 + 1 vector * 2 = 18
        assert len(all_aggs) == 18


class TestIndividualAggregatorFunctions:
    """Test suite for individual aggregator functions."""

    def test_numerical_aggregators(self):
        """Test numerical_aggregators function."""
        aggs = numerical_aggregators("test_column")

        assert len(aggs) == 7
        assert all(hasattr(agg, "on") for agg in aggs)
        assert all(agg.on == "test_column" for agg in aggs)

        # Check aggregator types
        agg_types = [type(agg) for agg in aggs]
        assert Count in agg_types
        assert Mean in agg_types
        assert Min in agg_types
        assert Max in agg_types
        assert Std in agg_types
        assert MissingValuePercentage in agg_types
        assert ZeroPercentage in agg_types

    def test_categorical_aggregators(self):
        """Test categorical_aggregators function."""
        aggs = categorical_aggregators("test_column")

        assert len(aggs) == 2
        assert all(hasattr(agg, "on") for agg in aggs)
        assert all(agg.on == "test_column" for agg in aggs)

        # Check aggregator types
        agg_types = [type(agg) for agg in aggs]
        assert Count in agg_types
        assert MissingValuePercentage in agg_types

    def test_vector_aggregators(self):
        """Test vector_aggregators function."""
        aggs = vector_aggregators("test_column")

        assert len(aggs) == 2
        assert all(hasattr(agg, "on") for agg in aggs)
        assert all(agg.on == "test_column" for agg in aggs)

        # Check aggregator types
        agg_types = [type(agg) for agg in aggs]
        assert Count in agg_types
        assert MissingValuePercentage in agg_types


if __name__ == "__main__":
    import sys
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
