import pyarrow as pa
import pytest
from packaging.version import parse as parse_version

import ray
from ray.data.aggregate import (
    ApproximateQuantile,
    ApproximateTopK,
    Count,
    Max,
    Mean,
    Min,
    MissingValuePercentage,
    Std,
    ZeroPercentage,
)
from ray.data.stats import (
    FeatureAggregators,
    categorical_aggregators,
    feature_aggregators_for_dataset,
    numerical_aggregators,
    vector_aggregators,
)
from ray.data.tests.conftest import get_pyarrow_version


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
        feature_aggs = feature_aggregators_for_dataset(ds)

        # Check that numerical columns are identified
        assert "int_col" in feature_aggs.numerical_columns
        assert "float_col" in feature_aggs.numerical_columns
        assert "decimal_col" in feature_aggs.numerical_columns
        assert "string_col" not in feature_aggs.numerical_columns

        # Check that string columns are identified
        assert "string_col" in feature_aggs.str_columns
        assert "int_col" not in feature_aggs.str_columns

        # Check that no vector columns are identified
        assert len(feature_aggs.vector_columns) == 0

        # Check that we have the right number of aggregators
        # 3 numerical columns * 8 aggregators each + 1 string column * 3 aggregators = 27 total
        assert len(feature_aggs.aggregators) == 27

    def test_categorical_columns_detection(self):
        """Test that string columns are correctly identified as categorical."""
        data = [
            {"category": "A", "name": "Alice", "value": 1},
            {"category": "B", "name": "Bob", "value": 2},
            {"category": "A", "name": "Charlie", "value": 3},
        ]

        ds = ray.data.from_items(data)
        feature_aggs = feature_aggregators_for_dataset(ds)

        # Check categorical columns
        assert "category" in feature_aggs.str_columns
        assert "name" in feature_aggs.str_columns
        assert "value" not in feature_aggs.str_columns

        # Check numerical columns
        assert "value" in feature_aggs.numerical_columns
        assert "category" not in feature_aggs.numerical_columns

        # Check aggregator count: 1 numerical * 8 + 3 categorical * 2 = 14
        assert len(feature_aggs.aggregators) == 14

    def test_vector_columns_detection(self):
        """Test that list columns are correctly identified as vector columns."""
        data = [
            {"vector": [1, 2, 3], "scalar": 1, "text": "hello"},
            {"vector": [4, 5, 6], "scalar": 2, "text": "world"},
            {"vector": [7, 8, 9], "scalar": 3, "text": "test"},
        ]

        ds = ray.data.from_items(data)
        feature_aggs = feature_aggregators_for_dataset(ds)

        # Check vector columns
        assert "vector" in feature_aggs.vector_columns
        assert "scalar" not in feature_aggs.vector_columns
        assert "text" not in feature_aggs.vector_columns

        # Check other column types
        assert "scalar" in feature_aggs.numerical_columns
        assert "text" in feature_aggs.str_columns

        # Check aggregator count: 1 numerical * 8 + 1 categorical * 3 + 1 vector * 2 = 12
        assert len(feature_aggs.aggregators) == 13

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
        feature_aggs = feature_aggregators_for_dataset(ds)

        # Check column classification
        assert "int_val" in feature_aggs.numerical_columns
        assert "float_val" in feature_aggs.numerical_columns
        assert "string_val" in feature_aggs.str_columns
        assert "vector_val" in feature_aggs.vector_columns
        # bool_val should be treated as numerical (integer-like)
        assert "bool_val" in feature_aggs.numerical_columns

        # Check aggregator count: 3 numerical * 8 + 1 categorical * 3 + 1 vector * 2 = 29
        assert len(feature_aggs.aggregators) == 29

    def test_column_filtering(self):
        """Test that only specified columns are included when columns parameter is provided."""
        data = [
            {"col1": 1, "col2": "a", "col3": [1, 2], "col4": 1.5},
            {"col1": 2, "col2": "b", "col3": [3, 4], "col4": 2.5},
        ]

        ds = ray.data.from_items(data)

        # Test with specific columns
        feature_aggs = feature_aggregators_for_dataset(ds, columns=["col1", "col3"])

        # Should only include col1 and col3
        assert "col1" in feature_aggs.numerical_columns
        assert "col2" not in feature_aggs.str_columns
        assert "col3" in feature_aggs.vector_columns
        assert "col4" not in feature_aggs.numerical_columns

        # Check aggregator count: 1 numerical * 8 + 1 vector * 2 = 10
        assert len(feature_aggs.aggregators) == 10

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

    @pytest.mark.skipif(
        get_pyarrow_version() < parse_version("20.0.0"),
        reason="Test requires PyArrow >= 20.0.0",
    )
    def test_unsupported_column_types(self):
        """Test that unsupported column types are handled gracefully."""

        table = pa.table(
            {
                "supported_int": [1, 2, 3],
                "supported_string": ["a", "b", "c"],
                "unsupported_timestamp": [pa.scalar(0, type=pa.timestamp("us"))] * 3,
                "unsupported_binary": [b"data"] * 3,
            }
        )

        ds = ray.data.from_arrow(table)
        feature_aggs = feature_aggregators_for_dataset(ds)

        # Only supported types should be included
        assert "supported_int" in feature_aggs.numerical_columns
        assert "supported_string" in feature_aggs.str_columns
        assert "unsupported_timestamp" not in feature_aggs.numerical_columns
        assert "unsupported_timestamp" not in feature_aggs.str_columns
        assert "unsupported_timestamp" not in feature_aggs.vector_columns
        assert "unsupported_binary" not in feature_aggs.numerical_columns
        assert "unsupported_binary" not in feature_aggs.str_columns
        assert "unsupported_binary" not in feature_aggs.vector_columns

        # Check aggregator count: 1 numerical * 8 + 1 categorical * 3 = 11
        assert len(feature_aggs.aggregators) == 11

    def test_aggregator_types_verification(self):
        """Test that the correct aggregator types are generated for each column type."""
        data = [
            {"num": 1, "cat": "a", "vec": [1, 2]},
            {"num": 2, "cat": "b", "vec": [3, 4]},
        ]

        ds = ray.data.from_items(data)
        feature_aggs = feature_aggregators_for_dataset(ds)

        # Check that we have the right types of aggregators
        agg_names = [agg.name for agg in feature_aggs.aggregators]

        # Numerical aggregators should include all 8 types
        num_agg_names = [name for name in agg_names if "num" in name]
        assert len(num_agg_names) == 8
        assert any("count" in name.lower() for name in num_agg_names)
        assert any("mean" in name.lower() for name in num_agg_names)
        assert any("min" in name.lower() for name in num_agg_names)
        assert any("max" in name.lower() for name in num_agg_names)
        assert any("std" in name.lower() for name in num_agg_names)
        assert any("missing" in name.lower() for name in num_agg_names)
        assert any("zero" in name.lower() for name in num_agg_names)
        assert any("approx_quantile" in name.lower() for name in num_agg_names)

        # Categorical aggregators should include count and missing percentage
        cat_agg_names = [name for name in agg_names if "cat" in name]
        assert len(cat_agg_names) == 3
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
        feature_aggs = feature_aggregators_for_dataset(ds)

        # Find aggregators for the numerical column
        num_aggs = [agg for agg in feature_aggs.aggregators if "num" in agg.name]
        assert len(num_aggs) == 8

        # Check that we have the right aggregator types
        agg_types = [type(agg) for agg in num_aggs]
        assert Count in agg_types
        assert Mean in agg_types
        assert Min in agg_types
        assert Max in agg_types
        assert Std in agg_types
        assert MissingValuePercentage in agg_types
        assert ZeroPercentage in agg_types
        assert ApproximateQuantile in agg_types

        # Find aggregators for the categorical column
        cat_aggs = [agg for agg in feature_aggs.aggregators if "cat" in agg.name]
        assert len(cat_aggs) == 3

        # Check that we have the right aggregator types for categorical
        cat_agg_types = [type(agg) for agg in cat_aggs]
        assert Count in cat_agg_types
        assert MissingValuePercentage in cat_agg_types
        assert ApproximateTopK in cat_agg_types
        # Should not have numerical aggregators for categorical columns
        assert Mean not in cat_agg_types
        assert Min not in cat_agg_types
        assert Max not in cat_agg_types
        assert Std not in cat_agg_types
        assert ZeroPercentage not in cat_agg_types

    def test_return_dataclass_structure(self):
        """Test that the function returns the correct FeatureAggregators dataclass."""
        data = [{"num": 1, "cat": "a", "vec": [1, 2]}]
        ds = ray.data.from_items(data)
        result = feature_aggregators_for_dataset(ds)

        # Should return a FeatureAggregators dataclass
        assert isinstance(result, FeatureAggregators)

        # Check that attributes exist and are lists
        assert isinstance(result.numerical_columns, list)
        assert isinstance(result.str_columns, list)
        assert isinstance(result.vector_columns, list)
        assert isinstance(result.aggregators, list)

        # Check that column names are strings
        for col in (
            result.numerical_columns + result.str_columns + result.vector_columns
        ):
            assert isinstance(col, str)

        # Check that aggregators have required attributes
        for agg in result.aggregators:
            assert hasattr(agg, "name")
            assert hasattr(agg, "get_target_column")

    def test_none_columns_parameter(self):
        """Test that None columns parameter includes all columns."""
        data = [{"col1": 1, "col2": "a"}]
        ds = ray.data.from_items(data)

        # Test with None (should be same as not providing columns parameter)
        result1 = feature_aggregators_for_dataset(ds, columns=None)
        result2 = feature_aggregators_for_dataset(ds)

        # Compare the dataclass attributes
        assert result1.numerical_columns == result2.numerical_columns
        assert result1.str_columns == result2.str_columns
        assert result1.vector_columns == result2.vector_columns
        assert len(result1.aggregators) == len(result2.aggregators)

    def test_empty_columns_list(self):
        """Test behavior with empty columns list."""
        data = [{"col1": 1, "col2": "a"}]
        ds = ray.data.from_items(data)

        feature_aggs = feature_aggregators_for_dataset(ds, columns=[])

        # Should have no columns and no aggregators
        assert len(feature_aggs.numerical_columns) == 0
        assert len(feature_aggs.str_columns) == 0
        assert len(feature_aggs.vector_columns) == 0
        assert len(feature_aggs.aggregators) == 0

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
        feature_aggs = feature_aggregators_for_dataset(ds)

        # Verify results
        assert "id" in feature_aggs.numerical_columns
        assert "value" in feature_aggs.numerical_columns
        assert "category" in feature_aggs.str_columns
        assert "vector" in feature_aggs.vector_columns

        # Check aggregator count: 2 numerical * 8 + 1 categorical * 3 + 1 vector * 2 = 21
        assert len(feature_aggs.aggregators) == 21


class TestIndividualAggregatorFunctions:
    """Test suite for individual aggregator functions."""

    def test_numerical_aggregators(self):
        """Test numerical_aggregators function."""
        aggs = numerical_aggregators("test_column")

        assert len(aggs) == 8
        assert all(hasattr(agg, "get_target_column") for agg in aggs)
        assert all(agg.get_target_column() == "test_column" for agg in aggs)

        # Check aggregator types
        agg_types = [type(agg) for agg in aggs]
        assert Count in agg_types
        assert Mean in agg_types
        assert Min in agg_types
        assert Max in agg_types
        assert Std in agg_types
        assert MissingValuePercentage in agg_types
        assert ZeroPercentage in agg_types
        assert ApproximateQuantile in agg_types

    def test_categorical_aggregators(self):
        """Test categorical_aggregators function."""
        aggs = categorical_aggregators("test_column")

        assert len(aggs) == 3
        assert all(hasattr(agg, "get_target_column") for agg in aggs)
        assert all(agg.get_target_column() == "test_column" for agg in aggs)

        # Check aggregator types
        agg_types = [type(agg) for agg in aggs]
        assert Count in agg_types
        assert MissingValuePercentage in agg_types
        assert ApproximateTopK in agg_types

    def test_vector_aggregators(self):
        """Test vector_aggregators function."""
        aggs = vector_aggregators("test_column")

        assert len(aggs) == 2
        assert all(hasattr(agg, "get_target_column") for agg in aggs)
        assert all(agg.get_target_column() == "test_column" for agg in aggs)

        # Check aggregator types
        agg_types = [type(agg) for agg in aggs]
        assert Count in agg_types
        assert MissingValuePercentage in agg_types


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
