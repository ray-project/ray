import pandas as pd
import pyarrow as pa
import pytest
from packaging.version import parse as parse_version

import ray
from ray._private.arrow_utils import get_pyarrow_version
from ray.data._internal.util import rows_same
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
from ray.data.datatype import DataType
from ray.data.stats import (
    DatasetSummary,
    _basic_aggregators,
    _default_dtype_aggregators,
    _dtype_aggregators_for_dataset,
    _numerical_aggregators,
    _temporal_aggregators,
)


class TestDtypeAggregatorsForDataset:
    """Test suite for _dtype_aggregators_for_dataset function."""

    @pytest.mark.parametrize(
        "data,expected_dtypes,expected_agg_count",
        [
            # Numerical columns only
            (
                [{"int_col": 1, "float_col": 1.5}],
                {
                    "int_col": "DataType(arrow:int64)",
                    "float_col": "DataType(arrow:double)",
                },
                16,  # 2 columns * 8 aggregators each
            ),
            # Mixed numerical and string
            (
                [{"num": 1, "str": "a"}],
                {"num": "DataType(arrow:int64)", "str": "DataType(arrow:string)"},
                11,  # 1 numerical * 8 + 1 string * 3
            ),
            # Boolean treated as numerical
            (
                [{"bool_col": True, "int_col": 1}],
                {
                    "bool_col": "DataType(arrow:bool)",
                    "int_col": "DataType(arrow:int64)",
                },
                16,  # 2 columns * 8 aggregators each
            ),
        ],
    )
    def test_column_type_detection(self, data, expected_dtypes, expected_agg_count):
        """Test that column types are correctly detected and mapped."""
        ds = ray.data.from_items(data)
        result = _dtype_aggregators_for_dataset(ds.schema())

        assert (result.column_to_dtype, len(result.aggregators)) == (
            expected_dtypes,
            expected_agg_count,
        )

    def test_column_filtering(self):
        """Test that only specified columns are included."""
        data = [{"col1": 1, "col2": "a", "col3": 1.5}]
        ds = ray.data.from_items(data)

        result = _dtype_aggregators_for_dataset(ds.schema(), columns=["col1", "col3"])

        assert (set(result.column_to_dtype.keys()), len(result.aggregators)) == (
            {"col1", "col3"},
            16,
        )

    def test_empty_columns_list(self):
        """Test behavior with empty columns list."""
        data = [{"col1": 1, "col2": "a"}]
        ds = ray.data.from_items(data)

        result = _dtype_aggregators_for_dataset(ds.schema(), columns=[])

        assert (len(result.column_to_dtype), len(result.aggregators)) == (0, 0)

    def test_invalid_columns_raises_error(self):
        """Test error handling when columns parameter contains non-existent columns."""
        data = [{"col1": 1}]
        ds = ray.data.from_items(data)

        with pytest.raises(ValueError, match="not found in dataset schema"):
            _dtype_aggregators_for_dataset(ds.schema(), columns=["nonexistent"])

    def test_none_schema_raises_error(self):
        """Test that None schema raises appropriate error."""
        with pytest.raises(ValueError, match="must have a schema"):
            _dtype_aggregators_for_dataset(None)

    def test_custom_dtype_mapping(self):
        """Test that custom dtype mappings override defaults."""
        data = [{"int_col": 1}]
        ds = ray.data.from_items(data)

        # Override int64 to only use Count and Mean
        custom_mapping = {DataType.int64(): lambda col: [Count(on=col), Mean(on=col)]}

        result = _dtype_aggregators_for_dataset(
            ds.schema(), dtype_agg_mapping=custom_mapping
        )

        assert [type(agg) for agg in result.aggregators] == [Count, Mean]

    @pytest.mark.skipif(
        get_pyarrow_version() < parse_version("14.0.0"),
        reason="Requires pyarrow >= 14.0.0",
    )
    def test_custom_dtype_mapping_pattern_precedence(self):
        """Test that specific custom mappings take precedence over default patterns."""
        import datetime

        # Use from_arrow to ensure we get exactly timestamp[us]
        t = pa.table(
            {"ts": pa.array([datetime.datetime(2024, 1, 1)], type=pa.timestamp("us"))}
        )
        ds = ray.data.from_arrow(t)

        # Override specific timestamp type to only use Count
        # Default for temporal is [Count, Min, Max, MissingValuePercentage]
        ts_dtype = DataType.from_arrow(pa.timestamp("us"))
        custom_mapping = {ts_dtype: lambda col: [Count(on=col)]}

        result = _dtype_aggregators_for_dataset(
            ds.schema(), dtype_agg_mapping=custom_mapping
        )

        # Should only have 1 aggregator if our specific override was used.
        # If the default DataType.temporal() pattern matched first, we'd get 4 aggregators.
        assert len(result.aggregators) == 1
        assert isinstance(result.aggregators[0], Count)

    @pytest.mark.skipif(
        get_pyarrow_version() < parse_version("14.0.0"),
        reason="Requires pyarrow >= 14.0.0",
    )
    @pytest.mark.parametrize(
        "pa_type",
        [
            pa.timestamp("us"),  # Temporal: count, min, max, missing%
            pa.date32(),  # Temporal
            pa.time64("us"),  # Temporal
        ],
    )
    def test_temporal_types(self, pa_type):
        """Test that temporal types get appropriate aggregators."""
        table = pa.table({"temporal_col": pa.array([1, 2, 3], type=pa_type)})
        ds = ray.data.from_arrow(table)

        result = _dtype_aggregators_for_dataset(ds.schema())

        assert "temporal_col" in result.column_to_dtype
        assert [type(agg) for agg in result.aggregators] == [
            Count,
            Min,
            Max,
            MissingValuePercentage,
        ]


class TestIndividualAggregatorFunctions:
    """Test suite for individual aggregator generator functions."""

    def test_numerical_aggregators(self):
        """Test _numerical_aggregators function."""
        aggs = _numerical_aggregators("test_col")

        assert len(aggs) == 8
        assert all(agg.get_target_column() == "test_col" for agg in aggs)
        assert [type(agg) for agg in aggs] == [
            Count,
            Mean,
            Min,
            Max,
            Std,
            ApproximateQuantile,
            MissingValuePercentage,
            ZeroPercentage,
        ]

    def test_temporal_aggregators(self):
        """Test _temporal_aggregators function."""
        aggs = _temporal_aggregators("test_col")

        assert len(aggs) == 4
        assert all(agg.get_target_column() == "test_col" for agg in aggs)
        assert [type(agg) for agg in aggs] == [Count, Min, Max, MissingValuePercentage]

    def test_basic_aggregators(self):
        """Test _basic_aggregators function."""
        aggs = _basic_aggregators("test_col")

        assert len(aggs) == 3
        assert all(agg.get_target_column() == "test_col" for agg in aggs)
        assert [type(agg) for agg in aggs] == [
            Count,
            MissingValuePercentage,
            ApproximateTopK,
        ]


class TestDefaultDtypeAggregators:
    """Test suite for _default_dtype_aggregators function."""

    @pytest.mark.skipif(
        get_pyarrow_version() < parse_version("14.0.0"),
        reason="Requires pyarrow >= 14.0.0",
    )
    @pytest.mark.parametrize(
        "dtype_factory,expected_agg_types,uses_pattern_matching",
        [
            (
                DataType.int32,
                [
                    Count,
                    Mean,
                    Min,
                    Max,
                    Std,
                    ApproximateQuantile,
                    MissingValuePercentage,
                    ZeroPercentage,
                ],
                False,
            ),  # Numerical
            (
                DataType.float64,
                [
                    Count,
                    Mean,
                    Min,
                    Max,
                    Std,
                    ApproximateQuantile,
                    MissingValuePercentage,
                    ZeroPercentage,
                ],
                False,
            ),  # Numerical
            (
                DataType.bool,
                [
                    Count,
                    Mean,
                    Min,
                    Max,
                    Std,
                    ApproximateQuantile,
                    MissingValuePercentage,
                    ZeroPercentage,
                ],
                False,
            ),  # Numerical
            (
                DataType.string,
                [Count, MissingValuePercentage, ApproximateTopK],
                False,
            ),  # Basic
            (
                DataType.binary,
                [Count, MissingValuePercentage, ApproximateTopK],
                False,
            ),  # Basic
            (
                lambda: DataType.temporal("timestamp", unit="us"),
                [Count, Min, Max, MissingValuePercentage],
                True,
            ),  # Temporal (pattern matched)
            (
                lambda: DataType.temporal("date32"),
                [Count, Min, Max, MissingValuePercentage],
                True,
            ),  # Temporal (pattern matched)
            (
                lambda: DataType.temporal("time64", unit="us"),
                [Count, Min, Max, MissingValuePercentage],
                True,
            ),  # Temporal (pattern matched)
        ],
    )
    def test_default_mappings(
        self, dtype_factory, expected_agg_types, uses_pattern_matching
    ):
        """Test that default mappings return correct aggregators."""
        from ray.data.datatype import TypeCategory

        mapping = _default_dtype_aggregators()
        dtype = dtype_factory()

        if uses_pattern_matching:
            # For pattern-matched types (like temporal), find the matching factory
            factory = None
            for mapping_key, mapping_factory in mapping.items():
                if isinstance(mapping_key, (TypeCategory, str)) and dtype.is_of(
                    mapping_key
                ):
                    factory = mapping_factory
                    break
            assert (
                factory is not None
            ), f"Type {dtype} should match a pattern in the mapping"
        else:
            # For exact matches, directly access the mapping
            assert dtype in mapping
            factory = mapping[dtype]

        # Call the factory with a test column to get aggregators
        aggs = factory("test_col")
        assert [type(agg) for agg in aggs] == expected_agg_types


class TestDatasetSummary:
    """Test suite for Dataset.summary() method."""

    def test_basic_summary(self):
        """Test basic summary computation."""
        ds = ray.data.from_items(
            [
                {"age": 25, "name": "Alice"},
                {"age": 30, "name": "Bob"},
            ]
        )

        summary = ds.summary()
        actual = summary.to_pandas()

        # Verify columns are present
        assert "age" in actual.columns
        assert "name" in actual.columns

        # Check key statistics using rows_same
        actual_subset = actual[
            actual["statistic"].isin(["count", "mean", "min", "max"])
        ].copy()
        actual_subset["age"] = actual_subset["age"].astype(float)
        actual_subset["name"] = actual_subset["name"].astype(float)

        expected = pd.DataFrame(
            {
                "statistic": ["count", "mean", "min", "max"],
                "age": [2.0, 27.5, 25.0, 30.0],
                "name": [2.0, None, None, None],
            }
        )

        assert rows_same(actual_subset, expected)

    def test_summary_with_column_filter(self):
        """Test summary with specific columns."""
        ds = ray.data.from_items(
            [
                {"col1": 1, "col2": "a", "col3": 3.5},
            ]
        )

        summary = ds.summary(columns=["col1"])
        actual = summary.to_pandas()
        # Check count and mean with rows_same
        actual_subset = actual[actual["statistic"].isin(["count", "mean"])][
            ["statistic", "col1"]
        ].copy()
        actual_subset["col1"] = actual_subset["col1"].astype(float)

        expected = pd.DataFrame(
            {
                "statistic": ["count", "mean"],
                "col1": [1.0, 1.0],
            }
        )

        assert rows_same(actual_subset, expected)

    def test_summary_custom_mapping(self):
        """Test summary with custom dtype aggregation mapping."""
        ds = ray.data.from_items([{"value": 10, "other": 20}])

        # Only Count and Mean for int64 columns
        custom_mapping = {DataType.int64(): lambda col: [Count(on=col), Mean(on=col)]}

        summary = ds.summary(override_dtype_agg_mapping=custom_mapping)
        actual = summary.to_pandas()

        # Convert to float for comparison
        actual["value"] = actual["value"].astype(float)
        actual["other"] = actual["other"].astype(float)

        # Columns are sorted alphabetically, so order is: statistic, other, value
        expected = pd.DataFrame(
            {
                "statistic": ["count", "mean"],
                "other": [1.0, 20.0],
                "value": [1.0, 10.0],
            }
        )

        assert rows_same(actual, expected)

    def test_get_column_stats(self):
        """Test get_column_stats method."""
        ds = ray.data.from_items(
            [
                {"x": 1, "y": 2},
                {"x": 3, "y": 4},
            ]
        )

        summary = ds.summary()
        actual = summary.get_column_stats("x")

        # Verify key statistics with rows_same (checking subset due to mixed types)
        expected_stats = ["count", "mean", "min", "max"]
        actual_subset = actual[actual["statistic"].isin(expected_stats)].copy()
        actual_subset["value"] = actual_subset["value"].astype(float)

        expected = pd.DataFrame(
            {
                "statistic": ["count", "mean", "min", "max"],
                "value": [2.0, 2.0, 1.0, 3.0],
            }
        )

        assert rows_same(actual_subset, expected)

    @pytest.mark.parametrize(
        "data,column,expected_df",
        [
            (
                [{"x": 1}, {"x": 2}, {"x": 3}],
                "x",
                pd.DataFrame(
                    {
                        "statistic": ["count", "mean", "min", "max"],
                        "x": [3.0, 2.0, 1.0, 3.0],
                    }
                ),
            ),
            (
                [{"y": 10}, {"y": 20}],
                "y",
                pd.DataFrame(
                    {
                        "statistic": ["count", "mean", "min", "max"],
                        "y": [2.0, 15.0, 10.0, 20.0],
                    }
                ),
            ),
            (
                [{"z": 0}, {"z": 0}, {"z": 1}],
                "z",
                pd.DataFrame(
                    {
                        "statistic": ["count", "mean", "min", "max"],
                        "z": [3.0, 1.0 / 3, 0.0, 1.0],
                    }
                ),
            ),
        ],
    )
    def test_summary_statistics_values(self, data, column, expected_df):
        """Test that computed statistics have correct values."""
        ds = ray.data.from_items(data)
        summary = ds.summary(columns=[column])
        actual = summary.to_pandas()

        # Filter to key statistics and convert to float
        actual_subset = actual[
            actual["statistic"].isin(["count", "mean", "min", "max"])
        ][["statistic", column]].copy()
        actual_subset[column] = actual_subset[column].astype(float)

        assert rows_same(actual_subset, expected_df)

    @pytest.mark.parametrize(
        "data,columns,expected_df",
        [
            # Single numerical column with two values
            (
                [{"x": 10}, {"x": 20}],
                ["x"],
                pd.DataFrame(
                    {
                        DatasetSummary.STATISTIC_COLUMN: [
                            "approx_quantile[0]",
                            "count",
                            "max",
                            "mean",
                            "min",
                            "missing_pct",
                            "std",
                            "zero_pct",
                        ],
                        "x": [20.0, 2.0, 20.0, 15.0, 10.0, 0.0, 5.0, 0.0],
                    }
                ),
            ),
            # Single numerical column with all same values
            (
                [{"y": 5}, {"y": 5}, {"y": 5}],
                ["y"],
                pd.DataFrame(
                    {
                        DatasetSummary.STATISTIC_COLUMN: [
                            "approx_quantile[0]",
                            "count",
                            "max",
                            "mean",
                            "min",
                            "missing_pct",
                            "std",
                            "zero_pct",
                        ],
                        "y": [5.0, 3.0, 5.0, 5.0, 5.0, 0.0, 0.0, 0.0],
                    }
                ),
            ),
            # Multiple numerical columns
            (
                [{"a": 1, "b": 10}, {"a": 3, "b": 30}],
                ["a", "b"],
                pd.DataFrame(
                    {
                        DatasetSummary.STATISTIC_COLUMN: [
                            "approx_quantile[0]",
                            "count",
                            "max",
                            "mean",
                            "min",
                            "missing_pct",
                            "std",
                            "zero_pct",
                        ],
                        "a": [3.0, 2.0, 3.0, 2.0, 1.0, 0.0, 1.0, 0.0],
                        "b": [30.0, 2.0, 30.0, 20.0, 10.0, 0.0, 10.0, 0.0],
                    }
                ),
            ),
            # Column with zeros and missing values
            (
                [{"z": 0}, {"z": 10}, {"z": None}],
                ["z"],
                pd.DataFrame(
                    {
                        DatasetSummary.STATISTIC_COLUMN: [
                            "approx_quantile[0]",
                            "count",
                            "max",
                            "mean",
                            "min",
                            "missing_pct",
                            "std",
                            "zero_pct",
                        ],
                        "z": [10.0, 3.0, 10.0, 5.0, 0.0, 100.0 / 3, 5.0, 50.0],
                    }
                ),
            ),
            # Column with all zeros
            (
                [{"w": 0}, {"w": 0}],
                ["w"],
                pd.DataFrame(
                    {
                        DatasetSummary.STATISTIC_COLUMN: [
                            "approx_quantile[0]",
                            "count",
                            "max",
                            "mean",
                            "min",
                            "missing_pct",
                            "std",
                            "zero_pct",
                        ],
                        "w": [0.0, 2.0, 0.0, 0.0, 0.0, 0.0, 0.0, 100.0],
                    }
                ),
            ),
        ],
    )
    def test_summary_full_dataframe(self, data, columns, expected_df):
        """Test summary with full DataFrame comparison."""
        ds = ray.data.from_items(data)
        summary = ds.summary(columns=columns)
        actual = summary.to_pandas()

        # Convert to float for comparison
        for col in expected_df.columns:
            if col != DatasetSummary.STATISTIC_COLUMN:
                expected_df[col] = expected_df[col].astype(float)
                actual[col] = actual[col].astype(float)

        assert rows_same(actual, expected_df)

    def test_summary_multiple_quantiles(self):
        """Test summary with multiple quantiles."""
        ds = ray.data.from_items(
            [
                {"x": 1},
                {"x": 2},
                {"x": 3},
                {"x": 4},
                {"x": 5},
            ]
        )

        # Create custom mapping with multiple quantiles
        custom_mapping = {
            DataType.int64(): lambda col: [
                Count(on=col, ignore_nulls=False),
                Min(on=col, ignore_nulls=True),
                Max(on=col, ignore_nulls=True),
                ApproximateQuantile(on=col, quantiles=[0.25, 0.5, 0.75]),
            ]
        }

        summary = ds.summary(columns=["x"], override_dtype_agg_mapping=custom_mapping)
        actual = summary.to_pandas()

        # Should have separate rows for each quantile with index-based labels [0], [1], [2]
        expected = pd.DataFrame(
            {
                DatasetSummary.STATISTIC_COLUMN: [
                    "approx_quantile[0]",
                    "approx_quantile[1]",
                    "approx_quantile[2]",
                    "count",
                    "max",
                    "min",
                ],
                "x": [2.0, 3.0, 4.0, 5.0, 5.0, 1.0],
            }
        )

        # Convert to float for comparison
        for col in expected.columns:
            if col != DatasetSummary.STATISTIC_COLUMN:
                expected[col] = expected[col].astype(float)
                actual[col] = actual[col].astype(float)

        assert rows_same(actual, expected)

    def test_summary_custom_quantiles_and_topk(self):
        """Test summary with custom ApproximateQuantile and ApproximateTopK values."""
        # Create data with numerical and string columns
        ds = ray.data.from_items(
            [
                {"value": 10, "category": "apple"},
                {"value": 20, "category": "banana"},
                {"value": 30, "category": "apple"},
                {"value": 40, "category": "cherry"},
                {"value": 50, "category": "banana"},
                {"value": 60, "category": "apple"},
                {"value": 70, "category": "date"},
            ]
        )

        # Custom mapping with different quantile values and top-k value
        custom_mapping = {
            DataType.int64(): lambda col: [
                Count(on=col, ignore_nulls=False),
                ApproximateQuantile(on=col, quantiles=[0.1, 0.5, 0.9]),
            ],
            DataType.string(): lambda col: [
                Count(on=col, ignore_nulls=False),
                ApproximateTopK(on=col, k=3),  # Top 3 instead of default 10
            ],
        }

        summary = ds.summary(override_dtype_agg_mapping=custom_mapping)
        actual = summary.to_pandas()

        expected_stats = [
            "approx_quantile[0]",
            "approx_quantile[1]",
            "approx_quantile[2]",
            "approx_topk[0]",
            "approx_topk[1]",
            "approx_topk[2]",
            "count",
        ]

        # Verify all expected statistics are present
        actual_stats = set(actual[DatasetSummary.STATISTIC_COLUMN].tolist())
        assert all(stat in actual_stats for stat in expected_stats)

        # Helper to get statistic value for a column
        def get_stat(stat_name, col_name):
            return actual[actual[DatasetSummary.STATISTIC_COLUMN] == stat_name][
                col_name
            ].iloc[0]

        # Verify all expected values
        expected_values = {
            ("approx_quantile[0]", "value"): 10.0,
            ("approx_quantile[1]", "value"): 40.0,
            ("approx_quantile[2]", "value"): 70.0,
            ("approx_topk[0]", "category"): {"category": "apple", "count": 3},
            ("approx_topk[1]", "category"): {"category": "banana", "count": 2},
            ("approx_topk[2]", "category"): {"category": "date", "count": 1},
            ("count", "value"): 7.0,
            ("count", "category"): 7,
        }

        for (stat, col), expected in expected_values.items():
            actual_value = get_stat(stat, col)
            assert (
                actual_value == expected
            ), f"{stat}[{col}] = {actual_value} != {expected}"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
