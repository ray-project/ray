import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Dict, List, Optional

import pandas as pd
import pyarrow as pa

from ray.air.util.tensor_extensions.arrow import convert_to_pyarrow_array
from ray.data.aggregate import (
    AggregateFnV2,
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

if TYPE_CHECKING:
    from ray.data.dataset import Schema
    from ray.data.datatype import DataType


logger = logging.getLogger(__name__)


@dataclass
class DatasetSummary:
    """Wrapper for dataset summary statistics.

    Provides methods to access computed statistics.

    Attributes:
        dataset_schema: PyArrow schema of the original dataset
    """

    STATISTIC_COLUMN = "statistic"

    _schema_matching_stats: pa.Table
    _schema_changing_stats: pa.Table
    dataset_schema: pa.Schema
    columns: list[str]

    def _safe_convert_table(self, table: pa.Table):
        """Safely convert a PyArrow table to pandas, handling problematic extension types.

        Args:
            table: PyArrow table to convert

        Returns:
            pandas DataFrame with converted data
        """
        from ray.data.block import BlockAccessor

        try:
            return BlockAccessor.for_block(table).to_pandas()
        except (TypeError, ValueError, pa.ArrowInvalid) as e:
            logger.warning(
                f"Direct conversion to pandas failed ({e}), "
                "attempting column-by-column conversion"
            )

            result_data = {}
            for col_name in table.schema.names:
                col = table.column(col_name)
                try:
                    result_data[col_name] = col.to_pandas()
                except (TypeError, ValueError, pa.ArrowInvalid):
                    # Cast problematic columns to null type
                    null_col = pa.nulls(len(col), type=pa.null())
                    result_data[col_name] = null_col.to_pandas()

            return pd.DataFrame(result_data)

    def to_pandas(self):
        """Convert summary to a single pandas DataFrame.

        Combines statistics from both schema-matching and schema-changing tables.

        Note: Some PyArrow extension types (like TensorExtensionType) may fail to convert
        to pandas when all values in a column are None. In such cases, this method
        attempts to convert column-by-column, casting problematic columns to null type.

        Returns:
            DataFrame with all statistics, where rows are unique statistics from both tables
        """
        df_matching = self._safe_convert_table(self._schema_matching_stats).set_index(
            self.STATISTIC_COLUMN
        )
        df_changing = self._safe_convert_table(self._schema_changing_stats).set_index(
            self.STATISTIC_COLUMN
        )

        # Combine tables: prefer schema_matching values, fill with schema_changing
        result = df_matching.combine_first(df_changing)

        return (
            result.reset_index()
            .sort_values(self.STATISTIC_COLUMN)
            .reset_index(drop=True)
        )

    def _extract_column_from_table(
        self, table: pa.Table, column: str
    ) -> Optional[dict]:
        """Extract a column from a PyArrow table if it exists.

        Args:
            table: PyArrow table to extract from
            column: Column name to extract

        Returns:
            DataFrame with 'statistic' and 'value' columns, or None if column doesn't exist
        """
        if column not in table.schema.names:
            return None

        df = table.to_pandas()[[self.STATISTIC_COLUMN, column]]
        return df.rename(columns={column: "value"})

    def get_column_stats(self, column: str):
        """Get all statistics for a specific column, merging from both tables.

        Args:
            column: Column name to get statistics for

        Returns:
            DataFrame with all statistics for the column
        """
        dfs = [
            df
            for table in [self._schema_matching_stats, self._schema_changing_stats]
            if (df := self._extract_column_from_table(table, column)) is not None
        ]

        if not dfs:
            raise ValueError(f"Column '{column}' not found in summary tables")

        # Concatenate and merge duplicate statistics (prefer non-null values)
        combined = pd.concat(dfs, ignore_index=True)

        # Group by statistic and take first non-null value for each group
        def first_non_null(series):
            non_null = series.dropna()
            return non_null.iloc[0] if len(non_null) > 0 else None

        result = (
            combined.groupby(self.STATISTIC_COLUMN, sort=False)["value"]
            .apply(first_non_null)
            .reset_index()
            .sort_values(self.STATISTIC_COLUMN)
            .reset_index(drop=True)
        )

        return result


@dataclass
class DtypeAggregators:
    """Container for columns and their aggregators.

    Attributes:
        column_to_dtype: Mapping from column name to dtype string representation
        aggregators: List of all aggregators to apply
    """

    column_to_dtype: Dict[str, str]
    aggregators: List[AggregateFnV2]


def numerical_aggregators(column: str) -> List[AggregateFnV2]:
    """Generate default metrics for numerical columns.

    This function returns a list of aggregators that compute the following metrics:
    - count
    - mean
    - min
    - max
    - std
    - approximate_quantile (median)
    - missing_value_percentage
    - zero_percentage

    Args:
        column: The name of the numerical column to compute metrics for.

    Returns:
        A list of AggregateFnV2 instances that can be used with Dataset.aggregate()
    """
    return [
        Count(on=column, ignore_nulls=False),
        Mean(on=column, ignore_nulls=True),
        Min(on=column, ignore_nulls=True),
        Max(on=column, ignore_nulls=True),
        Std(on=column, ignore_nulls=True, ddof=0),
        ApproximateQuantile(on=column, quantiles=[0.5]),
        MissingValuePercentage(on=column),
        ZeroPercentage(on=column, ignore_nulls=True),
    ]


def temporal_aggregators(column: str) -> List[AggregateFnV2]:
    """Generate default metrics for temporal columns.

    This function returns a list of aggregators that compute the following metrics:
    - count
    - min
    - max
    - missing_value_percentage

    Args:
        column: The name of the temporal column to compute metrics for.

    Returns:
        A list of AggregateFnV2 instances that can be used with Dataset.aggregate()
    """
    return [
        Count(on=column, ignore_nulls=False),
        Min(on=column, ignore_nulls=True),
        Max(on=column, ignore_nulls=True),
        MissingValuePercentage(on=column),
    ]


def basic_aggregators(column: str) -> List[AggregateFnV2]:
    """Generate default metrics for all columns.

    This function returns a list of aggregators that compute the following metrics:
    - count
    - missing_value_percentage
    - approximate_top_k (top 10 most frequent values)

    Args:
        column: The name of the column to compute metrics for.

    Returns:
        A list of AggregateFnV2 instances that can be used with Dataset.aggregate()
    """
    return [
        Count(on=column, ignore_nulls=False),
        MissingValuePercentage(on=column),
        ApproximateTopK(on=column, k=10),
    ]


def default_dtype_aggregators() -> Dict[
    "DataType", Callable[[str], List[AggregateFnV2]]
]:
    """Get default mapping from Ray Data DataType to aggregator factory functions.

    This function returns factory functions that create aggregators for specific columns.

    Returns:
        Dict mapping DataType to factory functions that take a column name
        and return a list of aggregators for that column.

    Examples:
        >>> from ray.data.datatype import DataType
        >>> from ray.data.stats import default_dtype_aggregators
        >>> mapping = default_dtype_aggregators()
        >>> factory = mapping.get(DataType.int32())
        >>> aggs = factory("my_column")  # Creates aggregators for "my_column"
    """
    from ray.data.datatype import DataType

    # Use pattern-matching types for cleaner mapping
    return {
        # Numerical types
        DataType.int8(): numerical_aggregators,
        DataType.int16(): numerical_aggregators,
        DataType.int32(): numerical_aggregators,
        DataType.int64(): numerical_aggregators,
        DataType.uint8(): numerical_aggregators,
        DataType.uint16(): numerical_aggregators,
        DataType.uint32(): numerical_aggregators,
        DataType.uint64(): numerical_aggregators,
        DataType.float32(): numerical_aggregators,
        DataType.float64(): numerical_aggregators,
        DataType.bool(): numerical_aggregators,
        # String and binary types
        DataType.string(): basic_aggregators,
        DataType.binary(): basic_aggregators,
        # Temporal types - pattern matches all temporal types (timestamp, date, time, duration)
        DataType.temporal(): temporal_aggregators,
        # Note: Complex types like lists, structs, maps use fallback logic
        # in _get_aggregators_for_dtype since they can't be easily enumerated
    }


def _get_fallback_aggregators(column: str, dtype: "DataType") -> List[AggregateFnV2]:
    """Get aggregators using heuristic-based type detection.

    This is a fallback when no explicit mapping is found for the dtype.

    Args:
        column: Column name
        dtype: Ray Data DataType for the column

    Returns:
        List of aggregators suitable for the column type
    """
    try:
        # Check for null type first
        if dtype.is_arrow_type() and pa.types.is_null(dtype._physical_dtype):
            return [Count(on=column, ignore_nulls=False)]
        elif dtype.is_numerical_type():
            return numerical_aggregators(column)
        elif dtype.is_temporal_type():
            return temporal_aggregators(column)
        else:
            # Default for strings, binary, lists, nested types, etc.
            return basic_aggregators(column)

    except Exception as e:
        logger.warning(
            f"Could not determine aggregators for column '{column}' with dtype {dtype}: {e}. "
            f"Using basic aggregators."
        )
        return basic_aggregators(column)


def _get_aggregators_for_dtype(
    column: str,
    dtype: "DataType",
    dtype_agg_mapping: Dict["DataType", Callable[[str], List[AggregateFnV2]]],
) -> List[AggregateFnV2]:
    """Get aggregators for a specific column based on its DataType.

    Attempts to match the dtype against the provided mapping first, then
    falls back to heuristic-based selection if no match is found.

    Args:
        column: Column name
        dtype: Ray Data DataType for the column
        dtype_agg_mapping: Mapping from DataType to factory functions

    Returns:
        List of aggregators with the column name properly set
    """
    from ray.data.datatype import _matches_dtype

    # Try to find a match in the mapping
    for mapping_key, factory in dtype_agg_mapping.items():
        if _matches_dtype(dtype, mapping_key):
            return factory(column)

    # Fallback: Use heuristic-based selection
    return _get_fallback_aggregators(column, dtype)


def _dtype_aggregators_for_dataset(
    schema: Optional["Schema"],
    columns: Optional[List[str]] = None,
    dtype_agg_mapping: Optional[
        Dict["DataType", Callable[[str], List[AggregateFnV2]]]
    ] = None,
) -> DtypeAggregators:
    """Generate aggregators for columns in a dataset based on their DataTypes.

    Args:
        schema: A Ray Schema instance
        columns: List of columns to include. If None, all columns will be included.
        dtype_agg_mapping: Optional user-provided mapping from DataType to aggregator factories.
            Each value should be a callable that takes a column name and returns aggregators.
            This will be merged with the default mapping (user mapping takes precedence).

    Returns:
        DtypeAggregators containing column-to-dtype mapping and aggregators

    Raises:
        ValueError: If schema is None or if specified columns don't exist in schema
    """
    from ray.data.datatype import DataType

    if not schema:
        raise ValueError("Dataset must have a schema to determine column types")

    if columns is None:
        columns = schema.names

    # Validate columns exist in schema
    missing_cols = set(columns) - set(schema.names)
    if missing_cols:
        raise ValueError(f"Columns {missing_cols} not found in dataset schema")

    # Build final mapping: default + user overrides
    defaults = default_dtype_aggregators()
    if dtype_agg_mapping:
        # Put user overrides first so they are checked before default patterns
        final_mapping = dtype_agg_mapping.copy()
        for k, v in defaults.items():
            if k not in final_mapping:
                final_mapping[k] = v
    else:
        final_mapping = defaults

    # Generate aggregators for each column
    column_to_dtype = {}
    all_aggs = []
    name_to_type = dict(zip(schema.names, schema.types))

    for name in columns:
        pa_type = name_to_type[name]
        if pa_type is None or pa_type is object:
            logger.warning(f"Skipping field '{name}': type is None or unsupported")
            continue

        ray_dtype = DataType.from_arrow(pa_type)
        column_to_dtype[name] = str(ray_dtype)
        all_aggs.extend(_get_aggregators_for_dtype(name, ray_dtype, final_mapping))

    return DtypeAggregators(
        column_to_dtype=column_to_dtype,
        aggregators=all_aggs,
    )


def _parse_summary_stats(
    agg_result: Dict[str, any],
    original_schema: pa.Schema,
    agg_schema: pa.Schema,
    aggregators: List[AggregateFnV2],
) -> tuple:
    """Parse aggregation results into schema-matching and schema-changing stats.

    Args:
        agg_result: Dictionary of aggregation results with keys like "count(col)"
        original_schema: Original dataset schema
        agg_schema: Schema of aggregation results
        aggregators: List of aggregators used to generate the results

    Returns:
        Tuple of (schema_matching_stats, schema_changing_stats, column_names)
    """
    schema_matching = {}
    schema_changing = {}
    columns = set()

    # Build a lookup map from "stat_name(col_name)" to aggregator
    agg_lookup = {agg.name: agg for agg in aggregators}

    for key, value in agg_result.items():
        if "(" not in key or not key.endswith(")"):
            continue

        # Get aggregator and extract info
        agg = agg_lookup.get(key)
        if not agg:
            continue

        col_name = agg.get_target_column()
        if not col_name:
            # Skip aggregations without a target column (e.g., Count())
            continue

        # Let the aggregator format its own results
        agg_type = agg_schema.field(key).type
        original_type = original_schema.field(col_name).type
        formatted_stats = agg.format_stats(value, agg_type, original_type)

        for stat_name, (stat_value, stat_type) in formatted_stats.items():
            # Add formatted stats to appropriate dict based on schema matching
            stats_dict = (
                schema_matching if stat_type == original_type else schema_changing
            )
            stats_dict.setdefault(stat_name, {})[col_name] = (stat_value, stat_type)

        columns.add(col_name)

    return schema_matching, schema_changing, columns


def _create_pyarrow_array(
    col_data: List, col_type: Optional[pa.DataType] = None, col_name: str = ""
) -> pa.Array:
    """Create a PyArrow array with fallback strategies.

    Uses convert_to_pyarrow_array from arrow_block.py for type inference and
    error handling when no specific type is provided.

    Args:
        col_data: List of column values
        col_type: Optional PyArrow type to use
        col_name: Column name for error messages (optional)

    Returns:
        PyArrow array
    """
    if col_type is not None:
        try:
            return pa.array(col_data, type=col_type)
        except (pa.ArrowTypeError, pa.ArrowInvalid):
            # Type mismatch - fall through to type inference
            pass

    # Use convert_to_pyarrow_array for type inference and error handling
    # This handles tensors, extension types, and fallback to ArrowPythonObjectArray
    return convert_to_pyarrow_array(col_data, col_name or "column")


def _build_summary_table(
    stats_dict: Dict[str, Dict[str, tuple]],
    all_columns: set,
    original_schema: pa.Schema,
    preserve_types: bool,
) -> pa.Table:
    """Build a PyArrow table from parsed statistics.

    Args:
        stats_dict: Nested dict of {stat_name: {col_name: (value, type)}}
        all_columns: Set of all column names across both tables
        original_schema: Original dataset schema
        preserve_types: If True, use original schema types for columns

    Returns:
        PyArrow table with statistics
    """
    if not stats_dict:
        return pa.table({})

    stat_names = sorted(stats_dict.keys())
    table_data = {DatasetSummary.STATISTIC_COLUMN: stat_names}

    for col_name in sorted(all_columns):
        # Collect values and infer type
        col_data = []
        first_type = None

        for stat_name in stat_names:
            if col_name in stats_dict[stat_name]:
                value, agg_type = stats_dict[stat_name][col_name]
                col_data.append(value)
                if first_type is None:
                    first_type = agg_type
            else:
                col_data.append(None)

        # Determine column type: prefer original schema, then first aggregation type, then infer
        if preserve_types and col_name in original_schema.names:
            col_type = original_schema.field(col_name).type
        else:
            col_type = first_type

        table_data[col_name] = _create_pyarrow_array(col_data, col_type, col_name)

    return pa.table(table_data)
