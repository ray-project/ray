"""Statistical aggregation utilities for Ray Data.

This module provides type-aware aggregation functionality for computing
statistics on Ray Data datasets based on column data types.
"""

import logging
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

import numpy as np
import pandas as pd
import pyarrow as pa
from pandas.core.arrays.masked import BaseMaskedDtype

from ray.data.aggregate import (
    AggregateFnV2,
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


# ==============================================================================
# Section 1: Type Conversion Utilities
# ==============================================================================


def _convert_to_pa_type(
    dtype: Union[np.dtype, pd.ArrowDtype, BaseMaskedDtype]
) -> pa.DataType:
    """Convert pandas/numpy dtype to PyArrow dtype.

    Args:
        dtype: A pandas or numpy dtype

    Returns:
        Corresponding PyArrow DataType
    """
    if isinstance(dtype, pd.ArrowDtype):
        return dtype.pyarrow_dtype
    elif isinstance(dtype, pd.StringDtype):
        # StringDtype is not a BaseMaskedDtype, handle separately
        return pa.string()
    elif isinstance(dtype, BaseMaskedDtype):
        dtype = dtype.numpy_dtype
    return pa.from_numpy_dtype(dtype)


# ==============================================================================
# Section 3: Aggregator Creation and Management
# ==============================================================================

# Placeholder used in template aggregators that will be replaced with actual column names
_COLUMN_PLACEHOLDER = "<column>"


def _create_numerical_aggregators(column: str) -> List[AggregateFnV2]:
    """Create aggregators for numerical columns.

    Args:
        column: Column name or placeholder

    Returns:
        List of aggregators suitable for numerical data
    """
    return [
        Count(on=column, ignore_nulls=False),
        Mean(on=column, ignore_nulls=True),
        Min(on=column, ignore_nulls=True),
        Max(on=column, ignore_nulls=True),
        Std(on=column, ignore_nulls=True, ddof=0),
        MissingValuePercentage(on=column),
        ZeroPercentage(on=column, ignore_nulls=True),
    ]


def _create_temporal_aggregators(column: str) -> List[AggregateFnV2]:
    """Create aggregators for temporal columns.

    Args:
        column: Column name or placeholder

    Returns:
        List of aggregators suitable for temporal data
    """
    return [
        Count(on=column, ignore_nulls=False),
        Min(on=column, ignore_nulls=True),
        Max(on=column, ignore_nulls=True),
        MissingValuePercentage(on=column),
    ]


def _create_basic_aggregators(column: str) -> List[AggregateFnV2]:
    """Create basic aggregators for non-numerical, non-temporal columns.

    Args:
        column: Column name or placeholder

    Returns:
        List of basic aggregators
    """
    return [
        Count(on=column, ignore_nulls=False),
        MissingValuePercentage(on=column),
    ]


def default_dtype_aggregators() -> Dict["DataType", List[AggregateFnV2]]:
    """Get default mapping from Ray Data DataType to aggregators.

    This function returns the default aggregators that will be applied to columns
    of different data types during summary operations.

    Returns:
        Dict mapping DataType to list of AggregateFnV2 functions to apply.
        The aggregators use placeholders (column="<column>") that will be replaced
        with actual column names when applied.

    Note:
        The aggregators returned use a placeholder column name "<column>" which
        should be replaced with the actual column name before use.

    Examples:
        >>> from ray.data.datatype import DataType
        >>> from ray.data.stats import default_dtype_aggregators
        >>> mapping = default_dtype_aggregators()
        >>> int_aggs = mapping.get(DataType.int32())
    """
    from ray.data.datatype import DataType

    mapping = {}

    # Numerical types
    numerical_aggs = _create_numerical_aggregators(_COLUMN_PLACEHOLDER)
    for dtype_factory in [
        DataType.int8,
        DataType.int16,
        DataType.int32,
        DataType.int64,
        DataType.uint8,
        DataType.uint16,
        DataType.uint32,
        DataType.uint64,
        DataType.float32,
        DataType.float64,
        DataType.bool,
    ]:
        mapping[dtype_factory()] = numerical_aggs

    # String and binary types
    basic_aggs = _create_basic_aggregators(_COLUMN_PLACEHOLDER)
    mapping[DataType.string()] = basic_aggs
    mapping[DataType.binary()] = basic_aggs

    # Temporal types
    temporal_aggs = _create_temporal_aggregators(_COLUMN_PLACEHOLDER)
    mapping[DataType.from_arrow(pa.timestamp("us"))] = temporal_aggs
    mapping[DataType.from_arrow(pa.date32())] = temporal_aggs
    mapping[DataType.from_arrow(pa.date64())] = temporal_aggs
    mapping[DataType.from_arrow(pa.time32("s"))] = temporal_aggs
    mapping[DataType.from_arrow(pa.time64("us"))] = temporal_aggs
    mapping[DataType.from_arrow(pa.duration("us"))] = temporal_aggs

    # Note: Complex types like lists, structs, maps use fallback logic
    # in _get_aggregators_for_dtype since they can't be easily enumerated

    return mapping


# ==============================================================================
# Section 4: Type Pattern Matching
# ==============================================================================


def _matches_dtype(column_dtype: "DataType", mapping_key: "DataType") -> bool:
    """Check if a column dtype matches a mapping key.

    Supports both exact matching and pattern matching using DataType.ANY sentinel.

    Args:
        column_dtype: The dtype of the column
        mapping_key: The dtype key from the mapping (may be a pattern)

    Returns:
        True if they match according to the matching rules

    Matching Rules:
        - Exact match: column_dtype == mapping_key
        - Pattern match: mapping_key uses ANY and column_dtype fits the pattern category
    """
    from ray.data.datatype import DataType

    # Exact match always works
    if column_dtype == mapping_key:
        return True

    # Check if mapping_key is a pattern (uses ANY sentinel)
    if not isinstance(mapping_key._internal_type, DataType._AnyType):
        return False

    # Pattern matching based on category
    pattern_category = mapping_key._pattern_category

    if pattern_category == "temporal":
        return column_dtype.is_temporal_type()
    elif pattern_category == "list":
        return column_dtype.is_list_type()
    elif pattern_category == "struct":
        return column_dtype.is_struct_type()
    elif pattern_category == "map":
        return column_dtype.is_map_type()
    elif pattern_category is None:
        # Generic ANY pattern - matches everything
        return True
    else:
        # Unknown category - don't match
        logger.warning(f"Unknown pattern category: {pattern_category}")
        return False


# ==============================================================================
# Section 5: Aggregator Cloning and Column Binding
# ==============================================================================


def _is_template_aggregator(agg: AggregateFnV2) -> bool:
    """Check if an aggregator is a template (uses placeholder column name).

    Args:
        agg: An aggregator instance

    Returns:
        True if the aggregator uses the placeholder column name
    """
    if not hasattr(agg, "get_target_column"):
        return False
    return agg.get_target_column() == _COLUMN_PLACEHOLDER


def _clone_aggregator_with_column(
    agg: AggregateFnV2, column: str
) -> Optional[AggregateFnV2]:
    """Clone an aggregator with a specific column name, preserving all other parameters.

    Uses introspection to automatically extract all constructor parameters
    from the existing aggregator and pass them to the new instance.

    Args:
        agg: Template aggregator to clone
        column: Target column name for the cloned aggregator

    Returns:
        Cloned aggregator with the new column name, or None if cloning failed
    """
    import inspect

    agg_class = type(agg)
    sig = inspect.signature(agg_class.__init__)
    kwargs = {}

    # Extract constructor parameters from the instance
    for param_name, param in sig.parameters.items():
        if param_name in ("self", "on"):
            continue  # Skip self and on (we'll override on)

        # Convention: most params are stored as _param_name
        attr_name = f"_{param_name}"
        if hasattr(agg, attr_name):
            kwargs[param_name] = getattr(agg, attr_name)
        elif param.default != inspect.Parameter.empty:
            # Use default if attribute not found and default exists
            pass

    # Set the new column name
    kwargs["on"] = column

    try:
        return agg_class(**kwargs)
    except Exception as e:
        logger.warning(
            f"Could not clone aggregator {agg_class.__name__} for column '{column}': {e}"
        )
        return None


def _bind_aggregators_to_column(
    template_aggs: List[AggregateFnV2], column: str
) -> List[AggregateFnV2]:
    """Bind template aggregators to a specific column.

    Args:
        template_aggs: List of aggregators (may use placeholder or actual column)
        column: Target column name

    Returns:
        List of aggregators bound to the specific column
    """
    bound_aggs = []
    for agg in template_aggs:
        if _is_template_aggregator(agg):
            cloned = _clone_aggregator_with_column(agg, column)
            if cloned is not None:
                bound_aggs.append(cloned)
        else:
            # Not a template, use as-is
            bound_aggs.append(agg)
    return bound_aggs


# ==============================================================================
# Section 6: Aggregator Selection Logic
# ==============================================================================


def _get_aggregators_for_dtype(
    column: str,
    dtype: "DataType",
    dtype_agg_mapping: Dict["DataType", List[AggregateFnV2]],
) -> List[AggregateFnV2]:
    """Get aggregators for a specific column based on its DataType.

    Attempts to match the dtype against the provided mapping first, then
    falls back to heuristic-based selection if no match is found.

    Args:
        column: Column name
        dtype: Ray Data DataType for the column
        dtype_agg_mapping: Mapping from DataType to list of aggregators

    Returns:
        List of aggregators with the column name properly set
    """
    # Try to find a match in the mapping
    template_aggs = None
    for mapping_key, aggs in dtype_agg_mapping.items():
        if _matches_dtype(dtype, mapping_key):
            template_aggs = aggs
            break

    # If match found, bind templates to column
    if template_aggs is not None:
        return _bind_aggregators_to_column(template_aggs, column)

    # Fallback: Use heuristic-based selection
    return _get_fallback_aggregators(column, dtype)


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
        # Check for null type first (DataType doesn't have is_null_type yet)
        if dtype.is_arrow_type() and pa.types.is_null(dtype._internal_type):
            return [Count(on=column, ignore_nulls=False)]
        elif dtype.is_numerical_type():
            return _create_numerical_aggregators(column)
        elif dtype.is_temporal_type():
            return _create_temporal_aggregators(column)
        else:
            # Default for strings, binary, lists, nested types, etc.
            return _create_basic_aggregators(column)

    except Exception as e:
        logger.warning(
            f"Could not determine aggregators for column '{column}' with dtype {dtype}: {e}. "
            f"Using basic aggregators."
        )
        return _create_basic_aggregators(column)


# ==============================================================================
# Section 7: Public API and Main Orchestration
# ==============================================================================


@dataclass
class DtypeAggregators:
    """Container for columns and their aggregators.

    Attributes:
        column_to_dtype: Mapping from column name to dtype string representation
        aggregators: List of all aggregators to apply
    """

    column_to_dtype: Dict[str, str]
    aggregators: List[AggregateFnV2]


def _prepare_user_mapping(
    user_mapping: Dict["DataType", List[AggregateFnV2]]
) -> Dict["DataType", List[AggregateFnV2]]:
    """Process user-provided dtype mapping to ensure aggregators have placeholders.

    Args:
        user_mapping: User-provided mapping from DataType to aggregators

    Returns:
        Processed mapping with all aggregators using placeholder columns
    """
    processed_mapping = {}
    for dtype_key, aggs in user_mapping.items():
        processed_aggs = []
        for agg in aggs:
            if hasattr(agg, "get_target_column"):
                col = agg.get_target_column()
                if col is None or col == "":
                    # Clone with placeholder
                    cloned = _clone_aggregator_with_column(agg, _COLUMN_PLACEHOLDER)
                    processed_aggs.append(cloned if cloned else agg)
                else:
                    processed_aggs.append(agg)
            else:
                processed_aggs.append(agg)
        processed_mapping[dtype_key] = processed_aggs
    return processed_mapping


def _convert_to_ray_datatype(field_name: str, field_type: Any) -> Optional["DataType"]:
    """Convert various type formats to Ray Data DataType.

    Args:
        field_name: Name of the field (for logging)
        field_type: Type to convert (PyArrow, NumPy, Pandas, or DataType)

    Returns:
        Ray Data DataType, or None if conversion failed
    """
    from ray.data.datatype import DataType

    if isinstance(field_type, DataType):
        return field_type

    try:
        if isinstance(field_type, pa.DataType):
            return DataType.from_arrow(field_type)
        elif isinstance(field_type, (np.dtype, pd.ArrowDtype, BaseMaskedDtype)):
            pa_type = _convert_to_pa_type(field_type)
            return DataType.from_arrow(pa_type)
        else:
            logger.warning(
                f"Skipping field '{field_name}': unsupported type {type(field_type)}"
            )
            return None
    except Exception as e:
        logger.warning(
            f"Skipping field '{field_name}': could not convert type {field_type} to DataType: {e}"
        )
        return None


def _dtype_aggregators_for_dataset(
    schema: Optional["Schema"],
    columns: Optional[List[str]] = None,
    dtype_agg_mapping: Optional[Dict["DataType", List[AggregateFnV2]]] = None,
) -> DtypeAggregators:
    """Generate aggregators for columns in a dataset based on their DataTypes.

    Args:
        schema: A Ray Schema instance
        columns: List of columns to include. If None, all columns will be included.
        dtype_agg_mapping: Optional user-provided mapping from DataType to aggregators.
            This will be merged with the default mapping (user mapping takes precedence).

    Returns:
        DtypeAggregators containing column-to-dtype mapping and aggregators

    Raises:
        ValueError: If schema is None or if specified columns don't exist in schema
    """

    if not schema:
        raise ValueError("Dataset must have a schema to determine column types")

    # Determine columns to process
    if columns is None:
        columns = schema.names

    # Validate columns exist in schema
    missing_cols = set(columns) - set(schema.names)
    if missing_cols:
        raise ValueError(f"Columns {missing_cols} not found in dataset schema")

    # Build final mapping: default + user overrides
    final_mapping = default_dtype_aggregators()
    if dtype_agg_mapping:
        processed_user_mapping = _prepare_user_mapping(dtype_agg_mapping)
        final_mapping.update(processed_user_mapping)

    # Create column name to type mapping
    name_to_type = dict(zip(schema.names, schema.types))

    # Generate aggregators for each column
    column_to_dtype = {}
    all_aggs = []

    for name in columns:
        field_type = name_to_type.get(name)
        if field_type is None:
            logger.warning(f"Skipping field '{name}': type is None")
            continue

        # Convert to Ray Data DataType
        ray_dtype = _convert_to_ray_datatype(name, field_type)
        if ray_dtype is None:
            continue

        # Store dtype string representation
        column_to_dtype[name] = str(ray_dtype)

        # Get aggregators for this column
        aggs = _get_aggregators_for_dtype(name, ray_dtype, final_mapping)
        all_aggs.extend(aggs)

    return DtypeAggregators(
        column_to_dtype=column_to_dtype,
        aggregators=all_aggs,
    )


class SummaryTableType(str, Enum):
    """Enum for specifying which table to access in DatasetSummary."""

    SCHEMA_MATCHING = "schema_matching"
    SCHEMA_CHANGING = "schema_changing"


@dataclass
class DatasetSummary:
    """Wrapper for dataset summary statistics with type-aware table separation.

    This class provides a two-table structure:
    - schema_matching_stats: Statistics that preserve the original column dtype
    - schema_changing_stats: Statistics that don't match the original column dtype

    Attributes:
        schema_matching_stats: PyArrow table with stats matching original column types
        schema_changing_stats: PyArrow table with stats that differ from original types
    """

    schema_matching_stats: pa.Table
    schema_changing_stats: pa.Table

    def to_pandas(self) -> pd.DataFrame:
        """Convert summary to a single pandas DataFrame.

        Combines statistics from both schema-matching and schema-changing tables.

        Note: Some PyArrow extension types (like TensorExtensionType) may fail to convert
        to pandas when all values in a column are None. In such cases, this method will
        attempt to convert column-by-column, skipping problematic columns and casting them
        to null type instead.

        Returns:
            DataFrame with all statistics, where rows are unique statistics from both tables
        """

        def safe_convert_table(table: pa.Table) -> pd.DataFrame:
            """Safely convert a PyArrow table to pandas, handling problematic extension types."""
            try:
                return table.to_pandas()
            except (TypeError, ValueError, pa.ArrowInvalid) as e:
                # If direct conversion fails, try column-by-column conversion
                # This can happen with extension types that have all None values
                logger.warning(
                    f"Direct conversion to pandas failed ({e}), attempting column-by-column conversion"
                )

                result_data = {}
                for col_name in table.schema.names:
                    col = table.column(col_name)
                    try:
                        result_data[col_name] = col.to_pandas()
                    except (TypeError, ValueError, pa.ArrowInvalid):
                        # If this column can't be converted, cast to null type
                        null_col = pa.nulls(len(col), type=pa.null())
                        result_data[col_name] = null_col.to_pandas()

                return pd.DataFrame(result_data)

        # Convert both tables to pandas
        df_matching = safe_convert_table(self.schema_matching_stats)
        df_changing = safe_convert_table(self.schema_changing_stats)

        # Merge data from both tables
        # Get all unique statistics
        all_stats = sorted(
            set(df_matching["statistic"].tolist() + df_changing["statistic"].tolist())
        )

        # Get all column names (excluding 'statistic')
        all_columns = [
            col for col in self.schema_matching_stats.schema.names if col != "statistic"
        ]

        # Build combined DataFrame
        combined_rows = []
        for stat in all_stats:
            row = {"statistic": stat}

            for col in all_columns:
                value = None

                # Try schema-matching table first
                stat_row = df_matching[df_matching["statistic"] == stat]
                if not stat_row.empty and col in df_matching.columns:
                    value = stat_row[col].iloc[0]

                # If not found or null, try schema-changing table
                if value is None or pd.isna(value):
                    stat_row = df_changing[df_changing["statistic"] == stat]
                    if not stat_row.empty and col in df_changing.columns:
                        value = stat_row[col].iloc[0]

                row[col] = value

            combined_rows.append(row)

        return pd.DataFrame(combined_rows)

    def get_column_stats(self, column: str) -> pd.DataFrame:
        """Get all statistics for a specific column, merging from both tables.

        Args:
            column: Column name to get statistics for

        Returns:
            DataFrame with all statistics for the column
        """
        dfs = []

        # Get from schema-matching table if column exists
        if column in self.schema_matching_stats.schema.names:
            df1 = self.schema_matching_stats.to_pandas()[["statistic", column]]
            df1 = df1.rename(columns={column: "value"})
            dfs.append(df1)

        # Get from schema-changing table if column exists
        if column in self.schema_changing_stats.schema.names:
            df2 = self.schema_changing_stats.to_pandas()[["statistic", column]]
            df2 = df2.rename(columns={column: "value"})
            dfs.append(df2)

        if not dfs:
            raise ValueError(f"Column '{column}' not found in summary tables")

        # Concatenate and return
        result = pd.concat(dfs, ignore_index=True)
        return result.sort_values("statistic").reset_index(drop=True)

    def __repr__(self) -> str:
        """String representation of DatasetSummary."""
        return (
            f"DatasetSummary(\n"
            f"  schema_matching_stats: {self.schema_matching_stats.num_rows} rows × {self.schema_matching_stats.num_columns} columns\n"
            f"  schema_changing_stats: {self.schema_changing_stats.num_rows} rows × {self.schema_changing_stats.num_columns} columns\n"
            f")"
        )
