import logging
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Dict, List, Optional

import pandas as pd
import pyarrow as pa

from ray.data._expression_evaluator import _is_pa_string_type
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
    from ray.data import Dataset


logger = logging.getLogger(__name__)
RAY_DATA_DUMMY_COL = "__ray_data_dummy_col"
STAT_ORDER = ["count", "mean", "min", "max", "std", "missing_pct", "zero_pct"]


@dataclass
class DatasetSummary:
    """A statistical summary of a dataset, organized by column type.

    This class contains separate pandas DataFrames for each column type:
    numerical, categorical, and vector columns. Each DataFrame contains
    the relevant statistics for that column type.

    Attributes:
        numerical: DataFrame with statistics for numerical columns
        categorical: DataFrame with statistics for categorical columns
        vector: DataFrame with statistics for vector/list columns
    """

    numerical: pd.DataFrame
    categorical: pd.DataFrame
    vector: pd.DataFrame

    def to_pandas(self) -> pd.DataFrame:
        # Combine all DataFrames into a single MultiIndex DataFrame
        summary_data = {}

        # Map column types to their corresponding DataFrames
        dataframe_map = {
            ColumnType.NUMERICAL: self.numerical,
            ColumnType.CATEGORICAL: self.categorical,
            ColumnType.VECTOR: self.vector,
        }

        # Loop over ColumnType enum to build summary data
        for column_type, df in dataframe_map.items():
            if not df.empty:
                for col in df.columns:
                    # Create a full stats dict with NaN for missing values
                    column_stats = {}
                    for stat in STAT_ORDER:
                        if stat in df.index:
                            column_stats[stat] = df.loc[stat, col]
                        else:
                            column_stats[stat] = float("nan")
                    summary_data[(column_type.value, col)] = column_stats

        if not summary_data:
            return pd.DataFrame()

        # Create MultiIndex DataFrame
        df = pd.DataFrame(summary_data)
        df.columns = pd.MultiIndex.from_tuples(df.columns, names=["agg", "column"])

        df = df.reindex(STAT_ORDER)
        return df

    def __repr__(self) -> str:
        """Return a string representation showing the combined MultiIndex DataFrame."""
        return self.to_pandas().to_string()


class ColumnType(Enum):
    """Enumeration of supported column types for statistical analysis."""

    NUMERICAL = "numerical"
    CATEGORICAL = "categorical"
    VECTOR = "vector"


def numerical_aggregators(column: str) -> List[AggregateFnV2]:
    """Generate default metrics for numerical columns.

    This function returns a list of aggregators that compute the following metrics:
    - count
    - mean
    - min
    - max
    - std
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
        MissingValuePercentage(on=column),
        ZeroPercentage(on=column, ignore_nulls=True),
    ]


def categorical_aggregators(column: str) -> List[AggregateFnV2]:
    """Generate default metrics for string columns.

    This function returns a list of aggregators that compute the following metrics:
    - count
    - MissingValuePercentage

    Args:
        column: The name of the categorical column to compute metrics for.

    Returns:
        A list of AggregateFnV2 instances that can be used with Dataset.aggregate()
    """
    return [
        Count(on=column, ignore_nulls=False),
        MissingValuePercentage(on=column),
    ]


def vector_aggregators(column: str) -> List[AggregateFnV2]:
    """Generate default metrics for vector columns.

    This function returns a list of aggregators that compute the following metrics:
    - count
    - MissingValuePercentage

    Args:
        column: The name of the vector column to compute metrics for.
    Returns:
        A list of AggregateFnV2 instances that can be used with Dataset.aggregate()

    """
    return [
        Count(on=column, ignore_nulls=False),
        MissingValuePercentage(on=column),
    ]


def _get_underlying_type(ftype: pa.DataType) -> pa.DataType:
    """Get the underlying Arrow type, handling dictionary encoding."""
    return ftype.value_type if pa.types.is_dictionary(ftype) else ftype


def _is_numerical_type(ftype: pa.DataType) -> bool:
    """Check if Arrow type is numerical (including dictionary-encoded numerical)."""
    underlying_type = _get_underlying_type(ftype)
    return (
        pa.types.is_integer(underlying_type)
        or pa.types.is_floating(underlying_type)
        or pa.types.is_decimal(underlying_type)
        or pa.types.is_boolean(underlying_type)
    )


def _is_string_type(ftype: pa.DataType) -> bool:
    """Check if Arrow type is string (including dictionary-encoded string)."""
    underlying_type = _get_underlying_type(ftype)
    return _is_pa_string_type(underlying_type)


@dataclass
class FeatureAggregators:
    """Container for categorized columns and their aggregators."""

    numerical_columns: List[str]
    str_columns: List[str]
    vector_columns: List[str]
    aggregators: List[AggregateFnV2]

    @property
    def columns_by_type(self) -> Dict[ColumnType, List[str]]:
        """Get columns organized by ColumnType enum for easier iteration."""
        return {
            ColumnType.NUMERICAL: self.numerical_columns,
            ColumnType.CATEGORICAL: self.str_columns,
            ColumnType.VECTOR: self.vector_columns,
        }


def feature_aggregators_for_dataset(
    dataset: "Dataset", columns: Optional[List[str]] = None
) -> FeatureAggregators:
    """Generate aggregators for all columns in a dataset.

    Args:
        dataset: A Ray Dataset instance
        columns: A list of columns to include in the summary. If None, all columns will be included.
    Returns:
        FeatureAggregators containing categorized column names and their aggregators
    """
    # Avoid triggering execution.
    schema = dataset.schema(fetch_if_missing=False)
    if not schema:
        raise ValueError("Dataset must have a schema to determine numerical columns")

    if columns is None:
        columns = schema.names

    # Validate columns exist in schema
    missing_cols = set(columns) - set(schema.names)
    if missing_cols:
        raise ValueError(f"Columns {missing_cols} not found in dataset schema")

    # Categorize columns and build aggregators
    numerical_columns = []
    str_columns = []
    vector_columns = []
    all_aggs = []

    # Get column types - Ray's Schema provides names and types as lists
    column_names = schema.names
    column_types = schema.types

    # Create a mapping of column names to types
    name_to_type = dict(zip(column_names, column_types))

    for name in columns:
        ftype = name_to_type.get(name)
        if ftype is None or not isinstance(ftype, pa.DataType):
            logger.warning(
                f"Skipping field {name}: type {ftype} is not a PyArrow DataType"
            )
            continue

        # Check for numerical types (including boolean as numerical)
        if _is_numerical_type(ftype):
            numerical_columns.append(name)
            all_aggs.extend(numerical_aggregators(name))
        elif _is_string_type(ftype):
            str_columns.append(name)
            all_aggs.extend(categorical_aggregators(name))
        elif pa.types.is_list(_get_underlying_type(ftype)):
            vector_columns.append(name)
            all_aggs.extend(vector_aggregators(name))
        else:
            logger.warning(f"Skipping field {name}: type {ftype} not supported")

    return FeatureAggregators(
        numerical_columns=numerical_columns,
        str_columns=str_columns,
        vector_columns=vector_columns,
        aggregators=all_aggs,
    )


def get_stat_names_for_column_type(column_type: ColumnType) -> List[str]:
    """Extract stat names from aggregators for a given column type.

    Args:
        column_type: A ColumnType enum value

    Returns:
        List of stat names (e.g., ["count", "mean", "min", ...])
    """
    # Map column types to their aggregator functions
    aggregator_map = {
        ColumnType.NUMERICAL: numerical_aggregators,
        ColumnType.CATEGORICAL: categorical_aggregators,
        ColumnType.VECTOR: vector_aggregators,
    }

    sample_aggs = aggregator_map[column_type](RAY_DATA_DUMMY_COL)

    # Extract the stat name from aggregator names like "count(dummy_col)" -> "count"
    stat_names = []
    for agg in sample_aggs:
        agg_name = agg.name
        # Remove "(dummy_col)" to get just the stat name
        stat_name = agg_name.replace(f"({RAY_DATA_DUMMY_COL})", "")
        stat_names.append(stat_name)
    return stat_names
