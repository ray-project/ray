import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, List, Optional

import pyarrow as pa

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
    from ray.data import Dataset


logger = logging.getLogger(__name__)


def numerical_aggregators(column: str) -> List[AggregateFnV2]:
    """Generate default metrics for numerical columns.

    This function returns a list of aggregators that compute the following metrics:
    - count
    - mean
    - min
    - max
    - std
    - approximate_quantile
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


def categorical_aggregators(column: str) -> List[AggregateFnV2]:
    """Generate default metrics for string columns.

    This function returns a list of aggregators that compute the following metrics:
    - count
    - MissingValuePercentage
    - ApproximateTopK

    Args:
        column: The name of the categorical column to compute metrics for.

    Returns:
        A list of AggregateFnV2 instances that can be used with Dataset.aggregate()
    """
    return [
        Count(on=column, ignore_nulls=False),
        MissingValuePercentage(on=column),
        ApproximateTopK(on=column, k=10),
    ]


def vector_aggregators(column: str) -> List[AggregateFnV2]:
    """Generate default metrics for vector columns.

    This function returns a list of aggregators that compute the following metrics:
    - count
    - MissingValuePercentage
    """
    return [
        Count(on=column, ignore_nulls=False),
        MissingValuePercentage(on=column),
    ]


@dataclass
class FeatureAggregators:
    """Container for categorized columns and their aggregators."""

    numerical_columns: List[str]
    str_columns: List[str]
    vector_columns: List[str]
    aggregators: List[AggregateFnV2]


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
    schema = dataset.schema()
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
        if name not in name_to_type:
            continue

        ftype = name_to_type[name]

        if not isinstance(ftype, pa.DataType):
            logger.warning(
                f"Skipping field {name}: type {ftype} is not a PyArrow DataType"
            )
            continue

        # Check for numerical types (including boolean as numerical)
        if (
            pa.types.is_integer(ftype)
            or pa.types.is_floating(ftype)
            or pa.types.is_decimal(ftype)
            or pa.types.is_boolean(ftype)
        ):
            numerical_columns.append(name)
            all_aggs.extend(numerical_aggregators(name))
        elif pa.types.is_string(ftype):
            str_columns.append(name)
            all_aggs.extend(categorical_aggregators(name))
        elif pa.types.is_list(ftype):
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
