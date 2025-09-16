import logging
from typing import TYPE_CHECKING, List, Optional, Tuple

import pyarrow as pa
import ray
from ray.data.aggregate import (
    AggregateFnV2,
    Count,
    Max,
    Mean,
    Min,
    Std,
    ZeroPercentage,
    MissingValuePercentage,
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
    - Std
    - MissingValuePercentage
    - ZeroPercentage

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
        column: The name of the numerical column to compute metrics for.

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
    """
    return [
        Count(on=column, ignore_nulls=False),
        MissingValuePercentage(on=column),
    ]


def feature_aggregators_for_dataset(
    dataset: "Dataset", columns: Optional[List[str]] = None
) -> Tuple[List[str], List[str], List[str], List[AggregateFnV2]]:
    """Generate aggregators for all columns in a dataset.

    Args:
        dataset: A Ray Dataset instance
        columns: A list of columns to include in the summary. If None, all columns will be included.
    Returns:
        A tuple of
          - List of numerical col names
          - List of str col names
          - List of vector col names
          - A list of AggregateFnV2 instances that can be used with Dataset.aggregate()
    """
    schema = dataset.schema()
    if not schema:
        raise ValueError("Dataset must have a schema to determine numerical columns")

    numerical_columns = []
    str_columns = []
    vector_columns = []
    all_aggs = []
    columns = columns or schema.names

    if columns is None:
        columns = schema.names
    else:
        columns_not_in_schema = set(columns) - set(schema.names)
        if columns_not_in_schema:
            raise ValueError(
                f"Columns {columns_not_in_schema} not found in dataset schema"
            )

    dropped_columns = set(schema.names) - set(columns)
    if dropped_columns:
        logger.warning(
            f"Dropping columns {dropped_columns} as they are not in the columns list"
        )

    fields_and_types = [
        (name, ftype)
        for name, ftype in zip(schema.names, schema.types)
        if name in columns
    ]

    for name, ftype in fields_and_types:
        if not isinstance(ftype, pa.DataType):
            logger.warning(
                f"Dropping field {name} as its type {ftype} is not a PyArrow DataType"
            )
            continue
        # Check if the field type is numerical using PyArrow's type system
        if (
            pa.types.is_integer(ftype)
            or pa.types.is_floating(ftype)
            or pa.types.is_decimal(ftype)
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
            logger.warning(
                f"Dropping field {name} as its type {ftype} is not numerical, string, or vector"
            )

    return (numerical_columns, str_columns, vector_columns, all_aggs)
