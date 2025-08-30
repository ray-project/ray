"""Logical operators for join operations in Ray Data.

This module provides the Join logical operator and JoinType enum for performing
various types of joins between datasets. The Join operator handles the logical
planning of join operations, including schema validation and join type specification.
"""

from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, Optional, Sequence, Tuple

from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.logical.operators.n_ary_operator import NAry

if TYPE_CHECKING:
    from ray.data import Schema


class JoinType(Enum):
    """Enumeration of supported join types in Ray Data.

    This enum defines all the join types that can be performed between datasets,
    including inner joins, outer joins, semi joins, and anti joins.
    """

    INNER = "inner"
    LEFT_OUTER = "left_outer"
    RIGHT_OUTER = "right_outer"
    FULL_OUTER = "full_outer"
    LEFT_SEMI = "left_semi"
    RIGHT_SEMI = "right_semi"
    LEFT_ANTI = "left_anti"
    RIGHT_ANTI = "right_anti"


class Join(NAry):
    """Logical operator for join operations between datasets.

    The Join operator represents a join operation in the logical plan. It handles
    various join types including inner, outer, semi, and anti joins. The operator
    validates schemas and ensures that join key columns are compatible between
    the left and right datasets.

    Examples:
        .. testcode::

            # Create sample datasets for demonstration
            import ray
            from ray.data import from_items

            # Create left dataset
            left_data = [{"id": i, "value": f"left_{i}"} for i in range(5)]
            left_dataset = from_items(left_data)

            # Create right dataset
            right_data = [{"id": i, "value": f"right_{i}"} for i in range(3, 8)]
            right_dataset = from_items(right_data)

            # Create a join operator for inner join
            join_op = Join(
                left_input_op=left_dataset._logical_plan.dag,
                right_input_op=right_dataset._logical_plan.dag,
                join_type="inner",
                left_key_columns=("id",),
                right_key_columns=("id",),
                num_partitions=4
            )
    """

    def __init__(
        self,
        left_input_op: LogicalOperator,
        right_input_op: LogicalOperator,
        join_type: str,
        left_key_columns: Tuple[str, ...],
        right_key_columns: Tuple[str, ...],
        *,
        num_partitions: int,
        left_columns_suffix: Optional[str] = None,
        right_columns_suffix: Optional[str] = None,
        partition_size_hint: Optional[int] = None,
        aggregator_ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        """Initialize the Join operator.

        Args:
            left_input_op: The input operator for the left dataset.
            right_input_op: The input operator for the right dataset.
            join_type: The type of join to perform. Must be one of the supported
                join types defined in JoinType enum.
            left_key_columns: The columns from the left dataset to use as join keys.
            right_key_columns: The columns from the right dataset to use as join keys.
            num_partitions: Total number of expected output partitions from this
                operator.
            left_columns_suffix: Optional suffix to append to left dataset column
                names to avoid naming conflicts.
            right_columns_suffix: Optional suffix to append to right dataset column
                names to avoid naming conflicts.
            partition_size_hint: Hint about the estimated average size of resulting
                partitions in bytes.
            aggregator_ray_remote_args: Optional Ray remote arguments for the
                aggregator.

        Raises:
            ValueError: If the join type is not supported or if key columns are invalid.
        """
        try:
            join_type_enum = JoinType(join_type)
        except ValueError:
            raise ValueError(
                f"Invalid join type: '{join_type}'. "
                f"Supported join types are: {', '.join(jt.value for jt in JoinType)}."
            )

        super().__init__(left_input_op, right_input_op, num_outputs=num_partitions)

        self._left_key_columns = left_key_columns
        self._right_key_columns = right_key_columns
        self._join_type = join_type_enum

        self._left_columns_suffix = left_columns_suffix
        self._right_columns_suffix = right_columns_suffix

        self._partition_size_hint = partition_size_hint
        self._aggregator_ray_remote_args = aggregator_ray_remote_args

    @staticmethod
    def _validate_schemas(
        left_op_schema: "Schema",
        right_op_schema: "Schema",
        left_key_column_names: Tuple[str, ...],
        right_key_column_names: Tuple[str, ...],
    ):
        """Validate that the schemas are compatible for the join operation.

        This method checks that:
        1. At least one key column is provided for each side
        2. The number of key columns matches between left and right sides
        3. The key columns have compatible types

        Args:
            left_op_schema: Schema of the left dataset.
            right_op_schema: Schema of the right dataset.
            left_key_column_names: Names of key columns in the left dataset.
            right_key_column_names: Names of key columns in the right dataset.

        Raises:
            ValueError: If the schemas are incompatible for joining.
        """

        def _col_names_as_str(keys: Sequence[str]) -> str:
            keys_joined = ", ".join(map(lambda k: f"'{k}'", keys))
            return f"[{keys_joined}]"

        if len(left_key_column_names) < 1:
            raise ValueError(
                f"At least 1 column name to join on has to be provided (got "
                f"{_col_names_as_str(left_key_column_names)})"
            )

        if len(left_key_column_names) != len(right_key_column_names):
            raise ValueError(
                f"Number of columns provided for left and right datasets has to match "
                f"(got {_col_names_as_str(left_key_column_names)} and "
                f"{_col_names_as_str(right_key_column_names)})"
            )

        def _get_key_column_types(
            schema: "Schema", keys: Tuple[str, ...]
        ) -> Optional[list]:
            """Extract the types of the specified key columns from a schema."""
            return (
                [
                    _type
                    for name, _type in zip(schema.names, schema.types)
                    if name in keys
                ]
                if schema
                else None
            )

        right_op_key_cols = _get_key_column_types(
            right_op_schema, left_key_column_names
        )
        left_op_key_cols = _get_key_column_types(left_op_schema, right_key_column_names)

        if left_op_key_cols != right_op_key_cols:
            raise ValueError(
                f"Key columns are expected to be present and have the same types "
                "in both left and right operands of the join operation: "
                f"left has {left_op_schema}, but right has {right_op_schema}"
            )
