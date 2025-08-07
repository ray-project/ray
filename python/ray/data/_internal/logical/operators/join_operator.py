from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, Optional, Sequence, Tuple

from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.logical.operators.n_ary_operator import NAry

if TYPE_CHECKING:
    from ray.data import Schema


class JoinType(Enum):
    INNER = "inner"
    LEFT_OUTER = "left_outer"
    RIGHT_OUTER = "right_outer"
    FULL_OUTER = "full_outer"
    LEFT_SEMI = "left_semi"
    RIGHT_SEMI = "right_semi"
    LEFT_ANTI = "left_anti"
    RIGHT_ANTI = "right_anti"


class Join(NAry):
    """Logical operator for join."""

    def __init__(
        self,
        left_input_op: LogicalOperator,
        right_input_op: LogicalOperator,
        join_type: str,
        left_key_columns: Tuple[str],
        right_key_columns: Tuple[str],
        *,
        num_partitions: int,
        left_columns_suffix: Optional[str] = None,
        right_columns_suffix: Optional[str] = None,
        partition_size_hint: Optional[int] = None,
        aggregator_ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        """
        Args:
            left_input_op: The input operator at left hand side.
            right_input_op: The input operator at right hand side.
            join_type: The kind of join that should be performed, one of ("inner",
               "left_outer", "right_outer", "full_outer", "left_semi", "right_semi",
               "left_anti", "right_anti").
            left_key_columns: The columns from the left Dataset that should be used as
              keys of the join operation.
            right_key_columns: The columns from the right Dataset that should be used as
              keys of the join operation.
            partition_size_hint: Hint to joining operator about the estimated
              avg expected size of the resulting partition (in bytes)
            num_partitions: Total number of expected blocks outputted by this
                operator.
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
        left_key_column_names: Tuple[str],
        right_key_column_names: Tuple[str],
    ):
        def _col_names_as_str(keys: Sequence[str]):
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

        def _get_key_column_types(schema: "Schema", keys: Tuple[str]):
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
