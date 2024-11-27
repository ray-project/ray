from enum import Enum
from typing import List

from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.logical.operators.n_ary_operator import NAry


class JoinType(Enum):
    INNER = "inner"
    LEFT_OUTER = "left_outer"
    RIGHT_OUTER = "right_outer"
    FULL_OUTER = "full_outer"


class Join(NAry):
    """Logical operator for join."""

    def __init__(
        self,
        left_input_op: LogicalOperator,
        right_input_op: LogicalOperator,
        join_type: str,
        keys: List[str],
    ):
        """
        Args:
            left_input_op: The input operator at left hand side.
            right_input_op: The input operator at right hand side.
            keys: The columns from the left and right Dataset that should be used as
              keys of the join operation.
            join_type: The kind of join that should be performed, one of (“inner”,
               “left_outer”, “right_outer”, “full_outer”).
        """

        try:
            join_type_enum = JoinType(join_type)
        except ValueError:
            raise ValueError(
                f"Invalid join type: '{join_type}'. "
                f"Supported join types are: {', '.join(jt.value for jt in JoinType)}."
            )

        super().__init__(left_input_op, right_input_op)
        self._keys = keys
        self._join_type = join_type_enum
