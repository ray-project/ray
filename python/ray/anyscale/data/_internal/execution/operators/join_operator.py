from typing import List

from ray.anyscale.data._internal.logical.operators.join_operator import JoinType
from ray.data._internal.execution.interfaces import PhysicalOperator


class JoinOperator(PhysicalOperator):
    """An operator that Joins its inputs together."""

    def __init__(
        self,
        left_input_op: PhysicalOperator,
        right_input_op: PhysicalOperator,
        join_type: JoinType,
        keys: List[str],
    ):
        super().__init__(
            "Join", [left_input_op, right_input_op], target_max_block_size=None
        )
        self._keys = keys
        self._join_type = join_type
