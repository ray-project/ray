from typing import Any, Callable, Dict, Optional

from ray.data._internal.compute import ComputeStrategy, TaskPoolStrategy
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.logical.operators.one_to_one_operator import AbstractOneToOne


class StreamingRepartition(AbstractOneToOne):
    """Logical operator for streaming repartition operation.
    Args:
        input_op: The input operator.
        target_num_rows_per_block: The target number of rows per block granularity for
           streaming repartition.
        enforce_target_num_rows_per_block: Whether to enforce the target number of rows per block. Default to False.
        compute: The compute strategy to use for the streaming repartition. Default to TaskPoolStrategy.
        ray_remote_args: The ray remote args to use for the streaming repartition. Default to None.
        ray_remote_args_fn: The ray remote args function to use for the streaming repartition. Default to None.
    """

    def __init__(
        self,
        input_op: LogicalOperator,
        target_num_rows_per_block: int,
        enforce_target_num_rows_per_block: bool = False,
        compute: Optional[ComputeStrategy] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
        ray_remote_args_fn: Optional[Callable[[], Dict[str, Any]]] = None,
    ):
        super().__init__("StreamingRepartition", input_op)
        self._target_num_rows_per_block = target_num_rows_per_block
        self._enforce_target_num_rows_per_block = enforce_target_num_rows_per_block
        self._compute = compute or TaskPoolStrategy()

    @property
    def target_num_rows_per_block(self) -> int:
        return self._target_num_rows_per_block

    @property
    def enforce_target_num_rows_per_block(self) -> bool:
        return self._enforce_target_num_rows_per_block

    def can_modify_num_rows(self) -> bool:
        return False
