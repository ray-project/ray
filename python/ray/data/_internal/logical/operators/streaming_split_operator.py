from typing import List, Optional

from ray.data import NodeIdStr
from ray.data._internal.logical.interfaces import LogicalOperator


class StreamingSplit(LogicalOperator):
    """Logical operator that represents splitting the input data to `n` splits."""

    def __init__(
        self,
        input_op: LogicalOperator,
        num_splits: int,
        equal: bool,
        locality_hints: Optional[List[NodeIdStr]] = None,
    ):
        super().__init__("StreamingSplit", [input_op])
        self._num_splits = num_splits
        self._equal = equal
        self._locality_hints = locality_hints
