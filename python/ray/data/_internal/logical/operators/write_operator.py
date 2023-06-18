from typing import Any, Dict, Optional

from ray.data._internal.compute import TaskPoolStrategy
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.logical.operators.map_operator import AbstractMap
from ray.data.datasource.datasource import Datasource


class Write(AbstractMap):
    """Logical operator for write."""

    def __init__(
        self,
        input_op: LogicalOperator,
        datasource: Datasource,
        ray_remote_args: Optional[Dict[str, Any]] = None,
        **write_args,
    ):
        super().__init__(
            "Write",
            input_op,
            ray_remote_args,
        )
        self._datasource = datasource
        self._write_args = write_args
        # Always use task to write.
        self._compute = TaskPoolStrategy()
        # Take the input blocks unchanged while writing.
        self._target_block_size = float("inf")
