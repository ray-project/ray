from typing import Any, Dict, Optional

from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data.datasource.datasource import Datasource


class Write(LogicalOperator):
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
            [input_op],
        )
        self._datasource = datasource
        self._write_args = write_args
        self._ray_remote_args = ray_remote_args
        self._compute = "tasks"
        self._target_block_size = float("inf")
