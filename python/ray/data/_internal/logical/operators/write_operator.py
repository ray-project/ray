from typing import Any, Dict, Optional

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
        self._compute = "tasks"
        # Take the input blocks unchanged while writing.
        self._target_block_size = float("inf")
        # No-op dummy UDF params so for the purpose of operator fusion, the Write
        # will be treated as Map.
        self._fn = None
        self._fn_args = None
        self._fn_kwargs = None
        self._fn_constructor_args = None
        self._fn_constructor_kwargs = None
