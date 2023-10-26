from typing import Any, Dict, Optional, Union

from ray.data._internal.compute import TaskPoolStrategy
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.logical.operators.map_operator import AbstractMap
from ray.data.datasource.datasink import Datasink
from ray.data.datasource.datasource import Datasource


class Write(AbstractMap):
    """Logical operator for write."""

    def __init__(
        self,
        input_op: LogicalOperator,
        datasink_or_legacy_datasource: Union[Datasink, Datasource],
        ray_remote_args: Optional[Dict[str, Any]] = None,
        **write_args,
    ):
        super().__init__(
            "Write",
            input_op,
            ray_remote_args,
        )
        self._datasink_or_legacy_datasource = datasink_or_legacy_datasource
        self._write_args = write_args
        # Always use task to write.
        self._compute = TaskPoolStrategy()
        # Take the input blocks unchanged while writing.
        self._min_rows_per_block = float("inf")
