from typing import Any, Dict

from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data.datasource.datasource import Datasource


class Read(LogicalOperator):
    """Logical operator for read."""

    def __init__(
        self,
        datasource: Datasource,
        parallelism: int = -1,
        ray_remote_args: Dict[str, Any] = None,
        read_args: Dict[str, Any] = None,
    ):
        super().__init__("Read", [])
        self._datasource = datasource
        self._parallelism = parallelism
        self._ray_remote_args = ray_remote_args
        self._read_args = read_args
