from typing import Any, Dict, List, Optional

from ray.data._internal.logical.operators.map_operator import AbstractMap
from ray.data.datasource.datasource import Datasource, ReadTask


class Read(AbstractMap):
    """Logical operator for read."""

    def __init__(
        self,
        datasource: Datasource,
        read_tasks: List[ReadTask],
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(f"Read{datasource.get_name()}", None, ray_remote_args)
        self._datasource = datasource
        self._read_tasks = read_tasks
