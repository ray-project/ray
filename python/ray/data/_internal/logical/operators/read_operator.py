from typing import Any, Dict, List, Optional

from ray.data._internal.logical.operators.map_operator import AbstractMap
from ray.data.datasource.datasource import Datasource, ReadTask


class Read(AbstractMap):
    """Logical operator for read."""

    def __init__(
        self,
        datasource: Datasource,
        read_tasks: List[ReadTask],
        estimated_num_blocks: int,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        if len(read_tasks) == estimated_num_blocks:
            suffix = ""
        else:
            suffix = f"->SplitBlocks({int(estimated_num_blocks / len(read_tasks))})"
        super().__init__(f"Read{datasource.get_name()}{suffix}", None, ray_remote_args)
        self._datasource = datasource
        self._estimated_num_blocks = estimated_num_blocks
        self._read_tasks = read_tasks

    def fusable(self) -> bool:
        """Whether this should be fused with downstream operators.

        When we are outputting multiple blocks per read task, we should disable fusion,
        as fusion would prevent the blocks from being dispatched to multiple processes
        for parallel processing in downstream operators.
        """
        return self._estimated_num_blocks == len(self._read_tasks)
