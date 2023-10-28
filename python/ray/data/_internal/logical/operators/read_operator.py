from typing import Any, Dict, Optional

from ray.data._internal.logical.operators.map_operator import AbstractMap
from ray.data.datasource.datasource import Datasource, Reader


class Read(AbstractMap):
    """Logical operator for read."""

    def __init__(
        self,
        datasource: Datasource,
        reader: Reader,
        parallelism: int,
        additional_split_factor: Optional[int] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        if additional_split_factor is None:
            suffix = ""
            self._estimated_num_blocks = parallelism
        else:
            suffix = f"->SplitBlocks({additional_split_factor})"
            self._estimated_num_blocks = parallelism * additional_split_factor
        super().__init__(f"Read{datasource.get_name()}{suffix}", None, ray_remote_args)
        self._datasource = datasource
        self._reader = reader
        self._parallelism = parallelism
        self._additional_split_factor = additional_split_factor

    def fusable(self) -> bool:
        """Whether this should be fused with downstream operators.

        When we are outputting multiple blocks per read task, we should disable fusion,
        as fusion would prevent the blocks from being dispatched to multiple processes
        for parallel processing in downstream operators.
        """
        return self._parallelism == self._estimated_num_blocks
