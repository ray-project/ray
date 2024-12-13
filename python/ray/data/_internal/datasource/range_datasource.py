import builtins
import functools
from copy import copy
from typing import Iterable, List, Optional, Tuple

import numpy as np

from ray.data._internal.util import _check_pyarrow_version
from ray.data.block import Block, BlockAccessor, BlockMetadata
from ray.data.context import DataContext
from ray.data.datasource import Datasource, ReadTask


class RangeDatasource(Datasource):
    """An example datasource that generates ranges of numbers from [0..n)."""

    def __init__(
        self,
        n: int,
        block_format: str = "arrow",
        tensor_shape: Tuple = (1,),
        column_name: Optional[str] = None,
    ):
        self._n = int(n)
        self._block_format = block_format
        self._tensor_shape = tensor_shape
        self._column_name = column_name

    def estimate_inmemory_data_size(self) -> Optional[int]:
        if self._block_format == "tensor":
            element_size = int(np.prod(self._tensor_shape))
        else:
            element_size = 1
        return 8 * self._n * element_size

    def get_read_tasks(
        self,
        parallelism: int,
    ) -> List[ReadTask]:
        read_tasks: List[ReadTask] = []
        n = self._n
        block_format = self._block_format
        tensor_shape = self._tensor_shape
        block_size = max(1, n // parallelism)
        # TODO(swang): This target block size may not match the driver's
        # context if it was overridden. Set target max block size during
        # optimizer stage to fix this.
        ctx = DataContext.get_current()
        if self._n == 0:
            target_rows_per_block = 0
        else:
            row_size_bytes = self.estimate_inmemory_data_size() // self._n
            row_size_bytes = max(row_size_bytes, 1)
            target_rows_per_block = max(1, ctx.target_max_block_size // row_size_bytes)

        # Example of a read task. In a real datasource, this would pull data
        # from an external system instead of generating dummy data.
        def make_block(start: int, count: int) -> Block:
            if block_format == "arrow":
                import pyarrow as pa

                return pa.Table.from_arrays(
                    [np.arange(start, start + count)],
                    names=[self._column_name or "value"],
                )
            elif block_format == "tensor":
                import pyarrow as pa

                tensor = np.ones(tensor_shape, dtype=np.int64) * np.expand_dims(
                    np.arange(start, start + count),
                    tuple(range(1, 1 + len(tensor_shape))),
                )
                return BlockAccessor.batch_to_block(
                    {self._column_name: tensor} if self._column_name else tensor
                )
            else:
                return list(builtins.range(start, start + count))

        def make_blocks(
            start: int, count: int, target_rows_per_block: int
        ) -> Iterable[Block]:
            while count > 0:
                num_rows = min(count, target_rows_per_block)
                yield make_block(start, num_rows)
                start += num_rows
                count -= num_rows

        if block_format == "tensor":
            element_size = int(np.prod(tensor_shape))
        else:
            element_size = 1

        i = 0
        while i < n:
            count = min(block_size, n - i)
            meta = BlockMetadata(
                num_rows=count,
                size_bytes=8 * count * element_size,
                schema=copy(self._schema),
                input_files=None,
                exec_stats=None,
            )
            read_tasks.append(
                ReadTask(
                    lambda i=i, count=count: make_blocks(
                        i, count, target_rows_per_block
                    ),
                    meta,
                )
            )
            i += block_size

        return read_tasks

    @functools.cached_property
    def _schema(self):
        if self._n == 0:
            return None

        if self._block_format == "arrow":
            _check_pyarrow_version()
            import pyarrow as pa

            schema = pa.Table.from_pydict({self._column_name or "value": [0]}).schema
        elif self._block_format == "tensor":
            _check_pyarrow_version()
            import pyarrow as pa

            tensor = np.ones(self._tensor_shape, dtype=np.int64) * np.expand_dims(
                np.arange(0, 10), tuple(range(1, 1 + len(self._tensor_shape)))
            )
            schema = BlockAccessor.batch_to_block(
                {self._column_name: tensor} if self._column_name else tensor
            ).schema
        elif self._block_format == "list":
            schema = int
        else:
            raise ValueError("Unsupported block type", self._block_format)
        return schema
