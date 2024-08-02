import math
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterable, List, Optional

import numpy as np

from ray.data._internal.output_buffer import BlockOutputBuffer
from ray.data._internal.util import _check_import
from ray.data.block import Block, BlockMetadata
from ray.data.context import DatasetContext
from ray.data.datasource import Datasource, ReadTask

if TYPE_CHECKING:
    from snowflake.connector.result_batch import ResultBatch


class SnowflakeDatasource(Datasource):

    MIN_ROWS_PER_READ_TASK = 50

    def __init__(self, sql: str, connection_parameters: Dict[str, Any]):
        _check_import(self, module="snowflake", package="snowflake-connector-python")

        self._sql = sql
        self._connection_parameters = connection_parameters

    def estimate_inmemory_data_size(self) -> Optional[int]:
        return None

    def get_read_tasks(self, parallelism: int) -> List[ReadTask]:
        from snowflake.connector import connect

        with connect(
            **self._connection_parameters
        ) as connection, connection.cursor() as cursor:
            cursor.execute(self._sql)
            num_rows_total = cursor.rowcount
            result_batches = cursor.get_result_batches()

        result_batches = [b for b in result_batches if b.rowcount > 0]

        if not result_batches:
            return []

        parallelism = min(
            parallelism,
            math.ceil(num_rows_total / self.MIN_ROWS_PER_READ_TASK),
            len(result_batches),
        )

        sample_block = result_batches[0].to_arrow()
        estimated_size_bytes_per_row = sample_block.nbytes // sample_block.num_rows
        schema = sample_block.schema

        tasks = []
        for result_batches_split in np.array_split(result_batches, parallelism):
            read_fn = _create_read_fn(result_batches_split)

            num_rows = sum(b.rowcount for b in result_batches_split)
            size_bytes = estimated_size_bytes_per_row * num_rows
            metadata = BlockMetadata(num_rows, size_bytes, schema, None, None)

            tasks.append(ReadTask(read_fn, metadata))

        return tasks


def _create_read_fn(
    result_batches_split: List["ResultBatch"],
) -> Callable[[], Iterable[Block]]:
    def read_fn() -> Iterable[Block]:
        ctx = DatasetContext.get_current()
        output_buffer = BlockOutputBuffer(
            target_max_block_size=ctx.target_max_block_size
        )

        for result_batch in result_batches_split:
            block = result_batch.to_arrow()
            output_buffer.add_block(block)
            if output_buffer.has_next():
                yield output_buffer.next()

        output_buffer.finalize()
        if output_buffer.has_next():
            yield output_buffer.next()

    return read_fn
