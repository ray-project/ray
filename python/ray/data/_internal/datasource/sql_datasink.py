from typing import Any, Callable, Iterable

from ray.data._internal.datasource.sql_datasource import Connection, _connect
from ray.data._internal.execution.interfaces import TaskContext
from ray.data.block import Block, BlockAccessor
from ray.data.datasource.datasink import Datasink


class SQLDatasink(Datasink):

    _MAX_ROWS_PER_WRITE = 128

    def __init__(self, sql: str, connection_factory: Callable[[], Connection]):
        self.sql = sql
        self.connection_factory = connection_factory

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
    ) -> Any:
        with _connect(self.connection_factory) as cursor:
            for block in blocks:
                block_accessor = BlockAccessor.for_block(block)

                values = []
                for row in block_accessor.iter_rows(public_row_format=False):
                    values.append(tuple(row.values()))
                    assert len(values) <= self._MAX_ROWS_PER_WRITE, len(values)
                    if len(values) == self._MAX_ROWS_PER_WRITE:
                        cursor.executemany(self.sql, values)
                        values = []

                if values:
                    cursor.executemany(self.sql, values)

        return "ok"
