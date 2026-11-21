from typing import TYPE_CHECKING, Callable, Iterable, cast

from ray.data._internal.datasource.sql_datasource import Connection, _connect
from ray.data._internal.execution.interfaces import TaskContext
from ray.data.block import Block, BlockAccessor
from ray.data.datasource.datasink import Datasink

if TYPE_CHECKING:
    from ray.data._internal.table_block import TableBlockAccessor


class SQLDatasink(Datasink[None]):

    _MAX_ROWS_PER_WRITE = 128

    def __init__(
        self,
        sql: str,
        connection_factory: Callable[[], Connection],
        column_names: list[str],
    ):
        self.sql = sql
        self.connection_factory = connection_factory
        self.column_names = column_names

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
    ) -> None:
        column_names = self.column_names
        with _connect(self.connection_factory) as cursor:
            for block in blocks:
                block_accessor = cast(
                    "TableBlockAccessor", BlockAccessor.for_block(block)
                )
                num_columns = len(block_accessor.column_names())
                if num_columns != len(column_names) and block_accessor.num_rows():
                    raise ValueError(
                        f"SQL write expected {len(column_names)} columns, got {num_columns}."
                    )

                values = []
                for row in block_accessor.iter_rows(public_row_format=False):
                    values.append(tuple(row[name] for name in column_names))
                    assert len(values) <= self._MAX_ROWS_PER_WRITE, len(values)
                    if len(values) == self._MAX_ROWS_PER_WRITE:
                        cursor.executemany(self.sql, values)
                        values = []

                if values:
                    cursor.executemany(self.sql, values)
