from typing import TYPE_CHECKING, Callable, Iterable, Optional, cast

from ray.data._internal.datasource.sql_datasource import Connection, _connect
from ray.data._internal.execution.interfaces import TaskContext
from ray.data.block import Block, BlockAccessor
from ray.data.datasource.datasink import Datasink, WriteResult

if TYPE_CHECKING:
    from ray.data._internal.table_block import TableBlockAccessor


class SQLDatasink(Datasink[Optional[list[str]]]):

    _MAX_ROWS_PER_WRITE = 128

    def __init__(
        self,
        sql: str,
        connection_factory: Callable[[], Connection],
        column_names: Optional[list[str]],
    ):
        self.sql = sql
        self.connection_factory = connection_factory
        self.column_names = column_names

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
    ) -> Optional[list[str]]:
        column_names = self.column_names
        with _connect(self.connection_factory) as cursor:
            for block in blocks:
                block_accessor = cast(
                    "TableBlockAccessor", BlockAccessor.for_block(block)
                )
                if not block_accessor.num_rows():
                    continue
                actual = block_accessor.column_names()
                if column_names is None:
                    column_names = actual
                else:
                    self._validate_columns(column_names, actual)

                values = []
                for row in block_accessor.iter_rows(public_row_format=False):
                    values.append(tuple(row[name] for name in column_names))
                    assert len(values) <= self._MAX_ROWS_PER_WRITE, len(values)
                    if len(values) == self._MAX_ROWS_PER_WRITE:
                        cursor.executemany(self.sql, values)
                        values = []

                if values:
                    cursor.executemany(self.sql, values)

        return column_names if self.column_names is None else None

    def on_write_complete(self, write_result: WriteResult[Optional[list[str]]]) -> None:
        orders = (order for order in write_result.write_returns if order is not None)
        first = next(orders, None)
        if first is not None:
            for order in orders:
                self._validate_columns(first, order)

    def _validate_columns(self, expected: list[str], actual: list[str]) -> None:
        if actual == expected:
            return
        missing = sorted(set(expected) - set(actual))
        unexpected = sorted(set(actual) - set(expected))
        if missing or unexpected or len(actual) != len(expected):
            hint = (
                "Ensure each block provides the required columns, then use "
                "Dataset.select_columns() to set their order. "
                if self.column_names is None
                else ""
            )
            raise ValueError(
                f"SQL write expected columns {expected}, got {actual}. "
                f"Missing columns: {missing}. Unexpected columns: {unexpected}. "
                f"{hint}Other write tasks may already have committed rows."
            )
        if self.column_names is None:
            raise ValueError(
                f"SQL write found inconsistent column order: {expected} and {actual}. "
                "Use Dataset.select_columns() to set the SQL parameter order. "
                "Other write tasks may already have committed rows."
            )
