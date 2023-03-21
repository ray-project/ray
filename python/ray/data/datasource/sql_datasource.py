import math
from contextlib import contextmanager
from typing import Any, Callable, Iterator, Iterable, List, Optional

import pyarrow as pa

from ray.data._internal.arrow_block import ArrowRow
from ray.data.block import Block, BlockAccessor, BlockMetadata
from ray.data.datasource.datasource import Datasource, Reader, ReadTask

Connection = Any  # A Python DB API2-compliant `Connection` object.
Cursor = Any  # A Python DB API2-compliant `Cursor` object.


def cursor_to_block(cursor) -> Block:
    rows = cursor.fetchall()
    # Each `column_description` is a 7-element sequence. The first element is the column
    # name. To learn more, read https://peps.python.org/pep-0249/#description.
    columns = [column_description[0] for column_description in cursor.description]
    pylist = [{c: v for c, v in zip(columns, row)} for row in rows]
    return pa.Table.from_pylist(pylist)


class SQLDatasource(Datasource[ArrowRow]):
    def __init__(self, connection_factory: Callable[[], Connection]):
        self.connection_factory = connection_factory

    def create_reader(self, sql: str) -> "Reader":
        return SQLReader(sql, self.connection_factory)


def check_connection_is_dbapi2_compliant(connection) -> None:
    for attr in "close", "commit", "cursor":
        if not hasattr(connection, attr):
            raise ValueError(
                "Your `connection_factory` created a `Connection` object without a "
                f"{attr!r} method, but this method is required by the Python DB API2 "
                "specification. Check that your database connector is DB API2-"
                "compliant. To learn more, read https://peps.python.org/pep-0249/."
            )


def check_cursor_is_dbapi2_compliant(cursor) -> None:
    # These aren't all the methods required by the specification, but it's all the ones
    # we care about.
    for attr in "execute", "fetchone", "fetchall", "description":
        if not hasattr(cursor, attr):
            raise ValueError(
                "Your database connector created a `Cursor` object without a "
                f"{attr!r} method, but this method is required by the Python DB API2 "
                "specification. Check that your database connector is DB API2-"
                "compliant. To learn more, read https://peps.python.org/pep-0249/."
            )


@contextmanager
def connect(connection_factory: Callable[[], Connection]) -> Iterator[Cursor]:
    connection = connection_factory()
    check_connection_is_dbapi2_compliant(connection)

    try:
        cursor = connection.cursor()
        check_cursor_is_dbapi2_compliant(cursor)
        yield cursor

    finally:
        # `rollback` is optional since not all databases provide transaction support.
        try:
            connection.rollback()
        except Exception as e:
            # Each connector implements its own `NotSupportError` class, so we check
            # the exception's name instead of using `isinstance`.
            if not (
                isinstance(e, AttributeError)
                or e.__class__.__name__ == "NotSupportedError"
            ):
                raise e from None

        connection.commit()
        connection.close()


class SQLReader(Reader):

    NUM_SAMPLE_ROWS = 100
    MIN_ROWS_PER_READ_TASK = 50

    def __init__(self, sql: str, connection_factory: Callable[[], Connection]):
        self.sql = sql
        self.connection_factory = connection_factory

    def estimate_inmemory_data_size(self) -> Optional[int]:
        None

    def get_read_tasks(self, parallelism: int) -> List[ReadTask]:
        # Databases like DB2, Oracle, and MS SQL Server don't support `LIMIT`.
        try:
            with connect(self.connection_factory) as cursor:
                cursor.execute(f"SELECT * FROM ({self.sql}) as T LIMIT 1 OFFSET 0")
            is_limit_supported = True
        except Exception:
            is_limit_supported = False

        # If `parallelism` is 1 or `LIMIT` clauses are unsupported, directly fetch all
        # rows. This avoids unnecessary queries to fetch a sample block and compute the 
        # total number of rows.
        if parallelism == 1 or not is_limit_supported:

            def read_fn() -> Iterable[Block]:
                with connect(self.connection_factory) as cursor:
                    cursor.execute(self.sql)
                    block = cursor_to_block(cursor)
                    return [block]

            metadata = BlockMetadata(None, None, None, None, None)
            return [ReadTask(read_fn, metadata)]

        num_rows_total = self._get_num_rows()

        if num_rows_total == 0:
            return []

        parallelism = min(
            parallelism, math.ceil(num_rows_total / self.MIN_ROWS_PER_READ_TASK)
        )
        num_rows_per_block = num_rows_total // parallelism
        num_blocks_with_extra_row = num_rows_total % parallelism

        sample_block_accessor = BlockAccessor.for_block(self._get_sample_block())
        estimated_size_bytes_per_row = math.ceil(
            sample_block_accessor.size_bytes() / sample_block_accessor.num_rows()
        )
        sample_block_schema = sample_block_accessor.schema()

        tasks = []
        offset = 0
        for i in range(parallelism):
            num_rows = num_rows_per_block
            if i < num_blocks_with_extra_row:
                num_rows += 1

            read_fn = self._create_read_fn(num_rows, offset)
            metadata = BlockMetadata(
                num_rows,
                estimated_size_bytes_per_row * num_rows,
                sample_block_schema,
                None,
                None,
            )
            tasks.append(ReadTask(read_fn, metadata))

            offset += num_rows

        return tasks

    def _get_num_rows(self) -> int:
        with connect(self.connection_factory) as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM ({self.sql}) as T")
            return cursor.fetchone()[0]

    def _get_sample_block(self) -> Block:
        with connect(self.connection_factory) as cursor:
            cursor.execute(
                f"SELECT * FROM ({self.sql}) as T LIMIT {self.NUM_SAMPLE_ROWS}"
            )
            return cursor_to_block(cursor)

    def _create_read_fn(self, num_rows: int, offset: int):
        def read_fn() -> Iterable[Block]:
            with connect(self.connection_factory) as cursor:
                cursor.execute(
                    f"SELECT * FROM ({self.sql}) as T LIMIT {num_rows} OFFSET {offset}"
                )
                block = cursor_to_block(cursor)
                return [block]

        return read_fn
