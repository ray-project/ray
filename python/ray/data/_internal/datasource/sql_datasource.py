import math
from contextlib import contextmanager
from typing import Any, Callable, Iterable, Iterator, List, Optional

from ray.data.block import Block, BlockAccessor, BlockMetadata
from ray.data.datasource.datasource import Datasource, ReadTask

Connection = Any  # A Python DB API2-compliant `Connection` object.
Cursor = Any  # A Python DB API2-compliant `Cursor` object.


def _cursor_to_block(cursor) -> Block:
    import pyarrow as pa

    rows = cursor.fetchall()
    # Each `column_description` is a 7-element sequence. The first element is the column
    # name. To learn more, read https://peps.python.org/pep-0249/#description.
    columns = [column_description[0] for column_description in cursor.description]
    pydict = {column: [row[i] for row in rows] for i, column in enumerate(columns)}
    return pa.Table.from_pydict(pydict)


def _check_connection_is_dbapi2_compliant(connection) -> None:
    for attr in "close", "commit", "cursor":
        if not hasattr(connection, attr):
            raise ValueError(
                "Your `connection_factory` created a `Connection` object without a "
                f"{attr!r} method, but this method is required by the Python DB API2 "
                "specification. Check that your database connector is DB API2-"
                "compliant. To learn more, read https://peps.python.org/pep-0249/."
            )


def _check_cursor_is_dbapi2_compliant(cursor) -> None:
    # These aren't all the methods required by the specification, but it's all the ones
    # we care about.
    for attr in "execute", "executemany", "fetchone", "fetchall", "description":
        if not hasattr(cursor, attr):
            raise ValueError(
                "Your database connector created a `Cursor` object without a "
                f"{attr!r} method, but this method is required by the Python DB API2 "
                "specification. Check that your database connector is DB API2-"
                "compliant. To learn more, read https://peps.python.org/pep-0249/."
            )


@contextmanager
def _connect(connection_factory: Callable[[], Connection]) -> Iterator[Cursor]:
    connection = connection_factory()
    _check_connection_is_dbapi2_compliant(connection)

    try:
        cursor = connection.cursor()
        _check_cursor_is_dbapi2_compliant(cursor)
        yield cursor
        connection.commit()
    except Exception:
        # `rollback` is optional since not all databases provide transaction support.
        try:
            connection.rollback()
        except Exception as e:
            # Each connector implements its own `NotSupportError` class, so we check
            # the exception's name instead of using `isinstance`.
            if (
                isinstance(e, AttributeError)
                or e.__class__.__name__ == "NotSupportedError"
            ):
                pass
        raise
    finally:
        connection.close()


class SQLDatasource(Datasource):

    NUM_SAMPLE_ROWS = 100
    MIN_ROWS_PER_READ_TASK = 50

    def __init__(self, sql: str, connection_factory: Callable[[], Connection]):
        self.sql = sql
        self.connection_factory = connection_factory

    def estimate_inmemory_data_size(self) -> Optional[int]:
        None

    def get_read_tasks(self, parallelism: int) -> List[ReadTask]:
        def fallback_read_fn() -> Iterable[Block]:
            with _connect(self.connection_factory) as cursor:
                cursor.execute(self.sql)
                block = _cursor_to_block(cursor)
                return [block]

        # If `parallelism` is 1, directly fetch all rows. This avoids unnecessary
        # queries to fetch a sample block and compute the total number of rows.
        if parallelism == 1:
            metadata = BlockMetadata(None, None, None, None, None)
            return [ReadTask(fallback_read_fn, metadata)]

        # Databases like DB2, Oracle, and MS SQL Server don't support `LIMIT`.
        try:
            with _connect(self.connection_factory) as cursor:
                cursor.execute(f"SELECT * FROM ({self.sql}) as T LIMIT 1 OFFSET 0")
            is_limit_supported = True
        except Exception:
            is_limit_supported = False

        if not is_limit_supported:
            metadata = BlockMetadata(None, None, None, None, None)
            return [ReadTask(fallback_read_fn, metadata)]

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
        with _connect(self.connection_factory) as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM ({self.sql}) as T")
            return cursor.fetchone()[0]

    def _get_sample_block(self) -> Block:
        with _connect(self.connection_factory) as cursor:
            cursor.execute(
                f"SELECT * FROM ({self.sql}) as T LIMIT {self.NUM_SAMPLE_ROWS}"
            )
            return _cursor_to_block(cursor)

    def _create_read_fn(self, num_rows: int, offset: int):
        def read_fn() -> Iterable[Block]:
            with _connect(self.connection_factory) as cursor:
                cursor.execute(
                    f"SELECT * FROM ({self.sql}) as T LIMIT {num_rows} OFFSET {offset}"
                )
                block = _cursor_to_block(cursor)
                return [block]

        return read_fn
