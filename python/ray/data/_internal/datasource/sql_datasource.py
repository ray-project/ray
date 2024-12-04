from contextlib import contextmanager
from typing import Any, Callable, Iterable, Iterator, List, Optional

from ray.data.block import Block, BlockMetadata
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
    def __init__(self, sql: str, connection_factory: Callable[[], Connection]):
        self.sql = sql
        self.connection_factory = connection_factory

    def estimate_inmemory_data_size(self) -> Optional[int]:
        return None

    def get_read_tasks(self, parallelism: int) -> List[ReadTask]:
        def read_fn() -> Iterable[Block]:
            with _connect(self.connection_factory) as cursor:
                cursor.execute(self.sql)
                block = _cursor_to_block(cursor)
                return [block]

        metadata = BlockMetadata(None, None, None, None, None)
        return [ReadTask(read_fn, metadata)]
