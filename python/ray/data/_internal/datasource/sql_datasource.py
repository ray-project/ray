import logging
import math
from contextlib import contextmanager
from typing import Any, Callable, Iterable, Iterator, List, Optional

from ray.data.block import Block, BlockMetadata
from ray.data.datasource.datasource import Datasource, ReadTask

Connection = Any  # A Python DB API2-compliant `Connection` object.
Cursor = Any  # A Python DB API2-compliant `Cursor` object.

logger = logging.getLogger(__name__)


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
    MIN_ROWS_PER_READ_TASK = 50

    def __init__(
        self,
        sql: str,
        connection_factory: Callable[[], Connection],
        shard_hash_fn: str,
        shard_keys: Optional[List[str]] = None,
    ):
        self.sql = sql
        if shard_keys and len(shard_keys) > 1:
            self.shard_keys = f"CONCAT({','.join(shard_keys)})"
        elif shard_keys and len(shard_keys) == 1:
            self.shard_keys = f"{shard_keys[0]}"
        else:
            self.shard_keys = None
        self.shard_hash_fn = shard_hash_fn
        self.connection_factory = connection_factory

    def estimate_inmemory_data_size(self) -> Optional[int]:
        return None

    def supports_sharding(self, parallelism: int) -> bool:
        """Check if database supports sharding with MOD/ABS/CONCAT operations.

        Returns:
            bool: True if sharding is supported, False otherwise.
        """
        if parallelism <= 1 or self.shard_keys is None:
            return False

        # Test if database supports required operations (MOD, ABS, MD5, CONCAT)
        # by executing a sample query
        hash_fn = self.shard_hash_fn
        query = (
            f"SELECT COUNT(1) FROM ({self.sql}) as T"
            f" WHERE MOD(ABS({hash_fn}({self.shard_keys})), {parallelism}) = 0"
        )
        try:
            with _connect(self.connection_factory) as cursor:
                cursor.execute(query)
            return True
        except Exception as e:
            logger.info(f"Database does not support sharding: {str(e)}.")
            return False

    def get_read_tasks(
        self, parallelism: int, per_task_row_limit: Optional[int] = None
    ) -> List[ReadTask]:
        def fallback_read_fn() -> Iterable[Block]:
            """Read all data in a single block when sharding is not supported."""
            with _connect(self.connection_factory) as cursor:
                cursor.execute(self.sql)
                return [_cursor_to_block(cursor)]

        # Check if sharding is supported by the database first
        # If not, fall back to reading all data in a single task without counting rows
        if not self.supports_sharding(parallelism):
            logger.info(
                "Sharding is not supported. "
                "Falling back to reading all data in a single task."
            )
            metadata = BlockMetadata(None, None, None, None)
            return [ReadTask(fallback_read_fn, metadata)]

        # Only perform the expensive COUNT(*) query if sharding is supported
        num_rows_total = self._get_num_rows()

        if num_rows_total == 0:
            return []

        parallelism = min(
            parallelism, math.ceil(num_rows_total / self.MIN_ROWS_PER_READ_TASK)
        )
        num_rows_per_block = num_rows_total // parallelism
        num_blocks_with_extra_row = num_rows_total % parallelism

        tasks = []
        for i in range(parallelism):
            num_rows = num_rows_per_block
            if i < num_blocks_with_extra_row:
                num_rows += 1
            read_fn = self._create_parallel_read_fn(i, parallelism)
            metadata = BlockMetadata(
                num_rows=num_rows,
                size_bytes=None,
                input_files=None,
                exec_stats=None,
            )
            tasks.append(
                ReadTask(read_fn, metadata, per_task_row_limit=per_task_row_limit)
            )

        return tasks

    def _get_num_rows(self) -> int:
        with _connect(self.connection_factory) as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM ({self.sql}) as T")
            return cursor.fetchone()[0]

    def _create_parallel_read_fn(self, task_id: int, parallelism: int):
        hash_fn = self.shard_hash_fn
        query = (
            f"SELECT * FROM ({self.sql}) as T "
            f"WHERE MOD(ABS({hash_fn}({self.shard_keys})), {parallelism}) = {task_id}"
        )

        def read_fn() -> Iterable[Block]:
            with _connect(self.connection_factory) as cursor:
                cursor.execute(query)
                block = _cursor_to_block(cursor)
                return [block]

        return read_fn
