import logging
import math
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Callable, Iterable, Iterator, List, Optional

from ray.data.block import Block, BlockMetadata
from ray.data.datasource.datasource import Datasource, ReadTask

Connection = Any  # A Python DB API2-compliant `Connection` object.
Cursor = Any  # A Python DB API2-compliant `Cursor` object.


if TYPE_CHECKING:
    from ray.data.context import DataContext


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


def _execute(cursor: Cursor, sql: str, params: Optional[Any]) -> None:
    if params is None:
        cursor.execute(sql)
    else:
        cursor.execute(sql, params)


class SQLDatasource(Datasource):
    MIN_ROWS_PER_READ_TASK = 50
    # Modulo syntaxes to probe, in preference order. ``MOD()`` is the
    # SQL-standard function (Oracle, MySQL, PostgreSQL); ``%`` is the fallback
    # for databases without ``MOD()`` (SQL Server, and SQLite built without
    # math functions).
    _MODULO_SYNTAXES = ("MOD", "%")

    def __init__(
        self,
        sql: str,
        connection_factory: Callable[[], Connection],
        shard_hash_fn: str,
        shard_keys: Optional[List[str]] = None,
        sql_params: Optional[Any] = None,
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
        self.sql_params = sql_params
        # Modulo syntax detected by ``supports_sharding()``: ``"MOD"`` or
        # ``"%"``. Stays ``None`` until a probe succeeds.
        self._modulo_syntax = None

    def estimate_inmemory_data_size(self) -> Optional[int]:
        return None

    def _build_modulo_expr(self, syntax: str, hash_fn: str, parallelism: int) -> str:
        """Build the modulo expression used to bucket rows into shards.

        ``"MOD"`` renders the SQL-standard ``MOD(x, n)`` function; ``"%"``
        renders the ``x % n`` operator. The operand is wrapped in ``ABS()`` so
        the bucket index is always in ``[0, parallelism)``, independent of how
        each database signs negative modulo results.
        """
        operand = f"ABS({hash_fn}({self.shard_keys}))"
        if syntax == "MOD":
            return f"MOD({operand}, {parallelism})"
        return f"({operand} % {parallelism})"

    def supports_sharding(self, parallelism: int) -> bool:
        """Check if the database supports hash sharding.

        Sharding needs ``ABS``, the configured hash function, ``CONCAT`` (for
        multi-column keys), and a modulo operation. The modulo syntax is not
        portable -- ``MOD()`` is missing on SQL Server and on SQLite built
        without math functions, while the ``%`` operator is missing on Oracle
        -- so this probes ``MOD()`` first and falls back to ``%``. The winning
        syntax is cached in ``self._modulo_syntax`` so the per-task read
        queries match the probe.

        Args:
            parallelism: The number of shards to split the read into.

        Returns:
            bool: True if sharding is supported, False otherwise.

        Raises:
            Exception: Any error raised while establishing the database
                connection (authentication, network, or configuration problems)
                is propagated instead of being masked as "sharding not
                supported".
        """
        if parallelism <= 1 or self.shard_keys is None:
            return False

        hash_fn = self.shard_hash_fn
        last_error: Optional[Exception] = None
        for syntax in self._MODULO_SYNTAXES:
            modulo_expr = self._build_modulo_expr(syntax, hash_fn, parallelism)
            query = f"SELECT COUNT(1) FROM ({self.sql}) as T WHERE {modulo_expr} = 0"
            connected = False
            try:
                with _connect(self.connection_factory) as cursor:
                    connected = True
                    _execute(cursor, query, self.sql_params)
            except Exception as e:
                # A failure while establishing the connection/cursor is not a
                # modulo-syntax problem, so surface it instead of misreporting
                # the database as "sharding not supported".
                if not connected:
                    raise
                last_error = e
                logger.debug(f"Sharding probe using {syntax!r} failed: {str(e)}.")
                continue
            self._modulo_syntax = syntax
            return True

        # Every modulo syntax failed. Include the last underlying error at info
        # level so an unrelated failure (e.g. a misconfigured ``shard_hash_fn``)
        # is not hidden behind the generic "not supported" message.
        logger.info(
            "Database does not support sharding: neither the MOD() function "
            f"nor the % operator is available. Last probe error: {last_error!r}"
        )
        return False

    def get_read_tasks(
        self,
        parallelism: int,
        per_task_row_limit: Optional[int] = None,
        data_context: Optional["DataContext"] = None,
    ) -> List[ReadTask]:
        def fallback_read_fn() -> Iterable[Block]:
            """Read all data in a single block when sharding is not supported."""
            with _connect(self.connection_factory) as cursor:
                _execute(cursor, self.sql, self.sql_params)
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
            _execute(cursor, f"SELECT COUNT(*) FROM ({self.sql}) as T", self.sql_params)
            return cursor.fetchone()[0]

    def _create_parallel_read_fn(self, task_id: int, parallelism: int):
        hash_fn = self.shard_hash_fn
        # Use the modulo syntax detected by supports_sharding(); default to
        # MOD() if no probe has run yet.
        syntax = self._modulo_syntax or "MOD"
        modulo_expr = self._build_modulo_expr(syntax, hash_fn, parallelism)
        query = f"SELECT * FROM ({self.sql}) as T WHERE {modulo_expr} = {task_id}"

        def read_fn() -> Iterable[Block]:
            with _connect(self.connection_factory) as cursor:
                _execute(cursor, query, self.sql_params)
                block = _cursor_to_block(cursor)
                return [block]

        return read_fn
