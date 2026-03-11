import io
import logging
import struct
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterable, List, Optional

import pyarrow as pa

from ray.data._internal.util import _check_import
from ray.data.block import Block, BlockMetadata
from ray.data.datasource.datasource import Datasource, ReadTask
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from psycopg import sql

    from ray.data.context import DataContext

logger = logging.getLogger(__name__)


class HologresReadTask(ReadTask):
    """
    A read task for Hologres.
    """

    def __init__(
        self,
        execute_fn: Callable[[], Iterable[Block]],
        metadata: BlockMetadata,
        shard_list: List[int],
        per_task_row_limit=None,
    ):
        super().__init__(
            read_fn=execute_fn, metadata=metadata, per_task_row_limit=per_task_row_limit
        )
        self._shard_list = shard_list

    def get_shard_list(self) -> List[int]:
        return self._shard_list


@DeveloperAPI
class HologresDatasource(Datasource):
    """
    A Ray datasource for reading from Hologres.
    """

    _SHARD_COUNT_QUERY = """
        SELECT g.property_value
        FROM hologres.hg_table_properties t, hologres.hg_table_group_properties g
        WHERE t.property_key='table_group'
        AND g.property_key='shard_count'
        AND t.table_namespace=%s
        AND t.table_name=%s
        AND t.property_value = g.tablegroup_name
    """

    def __init__(
        self,
        table: str,
        connection_uri: str,
        schema: Optional[str] = "public",
        columns: Optional[List[str]] = None,
        connection_options: Optional[Dict[str, Any]] = None,
        where_filter: Optional[str] = None,
        read_mode: str = "copy_to",
        is_compressed: bool = True,
    ):
        """
        Initialize a Hologres Datasource.

        Args:
            table: str. Table name to read.
            connection_uri: str. A string in PostgreSQL connection URI format (e.g.,
                "postgresql://username:password@host:port/database").
            schema: Optional[str]. Optional schema name to read from, default to public.
            columns: Optional[List[str]]. Optional List of columns to select from the data source.
                If no columns are specified, all columns will be selected by default.
            connection_options: Optional[Dict[str, Any]]. Optional Additional keyword arguments to pass to the
                PostgreSQL connection.
            where_filter: Optional[str]. Optional WHERE clause to filter rows from the table.
                The filter should be a valid SQL condition (without the WHERE keyword).
                For example: "age > 18 AND status = 'active'".
            read_mode: str. Specifies the method to read data from Hologres. Options are:
                - "select": Use standard SELECT queries
                - "copy_to": Use PostgreSQL copy protocol with Arrow format for faster bulk reads  (default)
            is_compressed: bool. Whether the returned data is compressed, only applicable to read_mode="copy_to"

        """
        self._schema = schema
        self._table = table
        self._connection_uri = connection_uri
        self._columns = columns
        self._connection_options = connection_options or {}
        self._where_filter = where_filter
        _VALID_READ_MODES = ("select", "copy_to")
        if read_mode not in _VALID_READ_MODES:
            raise ValueError(
                f"Invalid read_mode '{read_mode}'. "
                f"Must be one of {_VALID_READ_MODES}."
            )
        self._read_mode = read_mode
        self._is_compressed = is_compressed

        # Generate and validate the query before connecting to DB
        self._query = self._generate_query()
        self._validate_select_query(self._query.as_string())

        # Validate that the table exists
        if not self._table_exists():
            raise ValueError(f"Table {schema}.{table} does not exist in the database.")

    def _init_client(self):
        _check_import(self, module="psycopg", package="psycopg[binary]")
        import psycopg

        return psycopg.connect(self._connection_uri, **self._connection_options)

    def _table_exists(self) -> bool:
        """
        Check if the specified table exists in the database.

        Returns:
            True if the table exists, False otherwise.
        """
        conn = self._init_client()
        try:
            with conn.cursor() as cursor:
                # Check if table exists in the specified schema
                cursor.execute(
                    """
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = %s
                    AND table_name = %s
                """,
                    (self._schema, self._table),
                )
                result = cursor.fetchone()
                return result is not None
        except Exception as e:
            logger.error(f"Error checking if table exists: {str(e)}")
            # If there's any error checking the table, assume it doesn't exist
            return False
        finally:
            conn.close()

    @staticmethod
    def _validate_select_query(query_str: str) -> None:
        """Validate that the query is a single SELECT statement.

        Uses sqlparse to parse the full query and verify it is a single,
        safe SELECT statement without dangerous SQL constructs.

        Args:
            query_str: The SQL query string to validate.

        Raises:
            ValueError: If the query is not a valid single SELECT statement.
        """
        import sqlparse

        parsed = sqlparse.parse(query_str)

        if len(parsed) != 1:
            raise ValueError(
                "Query must be a single SQL statement; "
                "multiple statements are not allowed."
            )

        stmt = parsed[0]
        stmt_type = stmt.get_type()

        if stmt_type != "SELECT":
            raise ValueError(f"Only SELECT statements are allowed, got: {stmt_type}")

    def _generate_query(self) -> "sql.Composed":
        from psycopg import sql

        select_clause = (
            sql.SQL(", ").join(map(sql.Identifier, self._columns))
            if self._columns
            else sql.SQL("*")
        )
        query = sql.SQL("SELECT {0} FROM {1}.{2}").format(
            select_clause, sql.Identifier(self._schema), sql.Identifier(self._table)
        )
        if self._where_filter:
            query = sql.SQL("{0} WHERE ({1})").format(
                query, sql.SQL(self._where_filter)
            )
        return query

    def _build_read_sql(
        self, shard_list: List[int], original_query: "sql.Composed"
    ) -> "sql.Composed":
        """
        Build a query that targets specific shards using Hologres-specific WHERE clause.
        """
        from psycopg import sql

        if shard_list is None or len(shard_list) == 0:
            return original_query

        in_clause = sql.SQL("hg_shard_id IN ({0})").format(
            sql.SQL(", ").join(sql.Literal(s) for s in shard_list)
        )

        if self._where_filter:
            return sql.SQL("{0} AND {1}").format(original_query, in_clause)
        else:
            return sql.SQL("{0} WHERE {1}").format(original_query, in_clause)

    def estimate_inmemory_data_size(self) -> Optional[int]:
        return None

    def get_read_tasks(
        self,
        parallelism: int,
        per_task_row_limit: Optional[int] = None,
        data_context: Optional["DataContext"] = None,
    ) -> List[ReadTask]:
        """
        Create read tasks for the Hologres query, using shard-based parallelism.

        Args:
            parallelism: The desired number of partitions to read the data into.
                Uses Hologres shard distribution for parallelism.
            per_task_row_limit: The per-task row limit for the read tasks.
            data_context: The data context to use to get read tasks.

        Returns:
            A list of read tasks to be executed.
        """
        # Get the actual shard count from Hologres
        num_shards = self._get_shard_count()
        if num_shards is None or num_shards == 0:
            logger.warning(
                "Could not determine shard count, falling back to single task"
            )
            # If we can't get shard count, fall back to a single task
            return [
                ReadTask(
                    lambda: [self._execute_read_sql(self._query)],
                    BlockMetadata(
                        num_rows=None,
                        size_bytes=None,
                        exec_stats=None,
                        input_files=None,
                    ),
                    per_task_row_limit=per_task_row_limit,
                )
            ]

        # Limit parallelism based on available shards
        actual_parallelism = min(parallelism, num_shards)

        # Calculate how many shards each task should handle
        shards_per_task = num_shards // actual_parallelism
        extra_shards = num_shards % actual_parallelism

        def _get_read_task(shard_list: List[int]) -> ReadTask:
            # Build query with shard targeting
            return HologresReadTask(
                lambda sql=self._build_read_sql(shard_list, self._query): [
                    self._execute_read_sql(sql)
                ],
                BlockMetadata(
                    num_rows=None,
                    size_bytes=None,
                    exec_stats=None,
                    input_files=None,
                ),
                shard_list,
                per_task_row_limit=per_task_row_limit,
            )

        # Create read tasks, distributing shards across tasks
        read_tasks = []
        shard_start = 0
        for i in range(actual_parallelism):
            # Calculate how many shards this task gets (some may get an extra shard)
            shard_count_for_task = shards_per_task
            if i < extra_shards:
                shard_count_for_task += 1

            # Create list of shard IDs for this task
            shard_list = list(range(shard_start, shard_start + shard_count_for_task))

            read_tasks.append(_get_read_task(shard_list))
            shard_start += shard_count_for_task

        return read_tasks

    def _get_shard_count(self) -> Optional[int]:
        """
        Get the number of shards for the table to enable parallel reads.
        """
        conn = self._init_client()
        try:
            with conn.cursor() as cursor:
                cursor.execute(self._SHARD_COUNT_QUERY, (self._schema, self._table))
                result = cursor.fetchone()
                if result:
                    return int(result[0])
        except Exception as e:
            logger.warning(f"Failed to get shard count from Hologres: {e}")
        finally:
            conn.close()
        return None

    def _execute_read_sql(self, query: str) -> Block:
        import pandas as pd

        if self._read_mode == "copy_to":
            # Use copy_to protocol with Arrow format
            return self._execute_copy_to(query)
        else:
            # Use traditional SELECT query
            from psycopg.rows import dict_row

            conn = self._init_client()
            try:
                with conn.cursor(row_factory=dict_row) as cursor:
                    cursor.execute(query)
                    rows = cursor.fetchall()

                # Convert to pandas DataFrame then to PyArrow table
                if rows:
                    df = pd.DataFrame(rows)
                    return pa.Table.from_pandas(df)
                else:
                    return pa.Table.from_pydict({})

            except Exception as e:
                raise RuntimeError(f"Failed to execute block query: {e}")
            finally:
                conn.close()

    def _decompress(self, compressed_data: bytes) -> bytes:
        # First 4 bytes is the decompressed size (Big Endian)
        decompressed_size = struct.unpack(">I", compressed_data[:4])[0]
        # Rest is the LZ4 block
        payload = compressed_data[4:]
        # lz4.block.decompress expects the raw block and the uncompressed size
        import lz4.block

        return lz4.block.decompress(payload, uncompressed_size=decompressed_size)

    def _execute_copy_to(self, query) -> Block:
        """
        Execute a query using PostgreSQL copy protocol with Arrow format.
        """
        from psycopg import sql

        # Generate the COPY command to get Arrow data
        copy_query = sql.SQL("COPY ({0}) TO STDOUT WITH (FORMAT {1})").format(
            query, sql.SQL("ARROW_LZ4" if self._is_compressed else "ARROW")
        )

        conn = self._init_client()
        try:
            with conn.cursor() as cursor:
                with cursor.copy(copy_query) as copy:
                    tables = []
                    for row in copy.rows():
                        if self._is_compressed:
                            data = self._decompress(row[0])
                        else:
                            data = row[0]
                        with pa.ipc.open_stream(io.BytesIO(data)) as reader:
                            table_chunk = reader.read_all()
                            tables.append(table_chunk)

                    if not tables:
                        return pa.Table.from_pydict({})

                    # Combine all batches into a single PyArrow Table
                    return pa.concat_tables(tables)

        except Exception as e:
            raise RuntimeError(f"Failed to execute copy_to query: {e}")
        finally:
            conn.close()
