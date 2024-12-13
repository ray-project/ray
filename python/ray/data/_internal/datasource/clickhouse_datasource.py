import logging
import math
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

from ray.data._internal.util import _check_import
from ray.data.block import Block, BlockAccessor, BlockMetadata
from ray.data.datasource.datasource import Datasource, ReadTask
from ray.util.annotations import DeveloperAPI

logger = logging.getLogger(__name__)


@DeveloperAPI
class ClickHouseDatasource(Datasource):
    """
    A Ray datasource for reading from ClickHouse.
    """

    NUM_SAMPLE_ROWS = 100
    MIN_ROWS_PER_READ_TASK = 50
    _SIZE_ESTIMATE_QUERY = "SELECT SUM(byteSize(*)) AS estimate FROM ({query})"
    _COUNT_ESTIMATE_QUERY = "SELECT COUNT(*) AS estimate FROM ({query})"
    _SAMPLE_BLOCK_QUERY = "{query} LIMIT {limit_row_count}"
    _FIRST_BLOCK_QUERY = """
        {query}
        FETCH FIRST {fetch_row_count} {fetch_row_or_rows} ONLY
    """
    _NEXT_BLOCK_QUERY = """
        {query}
        OFFSET {offset_row_count} {offset_row_or_rows}
        FETCH NEXT {fetch_row_count} {fetch_row_or_rows} ONLY
    """

    def __init__(
        self,
        table: str,
        dsn: str,
        columns: Optional[List[str]] = None,
        order_by: Optional[Tuple[List[str], bool]] = None,
        client_settings: Optional[Dict[str, Any]] = None,
        client_kwargs: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize a ClickHouse Datasource.

        Args:
            table: Fully qualified table or view identifier (e.g.,
                "default.table_name").
            dsn: A string in DSN (Data Source Name) HTTP format (e.g.,
                "clickhouse+http://username:password@host:8124/default").
                For more information, see `ClickHouse Connection String doc
                <https://clickhouse.com/docs/en/integrations/sql-clients/cli#connection_string>`_.
            columns: Optional List of columns to select from the data source.
                If no columns are specified, all columns will be selected by default.
            order_by: Optional Tuple containing a list of columns to order by
                and a boolean indicating the order. Note: order_by is required to
                support parallelism.
            client_settings: Optional ClickHouse server settings to be used with the
                session/every request. For more information, see
                `ClickHouse Client Settings doc
                <https://clickhouse.com/docs/en/integrations/python#settings-argument>`_.
            client_kwargs: Optional Additional keyword arguments to pass to the
                ClickHouse client. For more information,
                see `ClickHouse Core Settings doc
                <https://clickhouse.com/docs/en/integrations/python#additional-options>`_.
        """
        self._table = table
        self._dsn = dsn
        self._columns = columns
        self._order_by = order_by
        self._client_settings = client_settings or {}
        self._client_kwargs = client_kwargs or {}
        self._query = self._generate_query()

    def _init_client(self):
        _check_import(self, module="clickhouse_connect", package="clickhouse-connect")
        import clickhouse_connect

        return clickhouse_connect.get_client(
            dsn=self._dsn,
            settings=self._client_settings or {},
            **self._client_kwargs or {},
        )

    def _generate_query(self) -> str:
        select_clause = ", ".join(self._columns) if self._columns else "*"
        query = f"SELECT {select_clause} FROM {self._table}"
        if self._order_by:
            columns, desc = self._order_by
            direction = " DESC" if desc else ""
            if len(columns) == 1:
                query += f" ORDER BY {columns[0]}{direction}"
            elif len(columns) > 1:
                columns_clause = ", ".join(columns)
                query += f" ORDER BY ({columns_clause}){direction}"
        return query

    def _build_block_query(self, limit_row_count: int, offset_row_count: int) -> str:
        if offset_row_count == 0:
            # The first block query is optimized to use FETCH FIRST clause
            # with an OFFSET specified.
            return self._FIRST_BLOCK_QUERY.format(
                query=self._query,
                fetch_row_count=limit_row_count,
                fetch_row_or_rows="ROWS" if limit_row_count > 1 else "ROW",
            )
        # Subsequent block queries use OFFSET and FETCH NEXT clauses to read the
        # next block of data.
        return self._NEXT_BLOCK_QUERY.format(
            query=self._query,
            offset_row_count=offset_row_count,
            offset_row_or_rows="ROWS" if offset_row_count > 1 else "ROW",
            fetch_row_count=limit_row_count,
            fetch_row_or_rows="ROWS" if limit_row_count > 1 else "ROW",
        )

    def _create_read_fn(
        self,
        query: str,
    ) -> Callable[[], Iterable[Block]]:
        def read_fn() -> Iterable[Block]:
            return [self._execute_block_query(query)]

        return read_fn

    def _get_sampled_estimates(self):
        if self._order_by is not None:
            # If the query is ordered, we can use a FETCH clause to get a sample.
            # This reduces the CPU overhead on ClickHouse and speeds up the
            # estimation query.
            query = self._FIRST_BLOCK_QUERY.format(
                query=self._query,
                fetch_row_count=self.NUM_SAMPLE_ROWS,
                fetch_row_or_rows="ROWS" if self.NUM_SAMPLE_ROWS > 1 else "ROW",
            )
        else:
            # If the query is not ordered, we need to use a LIMIT clause to
            # get a sample.
            query = self._SAMPLE_BLOCK_QUERY.format(
                query=self._query,
                limit_row_count=self.NUM_SAMPLE_ROWS,
            )
        sample_block_accessor = BlockAccessor.for_block(
            self._execute_block_query(query)
        )
        estimated_size_bytes_per_row = math.ceil(
            sample_block_accessor.size_bytes() / sample_block_accessor.num_rows()
        )
        sample_block_schema = sample_block_accessor.schema()
        return estimated_size_bytes_per_row, sample_block_schema

    def _get_estimate_count(self) -> Optional[int]:
        return self._execute_estimate_query(self._COUNT_ESTIMATE_QUERY)

    def _get_estimate_size(self) -> Optional[int]:
        return self._execute_estimate_query(self._SIZE_ESTIMATE_QUERY)

    def _execute_estimate_query(self, estimate_query: str) -> Optional[int]:
        client = self._init_client()
        try:
            # Estimate queries wrap around the primary query, self._query.
            # This allows us to use self._query as a sub-query to efficiently
            # and accurately estimate the size or count of the result set.
            query = estimate_query.format(query=self._query)
            result = client.query(query)
            if result and len(result.result_rows) > 0:
                estimate = result.result_rows[0][0]
                return int(estimate) if estimate is not None else None
        except Exception as e:
            logger.warning(f"Failed to execute estimate query: {e}")
        finally:
            client.close()
        return None

    def _execute_block_query(self, query: str) -> Block:
        import pyarrow as pa

        client = self._init_client()
        try:
            with client.query_arrow_stream(query) as stream:
                record_batches = list(stream)  # Collect all record batches
            return pa.Table.from_batches(record_batches)
        except Exception as e:
            raise RuntimeError(f"Failed to execute block query: {e}")
        finally:
            client.close()

    def estimate_inmemory_data_size(self) -> Optional[int]:
        """
        Estimate the in-memory data size for the query.

        Returns:
            Estimated in-memory data size in bytes, or
             None if the estimation cannot be performed.
        """
        return self._get_estimate_size()

    def get_read_tasks(self, parallelism: int) -> List[ReadTask]:
        """
        Create read tasks for the ClickHouse query.

        Args:
            parallelism: The desired number of partitions to read the data into.
                order_by must be set in the ClickHouseDatasource to
                support parallelism.

        Returns:
            A list of read tasks to be executed.
        """
        num_rows_total = self._get_estimate_count()
        if num_rows_total == 0 or num_rows_total is None:
            return []
        parallelism = min(
            parallelism, math.ceil(num_rows_total / self.MIN_ROWS_PER_READ_TASK)
        )
        # To ensure consistent order of query results, self._order_by
        # must be specified in order to support parallelism.
        if self._order_by is None and parallelism > 1:
            logger.warning(
                "ClickHouse datasource requires dataset to be explicitly ordered to "
                "support parallelism; falling back to parallelism of 1"
            )
            # When order_by is not specified and parallelism is greater than 1,
            # we need to reduce parallelism to 1 to ensure consistent results.
            # By doing this we ensure the downstream process is treated exactly as
            # a non-parallelized (single block) process would be.
            parallelism = 1
        num_rows_per_block = num_rows_total // parallelism
        num_blocks_with_extra_row = num_rows_total % parallelism
        (
            estimated_size_bytes_per_row,
            sample_block_schema,
        ) = self._get_sampled_estimates()

        def _get_read_task(
            block_rows: int, offset_rows: int, parallelized: bool
        ) -> ReadTask:
            if parallelized:
                # When parallelized, we need to build a block query with OFFSET
                # and FETCH clauses.
                query = self._build_block_query(block_rows, offset_rows)
            else:
                # When not parallelized, we can use the original query without
                # OFFSET and FETCH clauses.
                query = self._query
            return ReadTask(
                self._create_read_fn(query),
                BlockMetadata(
                    num_rows=block_rows,
                    size_bytes=estimated_size_bytes_per_row * block_rows,
                    schema=sample_block_schema,
                    input_files=None,
                    exec_stats=None,
                ),
            )

        if parallelism == 1:
            # When parallelism is 1, we can read the entire dataset in a single task.
            # We then optimize this scenario by using self._query directly without
            # unnecessary OFFSET and FETCH clauses.
            return [_get_read_task(num_rows_total, 0, False)]

        # Otherwise we need to split the dataset into multiple tasks.
        # Each task will include OFFSET and FETCH clauses to efficiently
        # read a subset of the dataset.
        read_tasks = []
        offset = 0
        for i in range(parallelism):
            num_rows = num_rows_per_block
            if i < num_blocks_with_extra_row:
                num_rows += 1
            read_tasks.append(_get_read_task(num_rows, offset, True))
            offset += num_rows
        return read_tasks
