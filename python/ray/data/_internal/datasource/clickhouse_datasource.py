import logging
import math
from typing import Any, Callable, Iterable, List, Optional

from ray.data._internal.util import _check_import
from ray.data.block import Block, BlockAccessor, BlockMetadata
from ray.data.datasource.datasource import Datasource, ReadTask
from ray.util.annotations import DeveloperAPI

logger = logging.getLogger(__name__)


def _convert_filter_value(key: str, operator: str, value: Any) -> Optional[str]:
    # TODO(jecsand838) add date and datetime filter support
    ops = {
        "==": {"types": ["*"]},
        "!=": {"types": ["*"]},
        "<": {"types": [int, float]},
        ">": {"types": [int, float]},
    }
    if value is None:
        if operator == "==":
            return f"{key} IS NULL"
        elif operator == "!=":
            return f"{key} IS NOT NULL"
        else:
            raise ValueError(
                "Your `filters` contains a non-nullable operator "
                f"'{operator}' being used to filters by null values on column: '{key}'."
                "To learn more, read "
                "https://clickhouse.com/docs/en/sql-reference/statements/select/where/."
            )
    elif isinstance(value, (str, bool, int, float)):
        operator = _validate_ops(key, operator, value, ops)
        return f"{key} {operator} {_convert_value(value)}"
    else:
        logger.warning(
            f"Unsupported data type {type(value).__name__}"
            f"for filtering on '{key}'. Please use one of: str, int, float, bool."
        )
        return None


def _validate_ops(column: str, op: str, value, ops) -> str:
    if op not in ops:
        raise ValueError(
            "Your `filters` contains a unsupported operator "
            f"'{op}' being used on column: '{column}'."
            "To learn more, read "
            "https://clickhouse.com/docs/en/sql-reference/statements/select/where/."
        )
    op_types = ops[op]["types"]
    if op_types != ["*"] and type(value) not in op_types:
        raise ValueError(
            "Your `filters` contains a non-supported operator "
            f"'{op}' being used on column: '{column}' with "
            f"value of type: '{type(value).__name__}'."
            f"Supported types for operator '{op}' are: {op_types}."
            "To learn more, read "
            "https://clickhouse.com/docs/en/sql-reference/statements/select/where/."
        )
    return op


def _convert_value(value) -> str:
    if isinstance(value, str):
        return f"'{value}'"
    elif isinstance(value, bool):
        return f"{str(value).lower()}"
    elif isinstance(value, (int, float)):
        return f"{str(value)}"
    else:
        raise ValueError(
            "Your `filters` contains a non-supported "
            f"value type '{type(value).__name__}'"
            "To learn more, read "
            "https://clickhouse.com/docs/en/sql-reference/statements/select/where/."
        )


@DeveloperAPI
class ClickHouseDatasource(Datasource):
    """
    A Ray datasource for reading from ClickHouse.
    """

    NUM_SAMPLE_ROWS = 100
    MIN_ROWS_PER_READ_TASK = 50

    def __init__(self, table: str, dsn: str, **kwargs):
        """
        Initialize a ClickHouse Datasource.

        Args:
            table: Fully qualified table or view identifier (e.g.,
                "default.table_name").
            dsn: A string in DSN (Data Source Name) HTTP format (e.g.,
                "clickhouse+http://username:password@host:8124/default").
                For more information, see `ClickHouse Connection String doc
                <https://clickhouse.com/docs/en/integrations/sql-clients/cli#connection_string>`_.
            **kwargs: Optional additional arguments:
                - columns: List of columns to select from the data source.
                    If no columns are specified, all columns will be
                    selected by default.
                - filters: Dict of fields and values for filtering the data via
                    a WHERE clause. The value should be a tuple where the first element
                    is one of ('==', '!=', '<', '>') and the second element is the
                    value to filter by. The default operator is 'is'. Only strings,
                    ints, floats, booleans, and None are currently
                    supported as values. All filter conditions will be joined
                    using the logical AND operation. For more information,
                    see `ClickHouse WHERE Clause doc
                    <https://clickhouse.com/docs/en/sql-reference/statements/select/where>`_.

                    Example:
                    ```python
                    {
                        "text": ("!=", None),
                        "age": (">", 25),
                        "status": ("!=", "inactive")
                    }
                    ```
                    This example will filter rows where "text" IS NOT NULL,
                    "age" is greater than 25,
                    **and** "status" is not equal to "inactive".
                - order_by: Tuple containing a list of columns to order by and a
                    boolean indicating the order.
                - client_settings: ClickHouse server settings to be used with the
                    session/every request. For more information,
                    see `ClickHouse Client Settings doc
                    <https://clickhouse.com/docs/en/integrations/python#settings-argument>`_.
                - client_kwargs: Additional keyword arguments to pass to
                    the ClickHouse client. For more information,
                    see `ClickHouse Core Settings doc
                    <https://clickhouse.com/docs/en/integrations/python#additional-options>`_.
        """
        self._table = table
        self._dsn = dsn
        self._columns = kwargs.get("columns")
        self._filters = kwargs.get("filters")
        self._order_by = kwargs.get("order_by")
        self._client_settings = kwargs.get("client_settings")
        self._client_kwargs = kwargs.get("client_kwargs")
        self._query = self._generate_query()
        self._estimates = {
            "size": f"SELECT SUM(byteSize(*)) AS estimate FROM ({self._query})",
            "count": f"SELECT COUNT(*) AS estimate FROM ({self._query})",
        }

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
        if self._filters:
            filter_conditions = [
                _convert_filter_value(column, operator, value)
                for column, (operator, value) in self._filters.items()
                if _convert_filter_value(column, operator, value) is not None
            ]
            if len(filter_conditions) == 1:
                query += f" WHERE {filter_conditions[0]}"
            elif len(filter_conditions) > 1:
                filter_clause = " AND ".join(f"({i})" for i in filter_conditions)
                query += f" WHERE {filter_clause}"
        if self._order_by:
            columns, desc = self._order_by
            direction = " DESC" if desc else ""
            if len(columns) == 1:
                query += f" ORDER BY {columns[0]}{direction}"
            elif len(columns) > 1:
                columns_clause = ", ".join(columns)
                query += f" ORDER BY ({columns_clause}){direction}"
        return query

    def estimate_inmemory_data_size(self) -> Optional[int]:
        """
        Estimate the in-memory data size for the query.

        Returns:
            Estimated in-memory data size in bytes, or
             None if the estimation cannot be performed.
        """
        return self._get_estimate("size")

    def get_read_tasks(self, parallelism: int) -> List[ReadTask]:
        """
        Create read tasks for the ClickHouse query.

        Args:
            parallelism: The desired number of partitions to read the data into.

        Returns:
            A list of read tasks to be executed.
        """
        num_rows_total = self._get_estimate("count")
        if num_rows_total == 0 or num_rows_total is None:
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
        read_tasks = []
        offset = 0
        for i in range(parallelism):
            num_rows = num_rows_per_block
            if i < num_blocks_with_extra_row:
                num_rows += 1
            read_fn = self._create_read_fn(num_rows, offset)
            metadata = BlockMetadata(
                num_rows=num_rows,
                size_bytes=estimated_size_bytes_per_row * num_rows,
                schema=sample_block_schema,
                input_files=None,
                exec_stats=None,
            )
            read_tasks.append(ReadTask(read_fn, metadata))
            offset += num_rows
        return read_tasks

    def _get_estimate(self, query_type: str) -> Optional[int]:
        client = self._init_client()
        try:
            result = client.query(self._estimates[query_type])
            if result and len(result.result_rows) > 0:
                estimate = result.result_rows[0][0]
                return int(estimate) if estimate is not None else None
        except Exception as e:
            logger.warning(f"Failed to estimate query {query_type}: {e}")
        finally:
            client.close()
        return None

    def _get_sample_block(self) -> Block:
        import pyarrow as pa

        client = self._init_client()
        try:
            query = f"SELECT * FROM ({self._query}) LIMIT {self.NUM_SAMPLE_ROWS}"
            with client.query_arrow_stream(query) as stream:
                record_batches = list(stream)  # Collect all record batches
            return pa.Table.from_batches(record_batches)
        except Exception as e:
            logger.warning(f"Failed to get sample block: {e}")
        finally:
            client.close()

    def _create_read_fn(
        self, num_rows: int, offset: int
    ) -> Callable[[], Iterable[Block]]:
        def read_fn() -> Iterable[Block]:
            import pyarrow as pa

            client = self._init_client()
            query = f"SELECT * FROM ({self._query}) LIMIT {num_rows} OFFSET {offset}"
            with client.query_arrow_stream(query) as stream:
                record_batches = list(stream)  # Collect all record batches
            client.close()
            return [pa.Table.from_batches(record_batches)]

        return read_fn
