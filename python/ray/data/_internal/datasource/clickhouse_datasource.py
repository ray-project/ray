import logging
from typing import Any, Dict, List, Optional, Tuple

from ray.data._internal.util import _check_import
from ray.data.block import Block, BlockMetadata
from ray.data.datasource.datasource import Datasource, ReadTask
from ray.util.annotations import DeveloperAPI

logger = logging.getLogger(__name__)


@DeveloperAPI
class ClickHouseDatasource(Datasource):
    """
    A Ray datasource for reading from ClickHouse.
    """

    def __init__(
        self,
        entity: str,
        dsn: str,
        columns: Optional[List[str]] = None,
        filters: Optional[Dict[str, Tuple[str, Any]]] = None,
        order_by: Optional[Tuple[List[str], bool]] = None,
        client_settings: Optional[Dict[str, Any]] = None,
        client_kwargs: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize a ClickHouseDatasource.

        Args:
            entity: Fully qualified table or view identifier (e.g.,
                "default.table_name").
            dsn: A string in standard DSN (Data Source Name) format.
            columns: Optional list of columns to select from the data source.
                If no columns are specified, all columns will be selected by default.
            filters: Optional fields and values mapping to use to filter the data via
                WHERE clause. The value should be a tuple where the first element is
                one of ('is', 'not', 'less', 'greater') and the second
                element is the value to filter by. The default operator
                is 'is'. Only strings, ints, floats, booleans,
                and None are allowed as values.
            order_by: Optional tuple containing a list of columns to order by and a
                boolean indicating whether the order should be descending
                (True for DESC, False for ASC).
            client_settings: ClickHouse server settings to be used with the
                session/every request
            client_kwargs: Optional keyword arguments to pass to the
                ClickHouse client.
        """
        self._entity = entity
        self._dsn = dsn
        self._columns = columns
        self._filters = filters
        self._order_by = order_by
        self._client_settings = client_settings if client_settings is not None else {}
        self._client_kwargs = client_kwargs if client_kwargs is not None else {}
        self._client = None
        self._query = self._generate_query()

    def _get_or_create_client(self):
        _check_import(self, module="clickhouse_connect", package="clickhouse-connect")
        import clickhouse_connect

        if self._client is None:
            self._client = clickhouse_connect.get_client(
                dsn=self._dsn,
                settings=self._client_settings,
                **self._client_kwargs,
            )

    def _generate_query(self) -> str:
        select_clause = ", ".join(self._columns) if self._columns else "*"
        query = f"SELECT {select_clause} FROM {self._entity}"

        if self._filters:

            def validate_non_numeric_ops(column: str, op: str) -> str:
                non_numeric_ops = ["is", "not"]
                if op not in non_numeric_ops:
                    logger.warning(
                        f"Unsupported operator '{op}' for filter on '{column}'. "
                        f"Defaulting to 'is'"
                    )
                    op = "is"
                return op

            ops = {"is": "=", "not": "!=", "less": "<", "greater": ">"}
            filter_conditions = []
            for key, (operator, value) in self._filters.items():
                operator = operator.lower()
                if operator not in ops:
                    logger.warning(
                        f"Unsupported operator '{operator}' for filter on '{key}' "
                        f"Defaulting to 'is'"
                    )
                    operator = "is"
                if value is None:
                    operator = validate_non_numeric_ops(key, operator)
                    if operator == "is":
                        filter_conditions.append(f"{key} IS NULL")
                    elif operator == "not":
                        filter_conditions.append(f"{key} IS NOT NULL")
                elif isinstance(value, str):
                    operator = validate_non_numeric_ops(key, operator)
                    filter_conditions.append(f"{key} {ops[operator]} '{value}'")
                elif isinstance(value, bool):
                    operator = validate_non_numeric_ops(key, operator)
                    filter_conditions.append(
                        f"{key} {ops[operator]} {str(value).lower()}"
                    )
                elif isinstance(value, (int, float)):
                    filter_conditions.append(f"{key} {ops[operator]} {value}")
                else:
                    logger.warning(
                        f"Unsupported data type for filter on "
                        f"'{key}': {type(value).__name__}"
                    )

            query += f" WHERE {' AND '.join(filter_conditions)}"

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
        self._get_or_create_client()
        estimate_query = f"SELECT SUM(byteSize(*)) AS total_size FROM ({self._query})"
        try:
            result = self._client.query(estimate_query)
            if result and len(result.result_rows) > 0:
                total_size = result.result_rows[0][0]
                return int(total_size) if total_size is not None else None
        except Exception as e:
            logger.warning(f"Failed to estimate query size: {e}")
        return None

    def get_read_tasks(self, parallelism: int) -> List[ReadTask]:
        """
        Create read tasks for the ClickHouse query.

        Args:
            parallelism: The desired number of partitions to read the data into.

        Returns:
            A list of read tasks to be executed.
        """
        import pyarrow as pa

        def _create_read_fn(partition_batches):
            def _read_fn() -> Block:
                yield pa.Table.from_batches(partition_batches)

            return _read_fn

        self._get_or_create_client()

        # Fetch the fragments from the ClickHouse client
        with self._client.query_arrow_stream(self._query) as stream:
            record_batches = list(stream)  # Collect all record batches

        # Split the record batches into partitions for parallelism
        num_batches = len(record_batches)
        batches_per_partition = max(1, num_batches // parallelism)
        partitions = [
            record_batches[i : i + batches_per_partition]
            for i in range(0, num_batches, batches_per_partition)
        ]

        read_tasks = []

        for partition in partitions:
            if len(partition) == 0:
                continue
            num_rows = sum(batch.num_rows for batch in partition)
            size_bytes = sum(batch.nbytes for batch in partition)
            schema = partition[0].schema if partition else None
            metadata = BlockMetadata(
                num_rows=num_rows,
                size_bytes=size_bytes,
                schema=schema,
                input_files=[],
                exec_stats=None,
            )
            read_fn = _create_read_fn(partition)
            read_tasks.append(ReadTask(read_fn, metadata))

        return read_tasks
