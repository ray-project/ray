import logging
import math
import re
from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING, Any

from ray.data._internal.object_extensions.arrow import raise_on_pickle_object_columns
from ray.data._internal.util import _check_import
from ray.data.block import Block, BlockAccessor, BlockMetadata
from ray.data.datasource.datasource import Datasource, ReadTask
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from ray.data.context import DataContext

logger = logging.getLogger(__name__)

# Matches strictly ClickHouse typed placeholders like {name:Type}
TYPED_PARAM_PATTERN = re.compile(r"\{([a-zA-Z_][a-zA-Z0-9_]*):([a-zA-Z0-9_(),\s]+)\}")
# Matches legacy or non-typed placeholders like %(name)s or bare {name}
INVALID_PARAM_PATTERN = re.compile(
    r"(%\([a-zA-Z_][a-zA-Z0-9_]*\)[s|d|f]|\{([a-zA-Z_][a-zA-Z0-9_]*)\})"
)


def validate_query_parameters(
    filter_str: str | None, query_parameters: dict[str, Any] | None
) -> None:
    """Validate placeholders in filter match ClickHouse Connect {name:Type} syntax."""
    if not query_parameters:
        return

    if not filter_str:
        raise ValueError(
            "`query_parameters` provided but no `filter` predicate was specified."
        )

    # Check for invalid/non-typed placeholder formats directly
    invalid_matches = INVALID_PARAM_PATTERN.findall(filter_str)
    for full_match, _ in invalid_matches:
        raise ValueError(
            f"Invalid placeholder format '{full_match}' in filter. "
            "Only strict typed placeholders in format '{{name:Type}}' are supported."
        )

    # Validate that all parameters in dict match typed placeholders in filter
    typed_placeholders = dict(TYPED_PARAM_PATTERN.findall(filter_str))
    missing_keys = set(typed_placeholders.keys()) - set(query_parameters.keys())
    if missing_keys:
        err_keys = sorted(missing_keys)
        raise ValueError(
            f"Missing values in `query_parameters` for filter placeholders: {err_keys}"
        )


def quote_clickhouse_identifier(name: str) -> str:
    """Validate and backtick-quote a single ClickHouse identifier."""
    if not isinstance(name, str) or not name.strip():
        raise ValueError(
            f"Invalid ClickHouse identifier: {name!r}. Must be a non-empty string."
        )

    sanitized_name = name.strip().replace("`", "``")
    return f"`{sanitized_name}`"


def parse_and_quote_table_identifier(table_input: str) -> str:
    """Parse and quote table input, supporting 'table' or 'db.table' format."""
    if not isinstance(table_input, str) or not table_input.strip():
        raise ValueError(
            f"Invalid table identifier: {table_input!r}. Expected non-empty string."
        )

    parts = table_input.strip().split(".")
    if len(parts) == 1:
        return quote_clickhouse_identifier(parts[0])
    elif len(parts) == 2:
        db, tbl = parts[0].strip(), parts[1].strip()
        if not db or not tbl:
            raise ValueError(
                f"Invalid 'database.table' specification: {table_input!r}."
            )
        return f"{quote_clickhouse_identifier(db)}.{quote_clickhouse_identifier(tbl)}"
    else:
        raise ValueError(
            f"Invalid table specification: {table_input!r}. "
            "Expected 'table' or 'database.table'."
        )


def format_selected_columns(columns: list[str] | None) -> str:
    """Format and backtick-quote selected column identifiers for the SELECT clause."""
    if not columns:
        return "*"

    quoted_cols = [quote_clickhouse_identifier(col) for col in columns]
    return ", ".join(quoted_cols)


def _is_filter_string_safe(filter_str: str) -> bool:
    in_string = False
    escape_next = False
    for c in filter_str:
        if in_string:
            if c == "'" and not escape_next:
                in_string = False
            escape_next = (c == "\\") and not escape_next
        else:
            if c == "'":
                in_string = True
                escape_next = False
            elif c == ";":
                return False
            else:
                escape_next = False
    return True


@DeveloperAPI
class ClickHouseDatasource(Datasource):
    """
    A Ray datasource for reading from ClickHouse.

    Args:
        table: Fully qualified table or view identifier (e.g., "default.table_name").
        dsn: A string in DSN (Data Source Name) HTTP format (e.g.,
            "clickhouse+http://username:password@host:8124/default").
            For more information, see `ClickHouse Connection String doc
            <https://clickhouse.com/docs/en/integrations/sql-clients/cli#connection_string>`_.
        columns: Optional List of columns to select from the data source.
            If no columns are specified, all columns will be selected by default.
        filter: Optional SQL filter string that will be used in the
            WHERE statement (e.g., "label = 2 AND text IS NOT NULL").
            The filter must be valid for use in a ClickHouse SQL WHERE clause.
            Note: Parallel reads are not currently supported when a filter is set.
            Specifying a filter forces the parallelism to 1 to ensure deterministic
            and consistent results. For more information, see
            `ClickHouse SQL WHERE Clause doc
            <https://clickhouse.com/docs/en/sql-reference/statements/select/where>`_.
        query_parameters: Optional dictionary of query parameters to bind to the
            filter placeholders. Placeholders must use the strict ClickHouse Connect
            `{name:Type}` syntax.
        order_by: Optional Tuple containing a list of columns to order by
            and a boolean indicating the order. Note: order_by is required to
            support parallelism.
        client_settings: Optional ClickHouse server settings to be used with the
            session/every request. For more information, see
            `ClickHouse Client Settings doc
            <https://clickhouse.com/docs/en/integrations/python#settings-argument>`_.
        client_kwargs: Optional Additional keyword arguments to pass to the
            ClickHouse client. For more information, see
            `ClickHouse Core Settings doc
            <https://clickhouse.com/docs/en/integrations/python#additional-options>`_.
    """

    NUM_SAMPLE_ROWS = 100
    MIN_ROWS_PER_READ_TASK = 50
    _BASE_QUERY = "SELECT {select_clause} FROM {table}"
    _EXPLAIN_FILTERS_QUERY = "EXPLAIN SELECT 1 FROM {table} WHERE {filter_clause}"
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
        columns: list[str] | None = None,
        filter: str | None = None,
        query_parameters: dict[str, Any] | None = None,
        order_by: tuple[list[str], bool] | None = None,
        client_settings: dict[str, Any] | None = None,
        client_kwargs: dict[str, Any] | None = None,
    ):
        self._raw_table = table
        self._table = parse_and_quote_table_identifier(table)
        self._dsn = dsn
        self._columns = columns
        self._filter = filter
        self._query_parameters = query_parameters
        validate_query_parameters(self._filter, self._query_parameters)
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

    def _validate_filter(self):
        if not self._filter:
            return
        if not _is_filter_string_safe(self._filter):
            err_msg = f"Invalid characters outside of string literals in filter: {self._filter}"
            raise ValueError(err_msg)
        client = self._init_client()
        try:
            test_query = self._EXPLAIN_FILTERS_QUERY.format(
                table=self._table,
                filter_clause=self._filter,
            )
            client.query(test_query, parameters=self._query_parameters)
        except Exception as e:  # noqa: BLE001
            raise ValueError(
                f"Invalid filter expression: {self._filter}. Error: {e}",
            )
        finally:
            client.close()

    def _generate_query(self) -> str:
        select_clause = format_selected_columns(self._columns)
        query = self._BASE_QUERY.format(
            select_clause=select_clause,
            table=self._table,
        )
        if self._filter:
            self._validate_filter()
            query += f" WHERE {self._filter}"
        if self._order_by:
            columns, desc = self._order_by
            quoted_order_columns = [quote_clickhouse_identifier(c) for c in columns]
            direction = " DESC" if desc else ""
            if len(quoted_order_columns) == 1:
                query += f" ORDER BY {quoted_order_columns[0]}{direction}"
            elif len(quoted_order_columns) > 1:
                columns_clause = ", ".join(quoted_order_columns)
                query += f" ORDER BY ({columns_clause}){direction}"
        return query

    def _build_block_query(self, limit_row_count: int, offset_row_count: int) -> str:
        if offset_row_count == 0:
            return self._FIRST_BLOCK_QUERY.format(
                query=self._query,
                fetch_row_count=limit_row_count,
                fetch_row_or_rows="ROWS" if limit_row_count > 1 else "ROW",
            )
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
            query = self._FIRST_BLOCK_QUERY.format(
                query=self._query,
                fetch_row_count=self.NUM_SAMPLE_ROWS,
                fetch_row_or_rows="ROWS" if self.NUM_SAMPLE_ROWS > 1 else "ROW",
            )
        else:
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

    def _get_estimate_count(self) -> int | None:
        return self._execute_estimate_query(self._COUNT_ESTIMATE_QUERY)

    def _get_estimate_size(self) -> int | None:
        return self._execute_estimate_query(self._SIZE_ESTIMATE_QUERY)

    def _execute_estimate_query(self, estimate_query: str) -> int | None:
        client = self._init_client()
        try:
            query = estimate_query.format(query=self._query)
            result = client.query(query, parameters=self._query_parameters)
            if result and len(result.result_rows) > 0:
                estimate = result.result_rows[0][0]
                return int(estimate) if estimate is not None else None
        except Exception as e:  # noqa: BLE001
            logger.warning(f"Failed to execute estimate query: {e}")
        finally:
            client.close()
        return None

    def _execute_block_query(self, query: str) -> Block:
        import pyarrow as pa

        client = self._init_client()
        try:
            with client.query_arrow_stream(
                query, parameters=self._query_parameters
            ) as stream:
                record_batches = list(stream)
            table = pa.Table.from_batches(record_batches)
        except Exception as e:  # noqa: BLE001
            raise RuntimeError(f"Failed to execute block query: {e}")
        finally:
            client.close()
        raise_on_pickle_object_columns(table)
        return table

    def estimate_inmemory_data_size(self) -> int | None:
        return self._get_estimate_size()

    def get_read_tasks(
        self,
        parallelism: int,
        per_task_row_limit: int | None = None,
        data_context: "DataContext | None" = None,
    ) -> list[ReadTask]:
        num_rows_total = self._get_estimate_count()
        if num_rows_total == 0 or num_rows_total is None:
            return []
        parallelism = min(
            parallelism, math.ceil(num_rows_total / self.MIN_ROWS_PER_READ_TASK)
        )
        if self._filter is not None and parallelism > 1:
            logger.warning(
                "ClickHouse datasource does not currently support parallel reads "
                "when a filter is set; falling back to parallelism of 1."
            )
            parallelism = 1
        if self._order_by is None and parallelism > 1:
            logger.warning(
                "ClickHouse datasource requires dataset to be explicitly ordered "
                "to support parallelism; falling back to parallelism of 1."
            )
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
                query = self._build_block_query(block_rows, offset_rows)
            else:
                query = self._query
            return ReadTask(
                self._create_read_fn(query),
                BlockMetadata(
                    num_rows=block_rows,
                    size_bytes=estimated_size_bytes_per_row * block_rows,
                    input_files=None,
                    exec_stats=None,
                ),
                schema=sample_block_schema,
                per_task_row_limit=per_task_row_limit,
            )

        if parallelism == 1:
            return [_get_read_task(num_rows_total, 0, False)]

        read_tasks = []
        offset = 0
        for i in range(parallelism):
            this_block_size = num_rows_per_block
            if i < num_blocks_with_extra_row:
                this_block_size += 1
            read_tasks.append(_get_read_task(this_block_size, offset, True))
            offset += this_block_size
        return read_tasks
