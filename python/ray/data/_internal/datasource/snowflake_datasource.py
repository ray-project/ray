import math
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterable, List, Optional

import numpy as np
import pyarrow as pa

from ray.data._internal.object_extensions.arrow import raise_on_pickle_object_columns
from ray.data._internal.util import _check_import
from ray.data.block import Block, BlockMetadata
from ray.data.datasource.datasource import Datasource, ReadTask

if TYPE_CHECKING:
    from snowflake.connector.result_batch import ArrowResultBatch, ResultBatch

    from ray.data.context import DataContext


class SnowflakeDatasource(Datasource):
    """Reads the result of a Snowflake query.

    The query runs once, on the driver, the first time Ray Data plans the read.
    Snowflake returns the result as result batches that workers can download
    without a connection, so each read task downloads a contiguous group of
    batches as Arrow tables.
    """

    MIN_ROWS_PER_READ_TASK = 50

    # `_download` passes `force_microsecond_precision`, which connector 4.3.0 added.
    MIN_CONNECTOR_VERSION = "4.3.0"

    def __init__(self, sql: str, connection_parameters: Dict[str, Any]):
        _check_import(
            self,
            module="snowflake",
            package=f"snowflake-connector-python>={self.MIN_CONNECTOR_VERSION}",
        )
        self._check_connector_version()

        self._sql = sql
        self._connection_parameters = connection_parameters
        # Set by `_execute_query`. Ray Data can call `get_read_tasks` more than
        # once, so the result is kept to avoid running the query again.
        self._result_batches: Optional[List["ArrowResultBatch"]] = None
        self._column_names: List[str] = []

    def estimate_inmemory_data_size(self) -> Optional[int]:
        return None

    def get_read_tasks(
        self,
        parallelism: int,
        per_task_row_limit: Optional[int] = None,
        data_context: Optional["DataContext"] = None,
    ) -> List[ReadTask]:
        if self._result_batches is None:
            self._execute_query()

        if not self._result_batches:
            # Like `read_sql`, return the column names even if there are no rows.
            empty_table = pa.table(
                {name: pa.array([], type=pa.null()) for name in self._column_names}
            )
            metadata = BlockMetadata(
                num_rows=0, size_bytes=0, input_files=None, exec_stats=None
            )
            return [
                ReadTask(lambda: [empty_table], metadata, schema=empty_table.schema)
            ]

        num_rows_total = sum(batch.rowcount for batch in self._result_batches)
        parallelism = max(
            1,
            min(
                parallelism,
                math.ceil(num_rows_total / self.MIN_ROWS_PER_READ_TASK),
                len(self._result_batches),
            ),
        )

        # Download one batch to get the schema and estimate the size of a row.
        sample_table = _download(self._result_batches[0])
        bytes_per_row = sample_table.nbytes / sample_table.num_rows

        read_tasks = []
        for indices in np.array_split(
            np.arange(len(self._result_batches)), parallelism
        ):
            result_batches = [self._result_batches[i] for i in indices]
            num_rows = sum(batch.rowcount for batch in result_batches)
            metadata = BlockMetadata(
                num_rows=num_rows,
                size_bytes=int(bytes_per_row * num_rows),
                input_files=None,
                exec_stats=None,
            )
            read_tasks.append(
                ReadTask(
                    _create_read_fn(result_batches),
                    metadata,
                    schema=sample_table.schema,
                    per_task_row_limit=per_task_row_limit,
                )
            )

        return read_tasks

    @classmethod
    def _check_connector_version(cls) -> None:
        import importlib.metadata

        from packaging.version import parse

        version = importlib.metadata.version("snowflake-connector-python")
        if parse(version) < parse(cls.MIN_CONNECTOR_VERSION):
            raise ImportError(
                f"`read_snowflake` needs snowflake-connector-python "
                f"{cls.MIN_CONNECTOR_VERSION} or later, but found {version}. "
                f"Upgrade it by running `pip install -U "
                f"'snowflake-connector-python>={cls.MIN_CONNECTOR_VERSION}'`."
            )

    def _execute_query(self) -> None:
        from snowflake.connector import connect

        with connect(
            **self._arrow_connection_parameters()
        ) as connection, connection.cursor() as cursor:
            cursor.execute(self._sql)
            result_batches = cursor.get_result_batches() or []
            # Each item in `description` is a sequence whose first element is the
            # column name. To learn more, read https://peps.python.org/pep-0249/.
            self._column_names = [column[0] for column in cursor.description]

        arrow_batches = _require_arrow_result_batches(result_batches)

        # Drop batches without rows. `get_read_tasks` samples the first batch to
        # estimate how many bytes a row takes, which needs a row to divide by, and
        # empty blocks only add overhead downstream.
        self._result_batches = [batch for batch in arrow_batches if batch.rowcount > 0]

    def _arrow_connection_parameters(self) -> Dict[str, Any]:
        """The connection parameters, with the result format pinned to Arrow.

        Read tasks turn result batches into blocks with `ResultBatch.to_arrow`,
        which only works on Arrow batches -- the JSON ones raise
        `NotSupportedError`. An account, a user or a session can default the
        format to JSON, so ask for Arrow on this connection. To learn more, read
        https://docs.snowflake.com/en/sql-reference/parameters.

        Returns:
            A copy of the connection parameters that asks for Arrow batches.
        """
        session_parameters = {
            **self._connection_parameters.get("session_parameters", {}),
            "PYTHON_CONNECTOR_QUERY_RESULT_FORMAT": "ARROW",
        }
        return {**self._connection_parameters, "session_parameters": session_parameters}


def _require_arrow_result_batches(
    result_batches: List["ResultBatch"],
) -> List["ArrowResultBatch"]:
    """Returns the batches as Arrow batches, or raises if any isn't one.

    `_arrow_connection_parameters` asks for Arrow, but the connector overrides
    that per statement if its Arrow extension is missing, and `to_arrow` then
    fails inside a read task. Catch it on the driver with a message that says
    what to do instead.

    Args:
        result_batches: The batches the query returned.

    Returns:
        The same batches, typed as Arrow batches.

    Raises:
        RuntimeError: If any batch isn't an Arrow batch.
    """
    from snowflake.connector.result_batch import ArrowResultBatch

    arrow_batches = [
        batch for batch in result_batches if isinstance(batch, ArrowResultBatch)
    ]
    if len(arrow_batches) != len(result_batches):
        raise RuntimeError(
            "`read_snowflake` needs Snowflake's Arrow result format, but the query "
            "returned JSON batches. This usually means the installed "
            "snowflake-connector-python can't use Arrow. Install a build with Arrow "
            "support, or pass `shard_keys` to read through `read_sql` instead."
        )
    return arrow_batches


def _create_read_fn(
    result_batches: List["ArrowResultBatch"],
) -> Callable[[], Iterable[Block]]:
    def read_fn() -> Iterable[Block]:
        for result_batch in result_batches:
            yield _download(result_batch)

    return read_fn


def _download(result_batch: "ArrowResultBatch") -> pa.Table:
    # The connector picks a timestamp unit per batch from the data in that batch, so
    # one `TIMESTAMP` column can come back as `timestamp[ns]` from one batch and
    # `timestamp[us]` from another: a batch holding a value outside the nanosecond
    # range (before 1677 or after 2262, like a `9999-12-31` end date) downscales to
    # microseconds. Read tasks would then emit blocks that don't share a schema, and
    # the schema reported here, which comes from the first batch, could be wrong. So
    # pin every batch to microseconds. This only affects `TIMESTAMP` columns with a
    # scale over 6, which the connector would otherwise read as nanoseconds.
    #
    # `to_arrow` also retries the download itself -- up to `MAX_DOWNLOAD_RETRY` (10)
    # attempts with exponential backoff, on any error -- so don't add another retry.
    table = result_batch.to_arrow(force_microsecond_precision=True)

    # The table comes from outside this process. Unpickling a pickled-object column
    # can run arbitrary code, so reject the table before anything uses it.
    raise_on_pickle_object_columns(table)

    return table
