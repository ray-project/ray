import math
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterable, List, Optional

import numpy as np
import pyarrow as pa

from ray._common.retry import call_with_retry
from ray.data._internal.util import _check_import
from ray.data.block import Block, BlockMetadata
from ray.data.datasource.datasource import Datasource, ReadTask

if TYPE_CHECKING:
    from snowflake.connector.result_batch import ResultBatch

    from ray.data.context import DataContext


class SnowflakeDatasource(Datasource):
    """Reads the result of a Snowflake query.

    The query runs once, on the driver, the first time Ray Data plans the read.
    Snowflake returns the result as result batches that workers can download
    without a connection, so each read task downloads a contiguous group of
    batches as Arrow tables.
    """

    MIN_ROWS_PER_READ_TASK = 50

    def __init__(self, sql: str, connection_parameters: Dict[str, Any]):
        _check_import(self, module="snowflake", package="snowflake-connector-python")

        self._sql = sql
        self._connection_parameters = connection_parameters
        # Set by `_execute_query`. Ray Data can call `get_read_tasks` more than
        # once, so the result is kept to avoid running the query again.
        self._result_batches: Optional[List["ResultBatch"]] = None
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

    def _execute_query(self) -> None:
        from snowflake.connector import connect

        with connect(
            **self._connection_parameters
        ) as connection, connection.cursor() as cursor:
            cursor.execute(self._sql)
            result_batches = cursor.get_result_batches() or []
            # Each item in `description` is a sequence whose first element is the
            # column name. To learn more, read https://peps.python.org/pep-0249/.
            self._column_names = [column[0] for column in cursor.description]

        # Drop batches without rows. For those, some connector versions return `None`
        # from `ResultBatch.to_arrow`.
        self._result_batches = [batch for batch in result_batches if batch.rowcount > 0]


def _create_read_fn(
    result_batches: List["ResultBatch"],
) -> Callable[[], Iterable[Block]]:
    def read_fn() -> Iterable[Block]:
        for result_batch in result_batches:
            yield _download(result_batch)

    return read_fn


def _download(result_batch: "ResultBatch") -> pa.Table:
    return call_with_retry(
        result_batch.to_arrow, description="download a Snowflake result batch"
    )
