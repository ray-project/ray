import logging
from dataclasses import dataclass
from typing import Any, Iterable, List, Optional

from pandas import DataFrame

import ray
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.execution.util import memory_string
from ray.data.block import Block, BlockAccessor
from ray.util.annotations import DeveloperAPI

logger = logging.getLogger(__name__)


@dataclass
@DeveloperAPI
class WriteResult:
    """Result of a write operation, containing stats/metrics
    on the written data.

    Attributes:
        total_num_rows: The total number of rows written.
        total_size_bytes: The total size of the written data in bytes.
    """

    num_rows: int = 0
    size_bytes: int = 0
    custom_result: Any = None

    @staticmethod
    def aggregate_write_results(write_results: List["WriteResult"]) -> "WriteResult":
        """Aggregate a list of write results.

        Args:
            write_results: A list of write results.

        Returns:
            A single write result that aggregates the input results.
        """
        total_num_rows = 0
        total_size_bytes = 0

        for write_result in write_results:
            total_num_rows += write_result.num_rows
            total_size_bytes += write_result.size_bytes

        return WriteResult(
            num_rows=total_num_rows,
            size_bytes=total_size_bytes,
        )


@DeveloperAPI
class Datasink:
    """Interface for defining write-related logic.

    If you want to write data to something that isn't built-in, subclass this class
    and call :meth:`~ray.data.Dataset.write_datasink`.
    """

    def on_write_start(self) -> None:
        """Callback for when a write job starts.

        Use this method to perform setup for write tasks. For example, creating a
        staging bucket in S3.
        """
        pass

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
    ) -> Any:
        """Write blocks. This is used by a single write task.

        Args:
            blocks: Generator of data blocks.
            ctx: ``TaskContext`` for the write task.
        """
        raise NotImplementedError

    def on_write_complete(self, custom_results: List[Any]):
        pass

    def on_write_failed(self, error: Exception) -> None:
        """Callback for when a write job fails.

        This is called on a best-effort basis on write failures.

        Args:
            error: The first error encountered.
        """
        pass

    def get_name(self) -> str:
        """Return a human-readable name for this datasink.

        This is used as the names of the write tasks.
        """
        name = type(self).__name__
        datasink_suffix = "Datasink"
        if name.startswith("_"):
            name = name[1:]
        if name.endswith(datasink_suffix):
            name = name[: -len(datasink_suffix)]
        return name

    @property
    def supports_distributed_writes(self) -> bool:
        """If ``False``, only launch write tasks on the driver's node."""
        return True

    @property
    def num_rows_per_write(self) -> Optional[int]:
        """The target number of rows to pass to each :meth:`~ray.data.Datasink.write` call.

        If ``None``, Ray Data passes a system-chosen number of rows.
        """
        return None


def handle_data_sink_write_results(
    data_sink: Datasink,
    write_result_blocks: List[Block],
):
    assert all(
        isinstance(block, DataFrame) and len(block) == 1
        for block in write_result_blocks
    )
    # Print out stats about the write operation.
    write_results = [result["write_result"].iloc[0] for result in write_result_blocks]
    total_num_rows = sum(result.num_rows for result in write_results)
    total_size_bytes = sum(result.size_bytes for result in write_results)
    logger.info(
        "Data sink %s finished. %d rows and %s data written.",
        data_sink.get_name(),
        total_num_rows,
        memory_string(total_size_bytes),
    )

    # Invoke the on_write_complete callback.
    custom_results = [result.custom_result for result in write_results]
    data_sink.on_write_complete(custom_results)


@DeveloperAPI
class DummyOutputDatasink(Datasink):
    """An example implementation of a writable datasource for testing.
    Examples:
        >>> import ray
        >>> from ray.data.datasource import DummyOutputDatasink
        >>> output = DummyOutputDatasink()
        >>> ray.data.range(10).write_datasink(output)
        >>> assert output.num_ok == 1
    """

    def __init__(self):
        ctx = ray.data.DataContext.get_current()

        # Setup a dummy actor to send the data. In a real datasource, write
        # tasks would send data to an external system instead of a Ray actor.
        @ray.remote(scheduling_strategy=ctx.scheduling_strategy)
        class DataSink:
            def __init__(self):
                self.rows_written = 0
                self.enabled = True

            def write(self, block: Block) -> None:
                block = BlockAccessor.for_block(block)
                self.rows_written += block.num_rows()

            def get_rows_written(self):
                return self.rows_written

        self.data_sink = DataSink.remote()
        self.num_ok = 0
        self.num_failed = 0
        self.enabled = True

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
    ) -> None:
        tasks = []
        if not self.enabled:
            raise ValueError("disabled")
        for b in blocks:
            tasks.append(self.data_sink.write.remote(b))
        ray.get(tasks)

    def on_write_complete(self, write_result_blocks: List[Block]) -> WriteResult:
        self.num_ok += 1
        aggregated_results = super().on_write_complete(write_result_blocks)
        return aggregated_results

    def on_write_failed(self, error: Exception) -> None:
        self.num_failed += 1
