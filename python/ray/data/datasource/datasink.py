from dataclasses import dataclass
import logging
from typing import Generic, Iterable, List, Optional, TypeVar


import ray
from ray.data._internal.execution.interfaces import TaskContext
from ray.data.block import Block, BlockAccessor
from ray.util.annotations import DeveloperAPI

logger = logging.getLogger(__name__)


WriteResultType = TypeVar("WriteResultType")


@dataclass
class WriteResult(Generic[WriteResultType]):

    num_rows: int
    size_bytes: int
    write_task_results: List[WriteResultType]


@DeveloperAPI
class Datasink(Generic[WriteResultType]):
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
    ) -> WriteResultType:
        """Write blocks. This is used by a single write task.

        Args:
            blocks: Generator of data blocks.
            ctx: ``TaskContext`` for the write task.

        Returns:
            Result of this write task. When the entire write operator succeeds,
            results of all write tasks will be passed to `on_write_complete`.
        """
        raise NotImplementedError

    def on_write_complete(self, write_result: WriteResult[WriteResultType]):
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


@DeveloperAPI
class DummyOutputDatasink(Datasink[int]):
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
        self.num_blocks_writen = 0
        self.num_failed = 0
        self.enabled = True

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
    ) -> int:
        tasks = []
        if not self.enabled:
            raise ValueError("disabled")
        for b in blocks:
            self.num_blocks_writen += 1
            tasks.append(self.data_sink.write.remote(b))
        ray.get(tasks)
        return self.num_blocks_writen

    def on_write_start(self) -> None:
        logger.info(f"Data sink {self.get_name()} started.")

    def on_write_complete(self, write_result: WriteResult[int]):
        total_num_blocks_written = sum(write_result.write_task_results)
        logger.info(
            f"Data sink {self.get_name()} finished. "
            f"{total_num_blocks_written} blocks written."
        )

    def on_write_failed(self, error: Exception) -> None:
        logger.error(f"Data sink {self.get_name()} failed: {error}")
