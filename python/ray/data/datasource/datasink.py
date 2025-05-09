import logging
from dataclasses import dataclass
from typing import Generic, Iterable, List, Optional, TypeVar

import ray
from ray.data._internal.execution.interfaces import TaskContext
from ray.data.block import Block, BlockAccessor
from ray.util.annotations import DeveloperAPI

logger = logging.getLogger(__name__)


WriteReturnType = TypeVar("WriteReturnType")
"""Generic type for the return value of `Datasink.write`."""


@dataclass
@DeveloperAPI
class WriteResult(Generic[WriteReturnType]):
    """Aggregated result of the Datasink write operations."""

    # Total number of written rows.
    num_rows: int
    # Total size in bytes of written data.
    size_bytes: int
    # All returned values of `Datasink.write`.
    write_returns: List[WriteReturnType]


@DeveloperAPI
class Datasink(Generic[WriteReturnType]):
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
    ) -> WriteReturnType:
        """Write blocks. This is used by a single write task.

        Args:
            blocks: Generator of data blocks.
            ctx: ``TaskContext`` for the write task.

        Returns:
            Result of this write task. When the entire write operator finishes,
            All returned values will be passed as `WriteResult.write_returns`
            to `Datasink.on_write_complete`.
        """
        raise NotImplementedError

    def on_write_complete(self, write_result: WriteResult[WriteReturnType]):
        """Callback for when a write job completes.

        This can be used to `commit` a write output. This method must
        succeed prior to ``write_datasink()`` returning to the user. If this
        method fails, then ``on_write_failed()`` is called.

        Args:
            write_result: Aggregated result of the
               Write operator, containing write results and stats.
        """
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
    def min_rows_per_write(self) -> Optional[int]:
        """The target number of rows to pass to each :meth:`~ray.data.Datasink.write` call.

        If ``None``, Ray Data passes a system-chosen number of rows.
        """
        return None


@DeveloperAPI
class DummyOutputDatasink(Datasink[None]):
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

    def on_write_complete(self, write_result: WriteResult[None]):
        self.num_ok += 1

    def on_write_failed(self, error: Exception) -> None:
        self.num_failed += 1
