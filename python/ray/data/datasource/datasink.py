from typing import Any, Iterable, List

from ray.data._internal.execution.interfaces import TaskContext
from ray.data.block import Block

WriteResult = Any


class Datasink:
    def on_write_start(self, **_) -> None:
        """Callback for when a write job starts.

        Use this method to perform setup for write tasks. For example, creating a
        staging bucket in S3.

        Args:
            _: Forward-compatibility placeholder.
        """
        pass

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
        **_,
    ) -> WriteResult:
        """Write blocks. This is used by a single write task.

        Args:
            blocks: List of data blocks.
            ctx: ``TaskContext`` for the write task.
            _: Forward-compatibility placeholder.

        Returns:
            The output of the write task.
        """
        raise NotImplementedError

    def on_write_complete(self, write_results: List[WriteResult], **_) -> None:
        """Callback for when a write job completes.

        This can be used to "commit" a write output. This method must
        succeed prior to ``write_datasink()`` returning to the user. If this
        method fails, then ``on_write_failed()`` is called.

        Args:
            write_results: The list of the write task results.
            _: Forward-compatibility placeholder.
        """
        pass

    def on_write_failed(self, error: Exception, **_) -> None:
        """Callback for when a write job fails.

        This is called on a best-effort basis on write failures.

        Args:
            error: The first error encountered.
            _: Forward-compatibility placeholder.
        """
        pass

    def get_name(self) -> str:
        """Return a human-readable name for this datasink.

        This is used as the names of the write tasks.
        """
        name = type(self).__name__
        datasource_suffix = "Datasink"
        if name.endswith(datasource_suffix):
            name = name[: -len(datasource_suffix)]
        return name

    @property
    def supports_distributed_writes(self) -> bool:
        """If ``False``, launch writes tasks on the head node only."""
        return True
