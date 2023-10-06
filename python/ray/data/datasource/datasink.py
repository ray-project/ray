from typing import Any, Iterable, List

from ray.data.block import Block
from ray.data.datasource import TaskContext

WriteResult = Any


class Datasink:
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
    ) -> WriteResult:
        """Write blocks out to the datasource. This is used by a single write task.

        Args:
            blocks: List of data blocks.
            ctx: ``TaskContext`` for the write task.

        Returns:
            The output of the write task.
        """
        raise NotImplementedError

    def on_write_complete(self, write_results: List[WriteResult], **kwargs) -> None:
        """Callback for when a write job completes.

        This can be used to "commit" a write output. This method must
        succeed prior to ``write_datasource()`` returning to the user. If this
        method fails, then ``on_write_failed()`` will be called.

        Args:
            write_results: The list of the write task results.
            kwargs: Forward-compatibility placeholder.
        """
        pass

    def on_write_failed(self, error: Exception, **kwargs) -> None:
        """Callback for when a write job fails.

        This is called on a best-effort basis on write failures.

        Args:
            write_results: The list of the write task result futures.
            error: The first error encountered.
            kwargs: Forward-compatibility placeholder.
        """
        pass
