"""Streaming sink interface for exactly-once semantics.

This module provides the StreamingSink interface for implementing exactly-once
processing (EOS) in streaming pipelines. The sink coordinates with streaming
sources to ensure transactional consistency across microbatches.

The pattern follows Spark Structured Streaming's epoch-based commit model:
1. Source begins epoch and prepares commit (two-phase commit)
2. Sink writes data for the epoch
3. Sink commits epoch (or aborts on failure)
4. Source commits offsets only after sink commit succeeds
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Generic, Iterable, TypeVar

from ray.data.block import Block
from ray.data.datasource.datasink import Datasink
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces import TaskContext

WriteReturnType = TypeVar("WriteReturnType")


@DeveloperAPI
class StreamingSink(Datasink[WriteReturnType], ABC, Generic[WriteReturnType]):
    """Interface for streaming sinks with epoch-based exactly-once semantics.

    This interface extends Datasink with epoch-based commit semantics for
    exactly-once processing. It coordinates with streaming sources to ensure
    transactional consistency across microbatches.

    The epoch lifecycle:
    1. begin_epoch() - Called at the start of each microbatch (epoch)
    2. write_epoch() - Called for each block in the epoch (may be called multiple times)
    3. commit_epoch() or abort_epoch() - Called at the end of the epoch

    Example:
        .. testcode::
            :skipif: True

            from ray.data.datasource.streaming_sink import StreamingSink

            class MyStreamingSink(StreamingSink):
                def begin_epoch(self, batch_id: int, checkpoint: Any) -> str:
                    # Prepare for new epoch, return commit token
                    return f"epoch_{batch_id}"

                def write_epoch(self, blocks: Iterable[Block], token: str) -> None:
                    # Write blocks for this epoch
                    for block in blocks:
                        # Write to external system
                        pass

                def commit_epoch(self, token: str) -> None:
                    # Commit the epoch transactionally
                    pass

                def abort_epoch(self, token: str) -> None:
                    # Abort the epoch on failure
                    pass
    """

    @abstractmethod
    def begin_epoch(self, batch_id: int, checkpoint: Any) -> str:
        """Begin a new epoch (microbatch) and return a commit token.

        This is called at the start of each microbatch. The sink should prepare
        for writing data and return a token that will be used for subsequent
        write() and commit_epoch()/abort_epoch() calls.

        Args:
            batch_id: Unique identifier for this microbatch/epoch
            checkpoint: Checkpoint from the streaming source (e.g., Kafka offsets)

        Returns:
            Commit token (string) that identifies this epoch. This token will be
            passed to write(), commit_epoch(), and abort_epoch() calls.
        """
        raise NotImplementedError

    @abstractmethod
    def write_epoch(
        self, blocks: Iterable[Block], token: str
    ) -> WriteReturnType:
        """Write blocks for a specific epoch.

        This method is called for each block in the epoch. The token identifies
        which epoch the blocks belong to.

        Args:
            blocks: Iterable of blocks to write
            token: Commit token from begin_epoch()

        Returns:
            Write result (same as Datasink.write())
        """
        raise NotImplementedError

    @abstractmethod
    def commit_epoch(self, token: str) -> None:
        """Commit an epoch transactionally.

        This is called when all blocks for an epoch have been written successfully.
        The sink should commit the epoch transactionally. After this succeeds,
        the streaming source will commit its offsets.

        Args:
            token: Commit token from begin_epoch()
        """
        raise NotImplementedError

    @abstractmethod
    def abort_epoch(self, token: str) -> None:
        """Abort an epoch on failure.

        This is called when an epoch fails (e.g., write() raises an exception).
        The sink should rollback any partial writes for this epoch.

        Args:
            token: Commit token from begin_epoch()
        """
        raise NotImplementedError

    def write(
        self,
        blocks: Iterable[Block],
        ctx: "TaskContext",
    ) -> WriteReturnType:
        """Write blocks (implements Datasink interface).

        This method is called by Ray Data's write operator. For streaming sinks,
        it delegates to write_epoch() using the token from the context.

        Args:
            blocks: Iterable of blocks to write
            ctx: TaskContext containing epoch token if available

        Returns:
            Write result
        """
        # Extract token from context if available
        token = getattr(ctx, "epoch_token", None)
        if token is None:
            raise ValueError(
                "StreamingSink.write() called without epoch_token in context. "
                "Use begin_epoch() first."
            )
        return self.write_epoch(blocks, token)
