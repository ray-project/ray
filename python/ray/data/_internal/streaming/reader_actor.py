"""Reader actors for streaming datasources.

This module provides long-lived reader actors that maintain connections to streaming
datasources (Kafka, Kinesis, etc.) to reduce ray.remote overhead.

Instead of launching new tasks for each microbatch, reader actors expose a poll_batch()
method that can be called repeatedly, maintaining connection state between calls.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Iterator, List, Optional

import ray
from ray.data.datasource import Datasource, ReadTask
from ray.util.annotations import DeveloperAPI

# ObjectRefGenerator is only available in newer Ray versions
try:
    from ray._raylet import ObjectRefGenerator
except ImportError:
    # Fallback for older Ray versions
    from typing import Any

    ObjectRefGenerator = Any

logger = logging.getLogger(__name__)


@DeveloperAPI
@ray.remote
class StreamingReaderActor:
    """Long-lived actor that maintains a connection to a streaming datasource.

    This actor reduces ray.remote overhead by maintaining connections (e.g., Kafka
    consumers) across multiple microbatches. Instead of creating new tasks for each
    microbatch, the operator calls poll_batch() on the actor.

    The actor is responsible for:
    1. Maintaining the datasource connection (initialized once)
    2. Polling data for each microbatch via poll_batch()
    3. Managing checkpoint state per partition
    4. Yielding blocks incrementally via ObjectRefGenerator

    Example:
        .. testcode::
            :skipif: True

            from ray.data._internal.streaming.reader_actor import StreamingReaderActor

            # Create actor with datasource and partition assignment
            actor = StreamingReaderActor.remote(
                datasource=my_kafka_datasource,
                partition_ids=["topic-0", "topic-1"],
                checkpoint={"topic-0": 100, "topic-1": 200}
            )

            # Poll for a batch (returns ObjectRefGenerator)
            gen = actor.poll_batch.remote(
                max_records=1000,
                max_bytes=10*1024*1024,
                batch_id=1
            )

            # Iterate over blocks
            for block_ref in gen:
                block = ray.get(block_ref)
                # Process block
    """

    def __init__(
        self,
        datasource: Datasource,
        partition_ids: List[str],
        checkpoint: Optional[Dict[str, Any]] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
    ):
        """Initialize the reader actor.

        Args:
            datasource: The streaming datasource to read from
            partition_ids: List of partition IDs assigned to this actor
            checkpoint: Initial checkpoint state (e.g., Kafka offsets)
            ray_remote_args: Ray remote args for the actor
        """
        self.datasource = datasource
        self.partition_ids = partition_ids
        self.checkpoint = checkpoint or {}
        self.ray_remote_args = ray_remote_args or {}

        # Initialize datasource connection if supported
        # Some datasources may need to set up connections here
        if hasattr(datasource, "initialize_reader_actor"):
            datasource.initialize_reader_actor(self, partition_ids)

        logger.info(
            f"StreamingReaderActor initialized for partitions: {partition_ids}"
        )

    def poll_batch(
        self,
        max_records: Optional[int] = None,
        max_bytes: Optional[int] = None,
        max_splits: Optional[int] = None,
        batch_id: int = 0,
    ) -> ObjectRefGenerator:
        """Poll a batch of data from the assigned partitions.

        This method is called for each microbatch. It reads data up to the specified
        limits and yields blocks incrementally via ObjectRefGenerator.

        Args:
            max_records: Maximum records to read in this batch
            max_bytes: Maximum bytes to read in this batch
            max_splits: Maximum splits/partitions to read from
            batch_id: Microbatch ID for checkpointing

        Returns:
            ObjectRefGenerator that yields blocks from this batch
        """
        # Get read tasks for this batch from the datasource
        # The datasource should use the actor's checkpoint state
        try:
            read_tasks = self._get_read_tasks_for_batch(
                max_records=max_records,
                max_bytes=max_bytes,
                max_splits=max_splits,
                batch_id=batch_id,
            )

            if not read_tasks:
                # No data available - return empty generator
                return self._empty_generator()

            # Execute read tasks and yield blocks incrementally
            # Use the first read task's read_fn (assuming all tasks use same pattern)
            read_fn = read_tasks[0].read_fn

            # Call read_fn with current checkpoint
            # The read_fn should yield blocks incrementally
            result = read_fn()

            # If read_fn returns an ObjectRefGenerator, return it directly
            if isinstance(result, ObjectRefGenerator):
                return result

            # Otherwise, wrap the result in a generator
            return self._wrap_result(result)

        except Exception as e:
            logger.error(f"Error polling batch {batch_id}: {e}", exc_info=e)
            raise

    def _get_read_tasks_for_batch(
        self,
        max_records: Optional[int],
        max_bytes: Optional[int],
        max_splits: Optional[int],
        batch_id: int,
    ) -> List[ReadTask]:
        """Get read tasks for this batch from the datasource.

        This method calls the datasource's get_read_tasks() with the actor's
        partition assignment and checkpoint state.
        """
        # Datasource must support reader actor pattern
        if not hasattr(self.datasource, "get_read_tasks_for_reader_actor"):
            raise RuntimeError(
                f"Datasource {type(self.datasource).__name__} does not support "
                "reader actors. Implement get_read_tasks_for_reader_actor() or "
                "disable reader actors."
            )

        return self.datasource.get_read_tasks_for_reader_actor(
            partition_ids=self.partition_ids,
            checkpoint=self.checkpoint,
            max_records_per_trigger=max_records,
            max_bytes_per_trigger=max_bytes,
            max_splits_per_trigger=max_splits,
            batch_id=batch_id,
        )

    def _task_matches_partitions(
        self, task: ReadTask, partition_ids: List[str]
    ) -> bool:
        """Check if a read task matches our assigned partitions.

        This is datasource-specific and may need to be overridden.
        """
        # Try to extract partition info from task metadata
        if hasattr(task, "metadata") and task.metadata:
            # Check if metadata contains partition info
            if hasattr(task.metadata, "input_files"):
                for file in task.metadata.input_files or []:
                    # Simple heuristic: check if partition ID is in the file path
                    if any(pid in str(file) for pid in partition_ids):
                        return True
        return False

    def _empty_generator(self) -> ObjectRefGenerator:
        """Return an empty ObjectRefGenerator."""
        # Create a simple generator that yields nothing
        @ray.remote(num_returns="streaming")
        def _empty_read_fn():
            # Empty generator - just return without yielding
            if False:
                yield  # Make it a generator function

        return _empty_read_fn.remote()

    def _wrap_result(self, result: Any) -> ObjectRefGenerator:
        """Wrap a result in an ObjectRefGenerator.

        This handles cases where read_fn returns blocks directly rather than
        an ObjectRefGenerator.
        """
        @ray.remote(num_returns="streaming")
        def _wrap_read_fn():
            if isinstance(result, Iterator):
                for item in result:
                    yield item
            else:
                yield result

        return _wrap_read_fn.remote()

    def update_checkpoint(self, checkpoint: Dict[str, Any]) -> None:
        """Update the checkpoint state for this actor.

        Called after a successful microbatch commit to update partition offsets.

        Args:
            checkpoint: New checkpoint state (e.g., updated Kafka offsets)
        """
        self.checkpoint.update(checkpoint)
        logger.debug(f"Updated checkpoint: {checkpoint}")

    def get_checkpoint(self) -> Dict[str, Any]:
        """Get the current checkpoint state.

        Returns:
            Current checkpoint dictionary
        """
        return self.checkpoint.copy()

    def shutdown(self) -> None:
        """Shutdown the reader actor and clean up resources.

        Called when the operator is shutting down or partitions are reassigned.
        """
        if hasattr(self.datasource, "shutdown_reader_actor"):
            try:
                self.datasource.shutdown_reader_actor(self)
            except Exception as e:
                logger.warning(f"Error shutting down reader actor: {e}")

        logger.info(f"StreamingReaderActor shutdown for partitions: {self.partition_ids}")
