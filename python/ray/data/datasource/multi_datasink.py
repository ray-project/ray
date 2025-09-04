import logging
from dataclasses import dataclass
from typing import Dict, Generic, Iterable, List, Optional, TypeVar

from ray.data._internal.execution.interfaces import TaskContext
from ray.data.block import Block
from ray.data.datasource.datasink import Datasink, WriteResult
from ray.util.annotations import DeveloperAPI

logger = logging.getLogger(__name__)


SinkReturnType = TypeVar("SinkReturnType")


@dataclass
@DeveloperAPI
class SinkWriteResult(Generic[SinkReturnType]):
    """Result of writing to a specific sink."""

    # Name of the sink
    sink_name: str
    # Success status
    success: bool
    # Error if any
    error: Optional[Exception]
    # Original write result if successful
    write_result: Optional[WriteResult[SinkReturnType]]


@DeveloperAPI
class MultiDatasink(Datasink[Dict[str, List[SinkWriteResult]]]):
    """A datasink that writes to multiple output sinks simultaneously.

    This datasink allows you to write the same data to multiple destinations
    in a single operation. Each sink operates independently, and failures in one
    sink do not prevent writes to other sinks.

    Examples:
        >>> import ray
        >>> from ray.data.datasource import MultiDatasink, DummyOutputDatasink
        >>> sink1 = DummyOutputDatasink()
        >>> sink2 = DummyOutputDatasink()
        >>> multi_sink = MultiDatasink({"sink1": sink1, "sink2": sink2})
        >>> ray.data.range(10).write_datasink(multi_sink)
        >>> assert sink1.num_ok == 1
        >>> assert sink2.num_ok == 1
    """

    def __init__(self, sinks: Dict[str, Datasink]):
        """Initialize a MultiDatasink.

        Args:
            sinks: A dictionary mapping sink names to Datasink instances.
                  Each provided sink will receive all data.
        """
        if not sinks:
            raise ValueError("At least one sink must be provided")

        self.sinks = sinks
        self.results = {}

    def on_write_start(self) -> None:
        """Call on_write_start for all sinks."""
        self.results = {}
        for name, sink in self.sinks.items():
            try:
                sink.on_write_start()
                self.results[name] = []
            except Exception as e:
                logger.exception(f"Failed to start write for sink '{name}': {e}")
                self.results[name] = [SinkWriteResult(name, False, e, None)]

    def write(
        self, blocks: Iterable[Block], ctx: TaskContext
    ) -> Dict[str, List[SinkWriteResult]]:
        """Write blocks to all configured sinks.

        Args:
            blocks: Generator of data blocks.
            ctx: ``TaskContext`` for the write task.

        Returns:
            Dictionary mapping sink names to lists of SinkWriteResult objects.
        """
        # We need to materialize the blocks since we're going to iterate over them multiple times
        materialized_blocks = list(blocks)

        results = {}
        for name, sink in self.sinks.items():
            # Skip sinks that failed during on_write_start
            if name in self.results and any(not r.success for r in self.results[name]):
                continue

            try:
                # Call the sink's write method
                write_return = sink.write(materialized_blocks, ctx)
                results[name] = [SinkWriteResult(name, True, None, write_return)]
            except Exception as e:
                logger.exception(f"Write failed for sink '{name}': {e}")
                results[name] = [SinkWriteResult(name, False, e, None)]

        return results

    def on_write_complete(
        self, write_result: WriteResult[Dict[str, List[SinkWriteResult]]]
    ):
        """Call on_write_complete for all sinks that were written successfully."""
        # Aggregate all write results by sink
        all_results = {}
        for result_dict in write_result.write_returns:
            for sink_name, sink_results in result_dict.items():
                if sink_name not in all_results:
                    all_results[sink_name] = []
                all_results[sink_name].extend(sink_results)

        # For each sink that had all successful writes, call on_write_complete
        for name, sink in self.sinks.items():
            # Skip if no results for this sink
            if name not in all_results:
                continue

            # Check if all writes to this sink were successful
            sink_results = all_results[name]
            if all(r.success for r in sink_results):
                try:
                    # Construct a WriteResult for this sink
                    # For simplicity, we'll set num_rows and size_bytes to the total for all tasks
                    sink_write_result = WriteResult(
                        num_rows=write_result.num_rows,
                        size_bytes=write_result.size_bytes,
                        write_returns=[
                            r.write_result for r in sink_results if r.write_result
                        ],
                    )
                    sink.on_write_complete(sink_write_result)
                except Exception as e:
                    logger.exception(f"on_write_complete failed for sink '{name}': {e}")
                    # Call on_write_failed for this sink
                    try:
                        sink.on_write_failed(e)
                    except Exception:
                        logger.exception(
                            f"on_write_failed also failed for sink '{name}'"
                        )

    def on_write_failed(self, error: Exception) -> None:
        """Call on_write_failed for all sinks."""
        for name, sink in self.sinks.items():
            try:
                sink.on_write_failed(error)
            except Exception as e:
                logger.exception(f"on_write_failed failed for sink '{name}': {e}")

    def get_name(self) -> str:
        """Return a name representing all the sinks."""
        sink_names = ", ".join(self.sinks.keys())
        return f"Multi({sink_names})"

    @property
    def supports_distributed_writes(self) -> bool:
        """Return True if all sinks support distributed writes."""
        return all(sink.supports_distributed_writes for sink in self.sinks.values())

    @property
    def min_rows_per_write(self) -> Optional[int]:
        """Return the maximum min_rows_per_write across all sinks."""
        values = [
            sink.min_rows_per_write
            for sink in self.sinks.values()
            if sink.min_rows_per_write is not None
        ]
        return max(values) if values else None
