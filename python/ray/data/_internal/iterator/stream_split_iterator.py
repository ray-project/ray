import logging
import threading
import time
from typing import TYPE_CHECKING, Dict, Iterator, List, Optional, Set, Tuple

import ray
from ray.data._internal.execution.interfaces import (
    NodeIdStr,
    RefBundle,
)
from ray.data._internal.execution.legacy_compat import execute_to_legacy_bundle_iterator
from ray.data._internal.stats import DatasetStats
from ray.data.context import DataContext
from ray.data.iterator import DataIterator
from ray.types import ObjectRef
from ray.util.debug import log_once

if TYPE_CHECKING:
    from ray.data.dataset import Dataset, Schema

logger = logging.getLogger(__name__)

BLOCKED_CLIENT_WARN_TIMEOUT = 30

SplitIdx = int


class StreamSplitDataIterator(DataIterator):
    """Implements a collection of iterators over a shared data stream."""

    YIELD_LOG_INTERVAL_S = 10

    @staticmethod
    def create(
        base_dataset: "Dataset",
        n: int,
        locality_hints: Optional[List[NodeIdStr]],
    ) -> List["StreamSplitDataIterator"]:
        """Create a split iterator from the given base Dataset and options.

        See also: `Dataset.streaming_split`.
        """
        # To avoid deadlock, the concurrency on this actor must be set to at least `n`.
        # We add 1 to the concurrency to allow for a shutdown_executor thread to run.
        coord_actor = SplitCoordinator.options(
            max_concurrency=n + 1,
            label_selector={
                ray._raylet.RAY_NODE_ID_KEY: ray.get_runtime_context().get_node_id()
            },
        ).remote(base_dataset, n, locality_hints)

        return [
            StreamSplitDataIterator(
                coord_actor=coord_actor,
                output_split_idx=split_idx,
                world_size=n,
            )
            for split_idx in range(n)
        ]

    def __init__(
        self,
        coord_actor: ray.actor.ActorHandle,
        output_split_idx: int,
        world_size: int,
    ):
        self._coord_actor = coord_actor
        self._output_split_idx = output_split_idx
        self._world_size = world_size
        self._iter_stats = DatasetStats(metadata={}, parent=None)
        logger.debug(
            f"StreamSplitDataIterator created: split={output_split_idx}, {world_size=}"
        )

    def _to_ref_bundle_iterator(
        self,
    ) -> Tuple[Iterator[RefBundle], Optional[DatasetStats], bool, None]:
        def gen_blocks() -> Iterator[RefBundle]:
            logger.debug(f"Split {self._output_split_idx}: requesting new epoch.")
            # Signal that this consumer is ready for the next epoch.
            # Blocks until all consumers have signaled and the executor starts.
            ray.get(self._coord_actor.signal_new_epoch.remote(self._output_split_idx))
            logger.debug(f"Split {self._output_split_idx} started")
            # Initial get with 0 prefetched bytes.
            future: ObjectRef[Optional[RefBundle]] = self._coord_actor.get.remote(
                self._output_split_idx, 0
            )
            last_log_time = 0.0
            while True:
                block_ref_and_md: Optional[RefBundle] = ray.get(future)
                if not block_ref_and_md:
                    break
                else:
                    # Calculate prefetched bytes: BatchIterator's current prefetch
                    # plus the block we just received (which will be added to
                    # BatchIterator's sliding window when we yield it).
                    prefetched_bytes = (
                        self._iter_stats.iter_prefetched_bytes
                        + block_ref_and_md.size_bytes()
                    )
                    future = self._coord_actor.get.remote(
                        self._output_split_idx,
                        prefetched_bytes,
                    )
                    yield RefBundle(
                        blocks=block_ref_and_md.blocks,
                        owns_blocks=False,
                        schema=block_ref_and_md.schema,
                    )

                    # Log dispatch progress.
                    now = time.time()
                    if now - last_log_time >= self.YIELD_LOG_INTERVAL_S:
                        last_log_time = now
                        logger.debug(
                            f"Split {self._output_split_idx}: "
                            f"consumer resumed after yield"
                        )

            logger.debug(f"Split {self._output_split_idx} exhausted")

        # Return None for executor since StreamSplitDataIterator has its own
        # mechanism for reporting prefetched bytes via SplitCoordinator.
        return gen_blocks(), self._iter_stats, False, None

    def stats(self) -> str:
        """Implements DataIterator."""
        # Merge the locally recorded iter stats and the remotely recorded
        # stream execution stats.
        logger.debug(f"Split {self._output_split_idx}: fetching stats remote")
        stats = ray.get(self._coord_actor.stats.remote())
        summary = stats.to_summary()
        summary.iter_stats = self._iter_stats.to_summary().iter_stats
        summary.iter_stats.streaming_split_coord_time.add(
            stats.streaming_split_coordinator_s.get()
        )
        return summary.to_string()

    def schema(self) -> Optional["Schema"]:
        """Implements DataIterator."""
        return ray.get(self._coord_actor.get_dataset_schema.remote())

    def get_context(self) -> DataContext:
        return ray.get(self._coord_actor.get_dataset_context.remote())

    def world_size(self) -> int:
        """Returns the number of splits total."""
        return self._world_size

    def _get_dataset_tag(self):
        return ray.get(self._coord_actor.get_dataset_tag.remote(self._output_split_idx))


@ray.remote(num_cpus=0)
class SplitCoordinator:
    """Coordinator actor for routing blocks to output splits.

    This actor runs a streaming executor locally on its main thread. Clients can
    retrieve results via actor calls running on other threads.

    Epoch lifecycle (each split consumer; ``StreamSplitDataIterator`` issues these
    RPCs)::

        signal_new_epoch(split_idx)
                 |
                 |  wait under Condition until all ``num_splits`` consumers
                 |  have signaled; then start executor + output iterator
                 v
        +------------------------+
        | get(split_idx, bytes)  |----+
        |   -> RefBundle         |    | repeat until stream exhausted
        +------------------------+    |
                 |                    |
                 v                    |
        +------------------------+    |
        | get(...) -> None       |<---+
        +------------------------+
                 |
                 +---- next pass: signal_new_epoch again
    """

    DISPATCH_LOG_INTERVAL_S = 10

    def __init__(
        self,
        dataset: "Dataset",
        num_splits: int,
        locality_hints: Optional[List[NodeIdStr]],
    ):
        # Set current DataContext.
        # This needs to be a deep copy so that updates to the base dataset's
        # context does not affect this process's global DataContext.
        self._data_context = dataset.context.copy()
        ray.data.DataContext._set_current(self._data_context)

        self._base_dataset = dataset
        self._num_splits = num_splits
        self._locality_hints = locality_hints

        # Condition variable protects epoch synchronization state.
        self._cond = threading.Condition()
        self._dataset_state_lock = threading.Lock()
        self._schema = None
        self._current_executor = None

        # Consumers that have called signal_new_epoch() for the current epoch
        # transition. Once all unique split indices arrive, the next epoch starts.
        # Guarded by self._cond.
        self._ready_consumers: Set[SplitIdx] = set()

        # Incremented at the start of each epoch; used for per-epoch logging.
        self._cur_epoch = -1

        # Guarded by self._cond.
        self._next_bundle: Dict[SplitIdx, RefBundle] = {}

        # Track prefetched bytes reported by each client (from BatchIterator).
        # Guarded by self._cond.
        self._client_prefetched_bytes: Dict[int, int] = {}

        # Add a new stats field to track coordinator overhead
        self._coordinator_overhead_s = 0.0

        # Per-split row dispatch counters (reset each epoch in _start_executor).
        self._num_rows_dispatched: Dict[int, int] = dict.fromkeys(range(num_splits), 0)
        self._last_dispatch_log_time: float = 0.0

        self._output_iterator = None
        # Store the error raised from the `_start_executor` call.
        self._gen_epoch_error: Optional[Exception] = None

        logger.debug(f"SplitCoordinator created: {num_splits=}, {locality_hints=}")

    def get_dataset_context(self) -> DataContext:
        return self._data_context

    def get_dataset_tag(self, output_split_idx: SplitIdx) -> str:
        return f"{self._base_dataset.get_dataset_id()}_split_{output_split_idx}"

    def get_dataset_schema(self):
        with self._dataset_state_lock:
            if self._schema is not None:
                return self._schema
            if self._current_executor is not None and self._current_executor.is_alive():
                raise RuntimeError(
                    "Cannot call schema() during active dataset execution. "
                    "Call schema() before or after iterating over the dataset, or call "
                    "schema() directly on the source Dataset object."
                )
            self._schema = self._base_dataset.schema()
            return self._schema

    def _start_executor(self):
        """Start a new streaming executor for the current epoch.

        Must be called while holding self._cond.
        """
        self._next_bundle.clear()
        self._client_prefetched_bytes.clear()
        self._num_rows_dispatched = dict.fromkeys(range(self._num_splits), 0)
        self._gen_epoch_error = None

        try:
            if self._current_executor is not None:
                self._current_executor.shutdown(force=True)

            plan = self._base_dataset._plan
            # Re-execute dataset
            self._current_executor = plan.create_executor()
            self._output_iterator = execute_to_legacy_bundle_iterator(
                self._current_executor, plan
            )
            logger.debug(
                f"Starting epoch {self._cur_epoch} (all {self._num_splits} clients "
                "synced)."
            )
        except Exception as e:
            logger.warning(f"Error creating executor for epoch {self._cur_epoch}: {e}")
            self._gen_epoch_error = e

    def signal_new_epoch(self, split_idx: SplitIdx) -> None:
        """Signal that this consumer is done with the current epoch and ready
        for the next one. Blocks until all consumers have signaled.

        Uses Condition.wait() instead of spin-wait — zero CPU while waiting.
        """
        with self._cond:
            self._ready_consumers.add(split_idx)

            if len(self._ready_consumers) == self._num_splits:
                # Last consumer to arrive — start the next epoch.
                self._ready_consumers.clear()
                self._cur_epoch += 1
                self._start_executor()
                self._cond.notify_all()
            else:
                # Wait for all other consumers to arrive.
                # Use a timeout so we can log a warning for blocked clients.
                target_epoch = self._cur_epoch + 1
                while not self._cond.wait_for(
                    lambda: self._cur_epoch >= target_epoch,
                    timeout=BLOCKED_CLIENT_WARN_TIMEOUT,
                ):
                    if log_once(f"stream_split_blocked_{split_idx}_{target_epoch}"):
                        ready = sorted(self._ready_consumers)
                        pending = sorted(
                            set(range(self._num_splits)) - self._ready_consumers
                        )
                        logger.warning(
                            f"StreamSplitDataIterator(split={split_idx}) "
                            f"blocked waiting on other clients for at least "
                            f"{BLOCKED_CLIENT_WARN_TIMEOUT}s. "
                            f"Ready splits: {ready}; waiting on splits: "
                            f"{pending}. All clients must read from the "
                            "DataIterator splits at the same time."
                        )

        if self._gen_epoch_error is not None:
            # If there was an error when advancing to the next epoch,
            # re-raise it for all threads.
            raise self._gen_epoch_error

    def stats(self) -> DatasetStats:
        """Returns stats from the base dataset."""
        executor = self._current_executor
        coordinator_overhead_s = self._coordinator_overhead_s
        if executor:
            stats = executor.get_stats()
        else:
            stats = self._base_dataset._raw_stats()

        stats.streaming_split_coordinator_s.add(coordinator_overhead_s)
        return stats

    def get(
        self,
        output_split_idx: SplitIdx,
        client_prefetched_bytes: int = 0,
    ) -> Optional[RefBundle]:
        """Blocking get operation.

        This is intended to be called concurrently from multiple clients.

        Args:
            output_split_idx: The output split index for this client.
            client_prefetched_bytes: The prefetched bytes reported by the
                client's BatchIterator, used for resource accounting.

        Returns:
            The next RefBundle for this split, or None if the epoch is done.
        """
        start_time = time.perf_counter()
        returned_normally = False
        try:
            if self._gen_epoch_error is not None:
                raise self._gen_epoch_error
            # Check for cached leftover blocks from a previous get().
            with self._cond:
                next_bundle = self._next_bundle.get(output_split_idx)

            # Fetch next bundle if needed.
            # This is a BLOCKING call (goes through get_output_blocking),
            # so do it outside the lock.
            while next_bundle is None or not next_bundle.blocks:
                next_bundle = self._output_iterator.get_next(output_split_idx)

            schema = next_bundle.schema
            block, metadata = next_bundle.blocks[-1]
            next_bundle = RefBundle(
                blocks=next_bundle.blocks[:-1],
                schema=next_bundle.schema,
                owns_blocks=next_bundle.owns_blocks,
                output_split_idx=next_bundle.output_split_idx,
            )

            # Accumulate any remaining blocks in next_bundle map as needed.
            with self._cond:
                self._next_bundle[output_split_idx] = next_bundle
                if not next_bundle.blocks:
                    del self._next_bundle[output_split_idx]

                # Update client prefetched bytes and report to resource manager.
                self._client_prefetched_bytes[
                    output_split_idx
                ] = client_prefetched_bytes
                self._report_prefetched_bytes_to_executor()

                # Track per-split row dispatch count.
                self._num_rows_dispatched[output_split_idx] += (
                    metadata.num_rows if metadata.num_rows else 0
                )
                num_rows_dispatched = self._num_rows_dispatched[output_split_idx]

            self._maybe_log_dispatch(
                split_idx=output_split_idx,
                num_rows_dispatched=num_rows_dispatched,
                client_prefetched_bytes=client_prefetched_bytes,
            )

            returned_normally = True
            return RefBundle(
                [(block, metadata)], schema=schema, owns_blocks=next_bundle.owns_blocks
            )
        except StopIteration:
            with self._cond:
                num_rows = self._num_rows_dispatched[output_split_idx]
            logger.debug(
                f"Split {output_split_idx} finished, dispatched {num_rows} rows."
            )
            return None

        except Exception as e:
            with self._cond:
                num_rows = self._num_rows_dispatched[output_split_idx]
            logger.warning(
                f"Split {output_split_idx} get() failed after {num_rows} rows: {e}"
            )
            raise
        finally:
            with self._cond:
                if not returned_normally:
                    self._client_prefetched_bytes[output_split_idx] = 0
                    self._report_prefetched_bytes_to_executor()
                self._coordinator_overhead_s += time.perf_counter() - start_time

    def _get_total_prefetched_bytes(self) -> int:
        """Get the total prefetched bytes including coordinator buffer and clients.

        Must be called while holding self._cond.
        """
        # Bytes buffered in the coordinator.
        total = sum(bundle.size_bytes() for bundle in self._next_bundle.values())
        # Bytes prefetched by each client's BatchIterator.
        total += sum(self._client_prefetched_bytes.values())
        return total

    def _report_prefetched_bytes_to_executor(self) -> None:
        """Report total prefetched bytes to the executor's resource manager.

        Must be called while holding self._cond.
        """
        if self._current_executor is not None:
            total_bytes = self._get_total_prefetched_bytes()
            try:
                self._current_executor.set_external_consumer_bytes(total_bytes)
            except AttributeError:
                # The executor may have been shut down or not fully initialized
                # (e.g., _resource_manager is only set during execute()).
                pass

    def get_client_prefetched_bytes(self) -> Dict[int, int]:
        """Get prefetched bytes for each client (for testing)."""
        with self._cond:
            return dict(self._client_prefetched_bytes)

    def _maybe_log_dispatch(
        self,
        *,
        split_idx: int,
        num_rows_dispatched: int,
        client_prefetched_bytes: int,
    ) -> None:
        """Log dispatch progress, throttled to once per interval.

        The intention for throttling is to avoid overwhelming the logs with too many
        messages.
        """
        now = time.time()
        with self._cond:
            if now - self._last_dispatch_log_time < self.DISPATCH_LOG_INTERVAL_S:
                return
            self._last_dispatch_log_time = now

        logger.debug(
            f"Split {split_idx} returned block: "
            f"{num_rows_dispatched=}, {client_prefetched_bytes=}"
        )

    def shutdown_executor(self):
        """Shuts down the internal data executor."""
        logger.debug(f"Shutting down executor (epoch={self._cur_epoch}).")
        with self._cond:
            # Call shutdown on the executor
            if self._current_executor is not None:
                self._current_executor.shutdown(force=False)
