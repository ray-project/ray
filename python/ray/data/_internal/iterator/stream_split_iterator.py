import logging
import threading
import time
from typing import TYPE_CHECKING, Dict, Iterator, List, Optional, Set, Tuple, Union

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
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

if TYPE_CHECKING:
    import pyarrow

    from ray.data.dataset import Dataset

logger = logging.getLogger(__name__)

BLOCKED_CLIENT_WARN_TIMEOUT = 30

SPLIT_IDX = int


class StreamSplitDataIterator(DataIterator):
    """Implements a collection of iterators over a shared data stream."""

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
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                ray.get_runtime_context().get_node_id(), soft=False
            ),
        ).remote(base_dataset, n, locality_hints)

        return [
            StreamSplitDataIterator(
                base_dataset=base_dataset,
                coord_actor=coord_actor,
                output_split_idx=split_idx,
                world_size=n,
            )
            for split_idx in range(n)
        ]

    def __init__(
        self,
        base_dataset: "Dataset",
        coord_actor: ray.actor.ActorHandle,
        output_split_idx: int,
        world_size: int,
    ):
        self._base_dataset = base_dataset
        self._coord_actor = coord_actor
        self._output_split_idx = output_split_idx
        self._world_size = world_size
        self._iter_stats = DatasetStats(metadata={}, parent=None)

    def _to_ref_bundle_iterator(
        self,
    ) -> Tuple[Iterator[RefBundle], Optional[DatasetStats], bool, None]:
        def gen_blocks() -> Iterator[RefBundle]:
            # Signal that this consumer is ready for the next epoch.
            # Blocks until all consumers have signaled and the executor starts.
            ray.get(self._coord_actor.signal_epoch_done.remote(self._output_split_idx))
            # Initial get with 0 prefetched bytes.
            future: ObjectRef[Optional[RefBundle]] = self._coord_actor.get.remote(
                self._output_split_idx, 0
            )
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

        self._base_dataset._plan._run_index += 1
        # Return None for executor since StreamSplitDataIterator has its own
        # mechanism for reporting prefetched bytes via SplitCoordinator.
        return gen_blocks(), self._iter_stats, False, None

    def stats(self) -> str:
        """Implements DataIterator."""
        # Merge the locally recorded iter stats and the remotely recorded
        # stream execution stats.
        stats = ray.get(self._coord_actor.stats.remote())
        summary = stats.to_summary()
        summary.iter_stats = self._iter_stats.to_summary().iter_stats
        summary.iter_stats.streaming_split_coord_time.add(
            stats.streaming_split_coordinator_s.get()
        )
        return summary.to_string()

    def schema(self) -> Union[type, "pyarrow.lib.Schema"]:
        """Implements DataIterator."""
        return self._base_dataset.schema()

    def get_context(self) -> DataContext:
        return self._base_dataset.context

    def world_size(self) -> int:
        """Returns the number of splits total."""
        return self._world_size

    def _get_dataset_tag(self):
        return f"{self._base_dataset.get_dataset_id()}_split_{self._output_split_idx}"


@ray.remote(num_cpus=0)
class SplitCoordinator:
    """Coordinator actor for routing blocks to output splits.

    This actor runs a streaming executor locally on its main thread. Clients can
    retrieve results via actor calls running on other threads.

    Epoch lifecycle:
        - Consumers call signal_epoch_done() to indicate they are ready for the
          next epoch. This blocks (via Condition.wait) until all consumers have
          signaled, at which point the executor is started and all waiters are
          released.
        - Consumers then call get() to pull blocks. When a consumer receives all
          its data (StopIteration), get() returns None.
        - The consumer's next call to signal_epoch_done() starts the cycle again.
    """

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

        # Condition variable protects all mutable state and replaces spin-waits.
        self._cond = threading.Condition()
        self._current_executor = None

        # Guarded by self._cond.
        self._next_bundle: Dict[SPLIT_IDX, RefBundle] = {}
        self._cur_epoch = -1

        # Consumers that have called signal_epoch_done() for the current epoch
        # transition. Once all consumers signal, the next epoch starts.
        # Guarded by self._cond.
        self._ready_consumers: Set[SPLIT_IDX] = set()

        # Consumers currently inside get(). Used to detect duplicate readers
        # on the same split, which would corrupt data.
        # Guarded by self._cond.
        self._active_consumers: Set[SPLIT_IDX] = set()

        # Track prefetched bytes reported by each client (from BatchIterator).
        # Guarded by self._cond.
        self._client_prefetched_bytes: Dict[int, int] = {}

        # Add a new stats field to track coordinator overhead
        self._coordinator_overhead_s = 0.0

        self._output_iterator = None
        # Store the error raised from the `gen_epoch` call.
        self._gen_epoch_error: Optional[Exception] = None

    def _start_executor(self):
        """Start a new streaming executor for the current epoch.

        Must be called while holding self._cond (or during __init__).
        """
        self._next_bundle.clear()
        self._client_prefetched_bytes.clear()
        self._gen_epoch_error = None

        try:
            if self._current_executor is not None:
                self._current_executor.shutdown(force=True)

            plan = self._base_dataset._plan
            self._current_executor = plan.create_executor()
            # NOTE: We pass dataset.context (the original, uncopied context)
            #       rather than self._data_context (the deep copy used for
            #       process isolation) because the planner adds callbacks
            #       (e.g. checkpoint) to the original context during
            #       _get_execution_dag. Using self._data_context would cause
            #       those callbacks to be silently missed.
            # TODO: Fix this by having Planner.plan() return callbacks explicitly
            self._output_iterator = execute_to_legacy_bundle_iterator(
                self._current_executor, plan, self._base_dataset.context
            )
        except Exception as e:
            self._gen_epoch_error = e

    def signal_epoch_done(self, split_idx: SPLIT_IDX) -> None:
        """Signal that this consumer is done with the current epoch and ready
        for the next one. Blocks until all consumers have signaled.

        Uses Condition.wait() instead of spin-wait — zero CPU while waiting.
        """
        with self._cond:
            self._ready_consumers.add(split_idx)

            if len(self._ready_consumers) == self._num_splits:
                # Last consumer to arrive — start the next epoch.
                self._ready_consumers.clear()
                # NOTE: Do NOT clear _active_consumers here. Legitimate
                # consumers clean up their own entry in get()'s finally
                # block before reaching signal_epoch_done(). If a stale
                # entry remains, it means a duplicate reader is still
                # inside get() — keeping it lets us detect the conflict.
                self._cur_epoch += 1
                self._start_executor()
                self._cond.notify_all()
            else:
                # Wait for all other consumers to arrive.
                target_epoch = self._cur_epoch + 1
                start_time = time.time()
                while self._cur_epoch < target_epoch:
                    # Use a timeout so we can log a warning for blocked clients.
                    self._cond.wait(timeout=BLOCKED_CLIENT_WARN_TIMEOUT)
                    if self._cur_epoch < target_epoch:
                        elapsed = time.time() - start_time
                        if elapsed >= BLOCKED_CLIENT_WARN_TIMEOUT:
                            logger.warning(
                                f"StreamSplitDataIterator(split={split_idx}) "
                                f"blocked waiting on other clients for more "
                                f"than {elapsed:.0f}s. All clients must read "
                                "from the DataIterator splits at the same time."
                            )

        if self._gen_epoch_error is not None:
            # If there was an error when advancing to the next epoch,
            # re-raise it for all threads.
            raise self._gen_epoch_error

    def stats(self) -> DatasetStats:
        """Returns stats from the base dataset."""
        if self._current_executor:
            stats = self._current_executor.get_stats()
        else:
            stats = self._base_dataset._plan.stats()

        stats.streaming_split_coordinator_s.add(self._coordinator_overhead_s)
        return stats

    def get(
        self,
        output_split_idx: SPLIT_IDX,
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

        with self._cond:
            # Reject concurrent readers on the same split.
            if output_split_idx in self._active_consumers:
                raise ValueError(
                    f"Concurrent read on split {output_split_idx} detected. "
                    "Each split must have exactly one reader at a time."
                )
            self._active_consumers.add(output_split_idx)

        if self._gen_epoch_error is not None:
            with self._cond:
                self._active_consumers.discard(output_split_idx)
            raise self._gen_epoch_error

        returned_normally = False
        try:
            # Check for cached leftover blocks from a previous get().
            with self._cond:
                next_bundle = self._next_bundle.get(output_split_idx)

            # Fetch next bundle if needed.
            # This is a BLOCKING call (goes through get_output_blocking),
            # so do it outside the lock.
            while next_bundle is None or not next_bundle.blocks:
                next_bundle = self._output_iterator.get_next(output_split_idx)

            schema = next_bundle.schema
            block = next_bundle.blocks[-1]
            remainder = RefBundle(
                blocks=next_bundle.blocks[:-1],
                schema=next_bundle.schema,
                owns_blocks=next_bundle.owns_blocks,
                output_split_idx=next_bundle.output_split_idx,
            )

            # Accumulate any remaining blocks in next_bundle map as needed.
            with self._cond:
                self._next_bundle[output_split_idx] = remainder
                if not remainder.blocks:
                    del self._next_bundle[output_split_idx]
                # Update client prefetched bytes and report to resource manager.
                self._client_prefetched_bytes[
                    output_split_idx
                ] = client_prefetched_bytes
                self._report_prefetched_bytes_to_executor()

            returned_normally = True
            return RefBundle([block], schema=schema, owns_blocks=remainder.owns_blocks)

        except StopIteration:
            return None

        finally:
            with self._cond:
                self._active_consumers.discard(output_split_idx)
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
            self._current_executor.set_external_consumer_bytes(total_bytes)

    def get_client_prefetched_bytes(self) -> Dict[int, int]:
        """Get prefetched bytes for each client (for testing)."""
        with self._cond:
            return dict(self._client_prefetched_bytes)

    def shutdown_executor(self):
        """Shuts down the internal data executor."""
        with self._cond:
            # Call shutdown on the executor
            if self._current_executor is not None:
                self._current_executor.shutdown(force=False)
