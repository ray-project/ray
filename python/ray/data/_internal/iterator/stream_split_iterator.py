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

        return [StreamSplitDataIterator(coord_actor, i, n) for i in range(n)]

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
        # Epoch this split is currently consuming, set by ``gen_blocks`` once
        # ``start_epoch`` returns and read by ``_on_iteration_end`` on the
        # consumer thread. The iterator-side ``finally`` runs on the consumer
        # thread, while ``gen_blocks`` runs in the async-prefetch filling
        # worker thread, so the read/write happen on different threads — but
        # they're ordered: ``start_epoch`` must return before any item is
        # yielded, which must happen before the consumer can ``break``. Plain
        # attribute access (no lock) so this iterator stays picklable, since
        # users pass split iterators to ``@ray.remote`` tasks.
        self._active_epoch: Optional[int] = None
        logger.debug(
            f"StreamSplitDataIterator created: split={output_split_idx}, {world_size=}"
        )

    def _to_ref_bundle_iterator(
        self,
    ) -> Tuple[Iterator[RefBundle], Optional[DatasetStats], bool, None]:
        def gen_blocks() -> Iterator[RefBundle]:
            logger.debug(f"Split {self._output_split_idx}: requesting new epoch.")
            cur_epoch = ray.get(
                self._coord_actor.start_epoch.remote(self._output_split_idx)
            )
            logger.debug(f"Split {self._output_split_idx}: epoch {cur_epoch} started")

            self._active_epoch = cur_epoch

            try:
                # Initial get with 0 prefetched bytes.
                future: ObjectRef[Optional[RefBundle]] = self._coord_actor.get.remote(
                    cur_epoch, self._output_split_idx, 0
                )
                last_log_time = 0.0
                while True:
                    block_ref_and_md: Optional[RefBundle] = ray.get(future)
                    if not block_ref_and_md:
                        break
                    else:
                        # Calculate prefetched bytes: BatchIterator's current
                        # prefetch plus the block we just received (which will
                        # be added to BatchIterator's sliding window when we
                        # yield it).
                        prefetched_bytes = (
                            self._iter_stats.iter_prefetched_bytes
                            + block_ref_and_md.size_bytes()
                        )
                        future = self._coord_actor.get.remote(
                            cur_epoch,
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
                                f"Split {self._output_split_idx} epoch "
                                f"{cur_epoch}: consumer resumed after yield"
                            )

                logger.debug(
                    f"Split {self._output_split_idx}: epoch {cur_epoch} exhausted"
                )
            finally:
                # Tell the coordinator this split has stopped consuming for
                # ``cur_epoch`` (normal exhaustion, an exception, or — once
                # ``gen_blocks`` is eventually closed — early ``break``).
                # Once every split has disengaged, the coordinator shuts the
                # executor down. Idempotent on the coordinator side, so it's
                # safe to fire this in addition to ``_on_iteration_end``.
                # Fire-and-forget: don't block the producer thread.
                self._coord_actor.client_disengaged.remote(
                    cur_epoch, self._output_split_idx
                )
                if self._active_epoch == cur_epoch:
                    self._active_epoch = None

        # Return None for executor since StreamSplitDataIterator has its own
        # mechanism for reporting prefetched bytes via SplitCoordinator.
        return gen_blocks(), self._iter_stats, False, None

    def _on_iteration_end(self) -> None:
        """Fire ``client_disengaged`` from the consumer's thread.

        ``gen_blocks`` runs in the async-prefetch filling worker thread, so
        on early ``break`` its ``finally`` only fires when GC reclaims the
        generator — which can be arbitrarily delayed (especially under CI
        load). The consumer-side ``_iter_batches`` ``finally`` runs
        synchronously on the consumer's thread, so calling this hook there
        gives us a deterministic shutdown path. ``client_disengaged`` is
        idempotent per ``(epoch, split_idx)``, so it's safe to fire here
        even when ``gen_blocks`` will eventually fire it too.
        """
        epoch = self._active_epoch
        if epoch is None:
            # Iteration never started, or already cleaned up.
            return
        self._coord_actor.client_disengaged.remote(epoch, self._output_split_idx)

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
    """

    DISPATCH_LOG_INTERVAL_S = 10

    def __init__(
        self,
        dataset: "Dataset",
        n: int,
        locality_hints: Optional[List[NodeIdStr]],
    ):

        # Set current DataContext.
        # This needs to be a deep copy so that updates to the base dataset's
        # context does not affect this process's global DataContext.
        self._data_context = dataset.context.copy()
        ray.data.DataContext._set_current(self._data_context)

        self._base_dataset = dataset
        self._n = n
        self._locality_hints = locality_hints
        self._lock = threading.RLock()
        self._dataset_state_lock = threading.Lock()
        self._schema = None
        self._current_executor = None

        # Guarded by self._lock.
        self._next_bundle: Dict[int, RefBundle] = {}
        self._unfinished_clients_in_epoch = n
        # Splits that have already disengaged from the current epoch.
        # Cleared whenever a new epoch starts. Once every split has
        # disengaged, the executor is shut down so it stops producing blocks
        # into the object store. Tracked as a set so ``client_disengaged``
        # is idempotent per ``(epoch, split_idx)`` — both the iterator-side
        # ``_on_iteration_end`` and ``gen_blocks``'s own ``finally`` can fire
        # it without double-counting. Guarded by self._lock.
        self._disengaged_splits_in_epoch: Set[int] = set()
        self._cur_epoch = -1

        # Track prefetched bytes reported by each client (from BatchIterator).
        # Guarded by self._lock.
        self._client_prefetched_bytes: Dict[int, int] = {}

        # Add a new stats field to track coordinator overhead
        self._coordinator_overhead_s = 0.0

        # Per-split row dispatch counters (reset each epoch in _barrier).
        self._num_rows_dispatched: Dict[int, int] = dict.fromkeys(range(n), 0)
        self._last_dispatch_log_time: float = 0.0

        self._output_iterator = None
        # Store the error raised from the `gen_epoch` call.
        self._gen_epoch_error: Optional[Exception] = None

        logger.debug(f"SplitCoordinator created: {n=}, {locality_hints=}")

    def get_dataset_context(self) -> DataContext:
        return self._data_context

    def get_dataset_tag(self, output_split_idx: int) -> str:
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

    def stats(self) -> DatasetStats:
        """Returns stats from the base dataset."""
        if self._current_executor:
            stats = self._current_executor.get_stats()
        else:
            stats = self._base_dataset._raw_stats()

        # Set the tracked overhead time
        stats.streaming_split_coordinator_s.add(self._coordinator_overhead_s)

        return stats

    def start_epoch(self, split_idx: int) -> str:
        """Called to start an epoch.

        Returns:
            UUID for the epoch, which must be used when accessing results via get().
        """

        # Wait for all clients to arrive at the barrier before starting a new epoch.
        epoch_id = self._barrier(split_idx)
        return epoch_id

    def _try_start_new_epoch(self, starting_epoch: int):
        with self._lock:
            # This check gates that we start epoch only once
            if self._cur_epoch == starting_epoch:
                # Reset state
                self._reset_state()
                # Ratchet epoch
                self._cur_epoch += 1

                try:
                    # Force executor shutdown if present
                    if self._current_executor is not None:
                        self._current_executor.shutdown(force=True)

                    plan = self._base_dataset._plan
                    # Re-execute dataset
                    self._current_executor = plan.create_executor()
                    self._output_iterator = execute_to_legacy_bundle_iterator(
                        self._current_executor, plan
                    )
                    # Register the streaming split external consumers with the executor's resource manager.
                    self._current_executor.set_external_consumer_bytes(0)
                    logger.debug(
                        f"Starting epoch {self._cur_epoch} (all {self._n} clients "
                        "synced)."
                    )

                except Exception as e:
                    logger.warning(
                        f"Error creating executor for epoch {self._cur_epoch}: {e}"
                    )
                    self._gen_epoch_error = e

        if self._gen_epoch_error is not None:
            # If there was an error when advancing to the next epoch,
            # re-raise it for all threads.
            raise self._gen_epoch_error

    def _reset_state(self):
        self._unfinished_clients_in_epoch = self._n
        self._disengaged_splits_in_epoch.clear()
        self._next_bundle.clear()
        self._gen_epoch_error = None
        # Reset per-split dispatch counters for the new epoch.
        self._num_rows_dispatched = dict.fromkeys(range(self._n), 0)

    def get(
        self,
        epoch_id: int,
        output_split_idx: int,
        client_prefetched_bytes: int = 0,
    ) -> Optional[RefBundle]:
        """Blocking get operation.

        This is intended to be called concurrently from multiple clients.

        Args:
            epoch_id: The epoch ID from start_epoch().
            output_split_idx: The output split index for this client.
            client_prefetched_bytes: The prefetched bytes reported by the
                client's BatchIterator, used for resource accounting.

        Returns:
            The next RefBundle for this split, or None if the epoch is done.
        """
        start_time = time.perf_counter()
        if epoch_id != self._cur_epoch:
            raise ValueError(
                "Invalid iterator: the dataset has moved on to another epoch."
            )

        returned_normally = False
        try:
            # Ensure there is at least one bundle.
            with self._lock:
                if output_split_idx in self._next_bundle:
                    next_bundle = self._next_bundle[output_split_idx]
                else:
                    next_bundle = None

            # Fetch next bundle if needed.
            while next_bundle is None or not next_bundle.blocks:
                # This is a BLOCKING call, so do it outside the lock.
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
            with self._lock:
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
                epoch_id=epoch_id,
                num_rows_dispatched=num_rows_dispatched,
                client_prefetched_bytes=client_prefetched_bytes,
            )

            returned_normally = True
            return RefBundle(
                [(block, metadata)], schema=schema, owns_blocks=next_bundle.owns_blocks
            )
        except StopIteration:
            with self._lock:
                num_rows = self._num_rows_dispatched[output_split_idx]
            logger.debug(
                f"Split {output_split_idx} epoch {epoch_id} finished, dispatched "
                f"{num_rows} rows."
            )
            return None
        except Exception as e:
            with self._lock:
                num_rows = self._num_rows_dispatched[output_split_idx]
            logger.warning(
                f"Split {output_split_idx} epoch {epoch_id} get() failed after "
                f"{num_rows} rows: {e}"
            )
            raise
        finally:
            # Clear prefetched bytes on any exit (StopIteration or other
            # exceptions) to avoid stale backpressure data.
            if not returned_normally:
                with self._lock:
                    self._client_prefetched_bytes[output_split_idx] = 0
                    self._report_prefetched_bytes_to_executor()
            # Track overhead time in the instance variable
            self._coordinator_overhead_s += time.perf_counter() - start_time

    def _get_total_prefetched_bytes(self) -> int:
        """Get the total prefetched bytes including coordinator buffer and clients.

        Must be called while holding self._lock.
        """
        # Bytes buffered in the coordinator.
        total = sum(bundle.size_bytes() for bundle in self._next_bundle.values())
        # Bytes prefetched by each client's BatchIterator.
        total += sum(self._client_prefetched_bytes.values())
        return total

    def _report_prefetched_bytes_to_executor(self) -> None:
        """Report total prefetched bytes to the executor's resource manager.

        Must be called while holding self._lock.
        """
        if self._current_executor is not None:
            total_bytes = self._get_total_prefetched_bytes()
            self._current_executor.set_external_consumer_bytes(total_bytes)

    def get_client_prefetched_bytes(self) -> Dict[int, int]:
        """Get prefetched bytes for each client (for testing)."""
        with self._lock:
            return dict(self._client_prefetched_bytes)

    def _is_executor_shutdown(self) -> bool:
        """Whether the current executor (if any) has been shut down.

        For testing only.
        """
        with self._lock:
            executor = self._current_executor
        return executor is not None and executor._shutdown

    def _maybe_log_dispatch(
        self,
        *,
        split_idx: int,
        epoch_id: int,
        num_rows_dispatched: int,
        client_prefetched_bytes: int,
    ) -> None:
        """Log dispatch progress, throttled to once per interval.

        The intention for throttling is to avoid overwhelming the logs with too many
        messages.
        """
        now = time.time()
        with self._lock:
            if now - self._last_dispatch_log_time < self.DISPATCH_LOG_INTERVAL_S:
                return
            self._last_dispatch_log_time = now

        logger.debug(
            f"Split {split_idx} epoch {epoch_id} returned block: "
            f"{num_rows_dispatched=}, {client_prefetched_bytes=}"
        )

    def shutdown_executor(self):
        """Shuts down the internal data executor."""
        logger.debug(f"Shutting down executor (epoch={self._cur_epoch}).")
        with self._lock:
            # Call shutdown on the executor
            if self._current_executor is not None:
                self._current_executor.shutdown(force=False)

    def client_disengaged(self, epoch_id: int, split_idx: int) -> None:
        """Called by a split iterator when it stops consuming for ``epoch_id``.

        Triggered from the iterator's ``finally`` block on normal exhaustion,
        early ``break``, or an exception in the consumer. Clears this split's
        prefetch state so resource accounting is accurate for the remaining
        consumers, and shuts down the executor once every split has
        disengaged from the current epoch. Idempotent per
        ``(epoch_id, split_idx)``.
        """
        executor_to_shutdown = None
        with self._lock:
            # Stale notification from a prior epoch: the executor that was
            # running when this iterator started has already been replaced
            # by ``_try_start_new_epoch``. Nothing to clean up here.
            if epoch_id != self._cur_epoch:
                return
            if split_idx in self._disengaged_splits_in_epoch:
                # Already processed for this epoch — both the iterator-side
                # ``_on_iteration_end`` and ``gen_blocks``'s own ``finally``
                # may fire this; skip the duplicate.
                return
            self._disengaged_splits_in_epoch.add(split_idx)

            self._client_prefetched_bytes[split_idx] = 0
            # Drop any blocks buffered for this split — the consumer won't
            # read them and they'd otherwise pin memory until the next epoch.
            self._next_bundle.pop(split_idx, None)
            self._report_prefetched_bytes_to_executor()

            if (
                len(self._disengaged_splits_in_epoch) >= self._n
                and self._current_executor is not None
            ):
                executor_to_shutdown = self._current_executor

        # Shut down outside the lock: ``StreamingExecutor.shutdown`` joins
        # the scheduling thread (up to 2s) and we don't want to block other
        # coordinator calls in the meantime. ``shutdown`` is idempotent.
        if executor_to_shutdown is not None:
            logger.debug(
                f"All splits disengaged from epoch {epoch_id}; shutting "
                "down executor."
            )
            executor_to_shutdown.shutdown(force=False)

    def _barrier(self, split_idx: int) -> int:
        """Arrive and block until the start of the given epoch."""
        # Decrement and await all clients to arrive here.
        with self._lock:
            logger.debug(
                f"Split {split_idx} arriving at barrier for epoch "
                f"{self._cur_epoch + 1}."
            )
            starting_epoch = self._cur_epoch
            self._unfinished_clients_in_epoch -= 1

        start_time = time.time()
        while (
            self._cur_epoch == starting_epoch and self._unfinished_clients_in_epoch != 0
        ):
            if time.time() - start_time > BLOCKED_CLIENT_WARN_TIMEOUT:
                if log_once(f"stream_split_blocked_{split_idx}_{starting_epoch}"):
                    logger.warning(
                        f"StreamSplitDataIterator(epoch={starting_epoch}, "
                        f"split={split_idx}) blocked waiting on other clients "
                        f"for more than {BLOCKED_CLIENT_WARN_TIMEOUT}s. All "
                        "clients must read from the DataIterator splits at "
                        "the same time. This warning will not be printed again "
                        "for this epoch."
                    )
            time.sleep(0.1)

        # Advance to the next epoch
        self._try_start_new_epoch(starting_epoch)

        if self._output_iterator is None:
            raise ValueError(
                "Invalid iterator: output iterator is not initialized. "
                "This may indicate too many concurrent consumers."
            )
        if self._cur_epoch != starting_epoch + 1:
            raise ValueError(
                f"Invalid iterator: too many concurrent consumers detected. "
                f"Expected epoch {starting_epoch + 1}, got {self._cur_epoch}."
            )

        return self._cur_epoch
