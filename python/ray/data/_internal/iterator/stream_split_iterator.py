import collections
import dataclasses
import logging
import threading
import time
from typing import TYPE_CHECKING, Any, Deque, Dict, Iterator, List, Optional, Tuple

import ray
from ray.data._internal.execution.interfaces import (
    NodeIdStr,
    RefBundle,
)
from ray.data._internal.execution.legacy_compat import execute_to_legacy_bundle_iterator
from ray.data._internal.stats import DatasetStats
from ray.data.block import BlockAccessor
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
        enable_2pc_batch_consumption_tracking: bool = False,
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
        ).remote(
            base_dataset,
            n,
            locality_hints,
            enable_2pc_batch_consumption_tracking,
        )

        return [
            StreamSplitDataIterator(
                coord_actor, i, n, enable_2pc_batch_consumption_tracking
            )
            for i in range(n)
        ]

    def __init__(
        self,
        coord_actor: ray.actor.ActorHandle,
        output_split_idx: int,
        world_size: int,
        enable_2pc_batch_consumption_tracking: bool = False,
    ):
        self._coord_actor = coord_actor
        self._output_split_idx = output_split_idx
        self._world_size = world_size
        self._iter_stats = DatasetStats(metadata={}, parent=None)
        # One-shot flag: when True, the next iter_batches() call rejoins the
        # in-progress epoch via rejoin_current_epoch() instead of arriving at
        # the start-of-epoch barrier. Used when this iterator is handed to a
        # Ray Train replacement worker whose predecessor died mid-epoch (so
        # the predecessor already counted toward the current epoch's barrier).
        # Cleared after the first iter_batches() call so subsequent epochs
        # follow the normal barrier path.
        self._is_replacement = False
        # 2PC mode: when True, gen_blocks tells the coord to reserve each
        # served block under our split_idx, and the BatchIterator fires a
        # synchronous coord.ack right before each yield via _ack_callback.
        # When False, get/ack/abort are no-ops at the coord side and the
        # BatchIterator runs in legacy (untracked) mode.
        self._enable_2pc_batch_consumption_tracking = (
            enable_2pc_batch_consumption_tracking
        )
        # Cumulative per-block (hex -> rows-acked-so-far). Updated by
        # _ack_callback as batches flow out; sent to coord on each ack.
        self._pending_offsets: Dict[str, int] = {}
        # Per-block num_rows cache so _ack_callback can prune fully-acked
        # entries from _pending_offsets without an extra coord round-trip.
        self._block_num_rows: Dict[str, int] = {}
        logger.debug(
            f"StreamSplitDataIterator created: split={output_split_idx}, "
            f"{world_size=}, 2pc={enable_2pc_batch_consumption_tracking}"
        )

    def _to_ref_bundle_iterator(
        self,
    ) -> Tuple[Iterator[RefBundle], Optional[DatasetStats], bool, None]:
        is_replacement = self._is_replacement
        self._is_replacement = False

        def gen_blocks() -> Iterator[RefBundle]:
            if is_replacement:
                logger.debug(
                    f"Split {self._output_split_idx}: rejoining in-progress epoch."
                )
                cur_epoch = ray.get(
                    self._coord_actor.rejoin_current_epoch.remote(
                        self._output_split_idx
                    )
                )
            else:
                logger.debug(f"Split {self._output_split_idx}: requesting new epoch.")
                cur_epoch = ray.get(
                    self._coord_actor.start_epoch.remote(self._output_split_idx)
                )
            logger.debug(f"Split {self._output_split_idx}: epoch {cur_epoch} started")

            last_log_time = 0.0

            # Initial get with 0 prefetched bytes. (When 2PC is on, the
            # coord registers each served block under our split_idx; when
            # off, get() is the legacy immediate-commit path.)
            future: ObjectRef[Optional[RefBundle]] = self._coord_actor.get.remote(
                cur_epoch, self._output_split_idx, 0
            )
            while True:
                block_ref_and_md: Optional[RefBundle] = ray.get(future)
                if not block_ref_and_md:
                    break
                else:
                    # Cache per-block num_rows so the ack callback can prune
                    # fully-acked entries from _pending_offsets without
                    # another coord round-trip.
                    if self._enable_2pc_batch_consumption_tracking:
                        for block_ref, md in block_ref_and_md.blocks:
                            self._block_num_rows[block_ref.hex()] = md.num_rows or 0

                    # Calculate prefetched bytes: BatchIterator's current prefetch
                    # plus the block we just received (which will be added to
                    # BatchIterator's sliding window when we yield it).
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
                            f"Split {self._output_split_idx} epoch {cur_epoch}: "
                            f"consumer resumed after yield"
                        )

            logger.debug(f"Split {self._output_split_idx}: epoch {cur_epoch} exhausted")

        # Return None for executor since StreamSplitDataIterator has its own
        # mechanism for reporting prefetched bytes via SplitCoordinator.
        return gen_blocks(), self._iter_stats, False, None

    def _create_batch_iterator(
        self,
        ref_bundles_iter: Iterator[RefBundle],
        prefetch_bytes_callback=None,
        **kwargs,
    ):
        """Construct a BatchIterator with 2PC instrumentation when enabled.

        When ``enable_2pc_batch_consumption_tracking`` is on, the iterator
        runs in provenance mode (each emitted Batch carries source-block
        provenance) and registers ``_ack_callback`` as ``on_yield`` so each
        batch is synchronously committed at the coord before user code sees
        it. When off, this matches the base ``DataIterator``-level
        construction.
        """
        # Local import to avoid an import cycle at module load.
        from ray.data._internal.block_batching.iter_batches import BatchIterator

        return BatchIterator(
            ref_bundles_iter,
            prefetch_bytes_callback=prefetch_bytes_callback,
            block_iter_with_ids=self._enable_2pc_batch_consumption_tracking,
            on_yield=(
                self._ack_callback
                if self._enable_2pc_batch_consumption_tracking
                else None
            ),
            **kwargs,
        )

    def _ack_callback(self, batch) -> None:
        """Synchronously commit a batch's rows to the SplitCoordinator before
        the batch is yielded to user code. (At-most-once delivery.)

        Reads the per-batch ``provenance``, accumulates per-block cumulative
        offsets into ``self._pending_offsets``, fires ``coord.ack`` blocking
        on confirmation, and prunes fully-acked entries from the local
        cache to keep ack payloads small.
        """
        provenance = getattr(batch, "provenance", None) or []
        for block_id, rows_taken in provenance:
            self._pending_offsets[block_id] = (
                self._pending_offsets.get(block_id, 0) + rows_taken
            )
        if self._pending_offsets:
            ray.get(
                self._coord_actor.ack.remote(
                    self._output_split_idx,
                    list(self._pending_offsets.items()),
                )
            )
        for bid in list(self._pending_offsets.keys()):
            if self._pending_offsets[bid] >= self._block_num_rows.get(bid, 0):
                del self._pending_offsets[bid]
                self._block_num_rows.pop(bid, None)

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
        enable_2pc_batch_consumption_tracking: bool = False,
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

        # ---- 2PC reservation/abort state (guarded by self._lock) ----
        # When enabled, every served block is added to _reserved[split_idx]
        # as a [block_ref_hex, offset, RefBundle] entry. Acks update the
        # offset in place; fully-acked entries are popped from the front.
        # On abort(split_idx), the unconsumed tail of each reservation is
        # sliced and pushed onto _requeue[split_idx]; subsequent get() calls
        # for that split drain _requeue first. When disabled, all three
        # methods (get's reservation step, ack, abort) are no-ops and the
        # coord behaves as it did before this change.
        self._enable_2pc_batch_consumption_tracking = (
            enable_2pc_batch_consumption_tracking
        )
        self._reserved: Dict[int, Deque[List[Any]]] = {}
        self._requeue: Dict[int, Deque[RefBundle]] = {
            i: collections.deque() for i in range(n)
        }

        logger.debug(
            f"SplitCoordinator created: {n=}, {locality_hints=}, "
            f"2pc={enable_2pc_batch_consumption_tracking}"
        )

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

    def rejoin_current_epoch(self, split_idx: int) -> int:
        """Returns the in-progress epoch id without arriving at the start barrier.

        Used by a Ray Train replacement worker (e.g., torchft replica
        replacement) to rejoin the epoch its dead predecessor was in the
        middle of. The predecessor already counted toward this epoch's
        barrier, so the replacement must not decrement
        ``_unfinished_clients_in_epoch`` again. The replacement then drains
        whatever blocks remain in this split's per-split buffer for the
        current epoch via ``get(...)``, and proceeds to subsequent epochs
        via the normal ``start_epoch`` path.
        """
        with self._lock:
            if self._cur_epoch < 0:
                raise ValueError(
                    "rejoin_current_epoch called before any epoch has started; "
                    "no in-progress epoch to rejoin."
                )
            return self._cur_epoch

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
        self._next_bundle.clear()
        self._gen_epoch_error = None
        # Reset per-split dispatch counters for the new epoch.
        self._num_rows_dispatched = dict.fromkeys(range(self._n), 0)
        # 2PC state does not carry across epochs: any reservations or
        # requeued blocks from the just-finished epoch are stale.
        self._reserved.clear()
        for split_idx in range(self._n):
            self._requeue[split_idx].clear()

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
            The next single-block RefBundle for this split, or ``None`` if
            the epoch is done.

            When ``enable_2pc_batch_consumption_tracking`` is on, the
            returned block is also registered as a reservation under
            ``output_split_idx``; the consumer is expected to later
            ``ack(...)`` the rows it consumes from this block (or to be
            ``abort(...)``-ed by Ray Train on worker death).
        """
        start_time = time.perf_counter()
        if epoch_id != self._cur_epoch:
            raise ValueError(
                "Invalid iterator: the dataset has moved on to another epoch."
            )

        returned_normally = False
        try:
            served_bundle = self._produce_single_block_bundle(output_split_idx)
            if served_bundle is None:
                # Caller path treats StopIteration as "epoch finished" by
                # producing None; mirror that here.
                return None

            block_ref, metadata = served_bundle.blocks[0]
            with self._lock:
                self._client_prefetched_bytes[
                    output_split_idx
                ] = client_prefetched_bytes
                self._report_prefetched_bytes_to_executor()
                self._num_rows_dispatched[output_split_idx] += (
                    metadata.num_rows if metadata.num_rows else 0
                )
                num_rows_dispatched = self._num_rows_dispatched[output_split_idx]
                if self._enable_2pc_batch_consumption_tracking:
                    self._reserve_block(output_split_idx, served_bundle)

            self._maybe_log_dispatch(
                split_idx=output_split_idx,
                epoch_id=epoch_id,
                num_rows_dispatched=num_rows_dispatched,
                client_prefetched_bytes=client_prefetched_bytes,
            )

            returned_normally = True
            return served_bundle
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

    def _produce_single_block_bundle(
        self, output_split_idx: int
    ) -> Optional[RefBundle]:
        """Produce the next single-block RefBundle for this split.

        Cascades through three sources in order:
          1. ``_requeue[split_idx]`` — sliced remainders from a dead
             consumer's aborted reservations.
          2. ``_next_bundle[split_idx]`` — leftover blocks from a previous
             upstream pull.
          3. ``self._output_iterator.get_next(split_idx)`` — pull a fresh
             multi-block bundle from upstream and stash the leftover.

        May raise ``StopIteration`` if the upstream pipeline is drained for
        this split. Returns ``None`` only via that exception path; callers
        currently rely on the surrounding ``try/except StopIteration`` in
        ``get`` to translate to ``None`` for the user.
        """
        # Source 1: per-split requeue (drained one block per get, FIFO).
        with self._lock:
            if self._requeue[output_split_idx]:
                return self._requeue[output_split_idx].popleft()

        # Source 2 + 3: cached leftover bundle, pulling from upstream when
        # empty. We pop one block off the tail and stash the rest back.
        with self._lock:
            next_bundle = self._next_bundle.get(output_split_idx)

        while next_bundle is None or not next_bundle.blocks:
            # Blocking call; do it outside the lock.
            next_bundle = self._output_iterator.get_next(output_split_idx)

        block, metadata = next_bundle.blocks[-1]
        leftover = RefBundle(
            blocks=next_bundle.blocks[:-1],
            schema=next_bundle.schema,
            owns_blocks=next_bundle.owns_blocks,
            output_split_idx=next_bundle.output_split_idx,
        )

        with self._lock:
            if leftover.blocks:
                self._next_bundle[output_split_idx] = leftover
            else:
                self._next_bundle.pop(output_split_idx, None)

        return RefBundle(
            [(block, metadata)],
            schema=next_bundle.schema,
            owns_blocks=next_bundle.owns_blocks,
        )

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

    # ------------------------------------------------------------------
    # 2PC: reserve / ack / abort
    # ------------------------------------------------------------------

    def _reserve_block(self, split_idx: int, bundle: RefBundle) -> None:
        """Append a single-block RefBundle as a reservation for this split.

        Caller must hold ``self._lock``. The bundle is expected to carry
        exactly one block (which is what ``coord.get`` returns).
        """
        block_ref, _md = bundle.blocks[0]
        deque_ = self._reserved.setdefault(split_idx, collections.deque())
        deque_.append([block_ref.hex(), 0, bundle])

    def ack(self, split_idx: int, acks: List[Tuple[str, int]]) -> None:
        """Commit row offsets for blocks reserved against this split.

        Called synchronously by the consumer just before yielding each
        batch to user code. ``acks`` is a list of
        ``(block_ref_hex, cumulative_offset)`` tuples covering the source
        blocks contributing to the imminent batch. The consumer drains
        from the front of the deque in FIFO order, so we update offsets
        in-place and pop fully-acked entries from the front in one pass.
        """
        with self._lock:
            deque_ = self._reserved.get(split_idx)
            if not deque_:
                return
            new_offsets = dict(acks)
            for entry in deque_:
                if entry[0] in new_offsets:
                    entry[1] = new_offsets[entry[0]]
            while deque_:
                _hex, offset, bundle = deque_[0]
                num_rows = bundle.blocks[0][1].num_rows
                if num_rows is not None and offset >= num_rows:
                    deque_.popleft()
                else:
                    break
            if not deque_:
                del self._reserved[split_idx]

    def abort(self, split_idx: int) -> None:
        """Rewind a dead consumer's unconsumed reservations into the requeue.

        For each remaining reservation entry on this split (in original
        insertion order), slice the block at the last-acked offset and push
        the unconsumed remainder onto ``_requeue[split_idx]``. The
        replacement consumer's ``get(split_idx)`` will drain those
        remainders before any new upstream blocks, so it picks up exactly
        where the dead consumer's ack stream left off — no duplicates, no
        gaps (modulo the microsecond ack/yield window documented in the
        design).
        """
        with self._lock:
            entries = self._reserved.pop(split_idx, None)
        if not entries:
            return

        for entry in entries:
            _hex, offset, bundle = entry
            block_ref, md = bundle.blocks[0]
            num_rows = md.num_rows or 0
            if offset >= num_rows:
                # Already fully consumed; nothing to requeue. (Defensive;
                # should have been dropped on the final ack.)
                continue

            if offset == 0:
                # Nothing acked yet for this block — push the original
                # bundle as-is. No need for a ray.put round-trip.
                requeue_bundle = bundle
            else:
                # Slice off the unconsumed tail and ray.put it as a new
                # block. Arrow slice is a zero-copy view; the ray.put cost
                # is the failure-path price of at-most-once recovery.
                #
                # We carry forward exec_stats / task_exec_stats / input_files
                # from the original block — the slice doesn't have its own
                # producing task, and these fields are accounting/lineage
                # only (not load-bearing for correctness). num_rows and
                # size_bytes are recomputed from the actual sliced bytes.
                full_block = ray.get(block_ref)
                sliced_block = BlockAccessor.for_block(full_block).slice(
                    offset, num_rows, copy=False
                )
                sliced_accessor = BlockAccessor.for_block(sliced_block)
                new_md = dataclasses.replace(
                    md,
                    num_rows=sliced_accessor.num_rows(),
                    size_bytes=sliced_accessor.size_bytes(),
                )
                new_ref = ray.put(sliced_block)
                requeue_bundle = RefBundle(
                    [(new_ref, new_md)],
                    schema=bundle.schema,
                    owns_blocks=True,
                )

            with self._lock:
                self._requeue[split_idx].append(requeue_bundle)

        logger.debug(
            f"abort(split={split_idx}) requeued {len(entries)} reservation(s)."
        )

    def shutdown_executor(self):
        """Shuts down the internal data executor."""
        logger.debug(f"Shutting down executor (epoch={self._cur_epoch}).")
        with self._lock:
            # Call shutdown on the executor
            if self._current_executor is not None:
                self._current_executor.shutdown(force=False)

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
