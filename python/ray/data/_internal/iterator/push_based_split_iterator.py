"""PROTOTYPE: push-based streaming_split.

A push-based alternative to ``stream_split_iterator.py``: instead of each
train worker calling ``coordinator.get()`` per block, the coordinator pushes
blocks to the workers, which iterate from a local queue.

How it works:

- The coordinator runs the streaming executor and one pusher thread per
  split; both are recreated every epoch, like the pull model.
- Flow control is demand-driven: a consumer keeps enough one-block requests
  outstanding to cover the same ``prefetch_batches * batch_size`` row window
  the pull model prefetches, topping the window up after each block it
  consumes. The local queue stores the prefetched blocks, keeping several
  transfers in flight. No polling — a dead or idle consumer just stops
  requesting.
- Blocks are pushed by value, so consumers hold no ObjectRefs and the
  executor frees each block as soon as it is delivered.
- Deliveries are sequence-numbered and reordered on arrival (Ray may run a
  multi-threaded actor's tasks out of order).
- Consumers reuse the standard batching pipeline (batch -> format/collate ->
  finalize), so ``iter_torch_batches`` works unchanged; only the ref-level
  prefetch/resolve stages are skipped.
- A consumer is any actor that mixes in ``PushSplitReceiverMixin`` (Ray
  Train's ``RayTrainWorker`` does).

Overview::

                  PushSplitCoordinator actor
    +--------------------------------------------------+
    |  StreamingExecutor:  read -> ... -> split(n)     |
    |      split 0       split 1     ...    split n-1  |
    |         |             |                  |       |
    |     pusher 0      pusher 1          pusher n-1   |
    +---------|-------------^--------------------------+
              | blocks      | request_block()
              | (by value,  | (window of requests kept
              |  sequenced) |  outstanding; topped up
              |             |  per consumed block)
              v             |
    +--------------------------------------------------+
    |  consumer actor i  (mixes PushSplitReceiverMixin)|
    |    deliveries -> reorder by seq -> local queue   |
    |    PushBasedDataIterator: pop -> batch -> train  |
    +--------------------------------------------------+

Not implemented (prototype): stats/metrics export, locality-aware pushing,
mid-epoch consumer replacement.
"""

import logging
import math
import queue
import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, Iterator, List, Optional, Set, Tuple, Union

import ray
from ray.data._internal.block_batching.interfaces import ResolvedBlock
from ray.data._internal.block_batching.iter_batches import BatchIterator
from ray.data._internal.stats import DatasetStats
from ray.data.block import Block
from ray.data.context import DataContext
from ray.data.iterator import DataIterator
from ray.util.debug import log_once

if TYPE_CHECKING:
    from ray.data.dataset import Dataset, Schema

logger = logging.getLogger(__name__)

BLOCKED_CLIENT_WARN_TIMEOUT = 30


@dataclass
class _BlockPush:
    """Wire header for one pushed block; the block itself travels by value
    as a resolved top-level task arg."""

    size_bytes: int
    num_rows: int


@dataclass
class _BlockDelivery:
    """Local queue entry: one materialized Block + its size."""

    block: Block
    size_bytes: int
    num_rows: int


@dataclass
class _EndOfEpoch:
    epoch_id: int


@dataclass
class _ExecutorError:
    error: Exception


# Sequenced deliveries; errors arrive unsequenced (fail fast).
_SequencedItem = Union[_BlockPush, _EndOfEpoch]
_QueueItem = Union[_BlockDelivery, _EndOfEpoch, _ExecutorError]


@ray.remote(num_cpus=0)
class PushSplitCoordinator:
    """Coordinator actor that pushes split output to registered consumers.

    Runs the streaming executor (one per epoch, like SplitCoordinator) plus
    one pusher thread per split, gated by the consumer's demand
    (request_block).
    """

    # How often a demand-waiting pusher re-checks its stop event; requests
    # wake it immediately.
    DEMAND_WAIT_TIMEOUT_S = 0.5

    def __init__(self, dataset: "Dataset", n: int):
        # Deep copy, same as SplitCoordinator.
        self._data_context = dataset.context.copy()
        ray.data.DataContext._set_current(self._data_context)

        self._base_dataset = dataset
        self._n = n

        # Guards epoch/barrier/finished-splits/consumer-registry state.
        self._lock = threading.RLock()
        self._dataset_state_lock = threading.Lock()
        self._schema = None

        self._current_executor = None
        self._output_iterator = None
        self._cur_epoch = -1
        self._num_unarrived_splits_at_barrier = n
        # Barrier waiters spin until the last arrival's teardown completes.
        self._teardown_complete_for: Optional[int] = None
        self._finished_splits: Set[int] = set()
        self._gen_epoch_error: Optional[Exception] = None

        # split_idx -> (handle, key); see register().
        self._consumers: Dict[int, Tuple[ray.actor.ActorHandle, str]] = {}
        self._pusher_threads: List[threading.Thread] = []
        self._pusher_stop_events: Dict[int, threading.Event] = {}

        # Per-split demand (in blocks), guarded by that split's Condition;
        # the pusher sends while demand > 0 and waits on the Condition
        # otherwise (a request wakes it immediately).
        self._demand_conds: Dict[int, threading.Condition] = {
            i: threading.Condition() for i in range(n)
        }
        self._demand_blocks: Dict[int, int] = dict.fromkeys(range(n), 0)

        # Byte accounting for producer pacing. Each entry has one writer
        # (_bytes_pushed: the split's pusher; _bytes_consumed_reported:
        # request_block under the split's Condition); readers tolerate
        # staleness, so no extra locking is needed.
        self._bytes_pushed: Dict[int, int] = dict.fromkeys(range(n), 0)
        self._bytes_consumed_reported: Dict[int, int] = dict.fromkeys(range(n), 0)

        logger.debug(f"PushSplitCoordinator created: {n=}")

    # ------------------------------------------------------------------
    # Control plane (actor tasks).
    # ------------------------------------------------------------------

    def register(
        self,
        split_idx: int,
        consumer: ray.actor.ActorHandle,
        key: str,
    ) -> None:
        """Register the consumer actor for a split (called once per epoch;
        re-registering is fine). The actor class must mix in
        ``PushSplitReceiverMixin``."""
        if not hasattr(consumer, "_push_split_deliver"):
            raise ValueError(
                f"The consumer actor for split {split_idx} does not expose "
                "the push receiver methods. Mix PushSplitReceiverMixin into "
                "the actor class that hosts the PushBasedDataIterator."
            )
        with self._lock:
            self._consumers[split_idx] = (consumer, key)
        logger.debug(f"Registered consumer for split {split_idx}.")

    def start_epoch(self, split_idx: int) -> int:
        """Barrier: blocks until all n splits arrive, then starts the epoch."""
        return self._barrier(split_idx)

    def request_block(
        self,
        split_idx: int,
        epoch_id: int,
        consumed_bytes: int,
        num_blocks: int = 1,
    ) -> None:
        """Fire-and-forget demand from a consumer: "send me ``num_blocks``
        more blocks".

        Sent once per consumed block; the consumer sizes ``num_blocks`` to
        keep its prefetch window full (0 is a pure consumption report).
        ``consumed_bytes`` doubles as the consumption report that paces the
        producer.
        """
        cond = self._demand_conds[split_idx]
        with cond:
            # Checked under the cond so a stale request from a previous epoch
            # can never leave demand behind (see _try_start_new_epoch).
            if epoch_id != self._cur_epoch:
                return
            self._demand_blocks[split_idx] += num_blocks
            self._bytes_consumed_reported[split_idx] += consumed_bytes
            cond.notify()
        # Producer pacing is refreshed by the pusher after each push
        # (cheaper than doing it per request).

    def notify_split_finished(self, epoch_id: int, split_idx: int) -> None:
        """Consumer stopped iterating ``epoch_id``; stale epochs are ignored."""
        with self._lock:
            if epoch_id != self._cur_epoch:
                return
            stop_event = self._pusher_stop_events.get(split_idx)
        if stop_event is not None:
            stop_event.set()
        self._finish_split(epoch_id, split_idx)

    def get_dataset_schema(self):
        with self._dataset_state_lock:
            if self._schema is not None:
                return self._schema
            if self._current_executor is not None and self._current_executor.is_alive():
                raise RuntimeError(
                    "Cannot call schema() during active dataset execution."
                )
            self._schema = self._base_dataset.schema()
            return self._schema

    def stats(self):
        if self._current_executor:
            return self._current_executor.get_stats()
        return self._base_dataset._raw_stats()

    def get_dataset_context(self) -> "DataContext":
        return self._data_context

    def get_dataset_tag(self, output_split_idx: int) -> Dict[str, str]:
        return {
            "dataset": self._base_dataset.get_dataset_id(),
            "split_index": str(output_split_idx),
        }

    def shutdown_executor(self):
        with self._lock:
            if self._current_executor is not None:
                self._current_executor.shutdown(force=False)

    def _is_executor_shutdown(self) -> bool:
        """For testing only."""
        with self._lock:
            executor = self._current_executor
        return executor is not None and executor._shutdown

    # ------------------------------------------------------------------
    # Epoch lifecycle.
    # ------------------------------------------------------------------

    def _barrier(self, split_idx: int) -> int:
        """Arrive and block until the start of the next epoch."""
        with self._lock:
            logger.debug(
                f"Split {split_idx} arriving at barrier for epoch "
                f"{self._cur_epoch + 1}."
            )
            starting_epoch = self._cur_epoch
            self._num_unarrived_splits_at_barrier -= 1
            is_last_arrival = self._num_unarrived_splits_at_barrier == 0

        if is_last_arrival:
            # The last arrival tears down the previous epoch before the
            # barrier releases; done outside self._lock so exiting pushers
            # can still take it.
            self._teardown_epoch()
            with self._lock:
                self._teardown_complete_for = starting_epoch

        start_time = time.time()
        while self._cur_epoch == starting_epoch and (
            self._num_unarrived_splits_at_barrier != 0
            or self._teardown_complete_for != starting_epoch
        ):
            if time.time() - start_time > BLOCKED_CLIENT_WARN_TIMEOUT:
                if log_once(f"push_split_blocked_{split_idx}_{starting_epoch}"):
                    logger.warning(
                        f"PushBasedDataIterator(epoch={starting_epoch}, "
                        f"split={split_idx}) blocked waiting on other clients "
                        f"for more than {BLOCKED_CLIENT_WARN_TIMEOUT}s. All "
                        "clients must iterate their splits at the same time."
                    )
            time.sleep(0.1)

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

    def _teardown_epoch(self) -> None:
        """Stop pushers, force-shutdown the executor, join pusher threads.

        Shutdown must precede join: it is what unblocks a pusher waiting in
        get_next.
        """
        for event in self._pusher_stop_events.values():
            event.set()
        if self._current_executor is not None:
            self._current_executor.shutdown(force=True)
        for thread in self._pusher_threads:
            thread.join(timeout=10)
            if thread.is_alive():
                logger.warning(f"Pusher thread {thread.name} did not exit in 10s.")
        self._pusher_threads = []
        self._pusher_stop_events = {}

    def _try_start_new_epoch(self, starting_epoch: int) -> None:
        with self._lock:
            # Start the epoch exactly once. The bump must precede
            # _reset_state (see request_block).
            if self._cur_epoch == starting_epoch:
                self._cur_epoch += 1
                self._reset_state()
                try:
                    if len(self._consumers) != self._n:
                        raise RuntimeError(
                            f"Expected {self._n} registered consumers, got "
                            f"{len(self._consumers)}."
                        )
                    ds = self._base_dataset
                    self._current_executor = ds._create_executor()
                    self._output_iterator = ds._build_bundle_iterator(
                        self._current_executor
                    )
                    # Register the external consumers with the resource
                    # manager (same as SplitCoordinator).
                    self._current_executor.set_external_consumer_bytes(0)
                    self._spawn_pushers()
                    logger.debug(
                        f"Starting epoch {self._cur_epoch} (all {self._n} "
                        "clients synced)."
                    )
                except Exception as e:
                    logger.warning(
                        f"Error creating executor for epoch {self._cur_epoch}: {e}"
                    )
                    self._gen_epoch_error = e

        if self._gen_epoch_error is not None:
            raise self._gen_epoch_error

    def _reset_state(self) -> None:
        self._num_unarrived_splits_at_barrier = self._n
        self._finished_splits.clear()
        self._gen_epoch_error = None
        # The previous epoch's pushers were already joined, so plain
        # reassignment is safe here.
        self._bytes_pushed = dict.fromkeys(range(self._n), 0)
        self._bytes_consumed_reported = dict.fromkeys(range(self._n), 0)
        for i in range(self._n):
            with self._demand_conds[i]:
                self._demand_blocks[i] = 0

    def _spawn_pushers(self) -> None:
        self._pusher_stop_events = {i: threading.Event() for i in range(self._n)}
        self._pusher_threads = []
        for i in range(self._n):
            thread = threading.Thread(
                target=self._pusher_loop,
                args=(self._cur_epoch, i, self._pusher_stop_events[i]),
                name=f"push_split_pusher_{i}",
                daemon=True,
            )
            thread.start()
            self._pusher_threads.append(thread)

    # ------------------------------------------------------------------
    # Pusher (plain threads, one per split per epoch).
    # ------------------------------------------------------------------

    def _make_consumer_ops(self, epoch_id: int, split_idx: int):
        """Build (push_block, push_eof, push_error) targeting the consumer's
        PushSplitReceiverMixin methods."""
        consumer, key = self._consumers[split_idx]

        def push_block(seq, entry, size_bytes, num_rows):
            # entry.ref is a top-level arg, so Ray resolves it and the
            # consumer receives the Block by value — no ObjectRef crosses
            # the wire, and the executor can free the block once delivered.
            consumer._push_split_deliver.remote(
                key, epoch_id, seq, _BlockPush(size_bytes, num_rows), entry.ref
            )

        def push_eof(seq):
            consumer._push_split_deliver.remote(
                key, epoch_id, seq, _EndOfEpoch(epoch_id)
            )

        def push_error(error):
            consumer._push_split_deliver_error.remote(key, epoch_id, error)

        return push_block, push_eof, push_error

    def _pusher_loop(
        self, epoch_id: int, split_idx: int, stop: threading.Event
    ) -> None:
        push_block, push_eof, push_error = self._make_consumer_ops(epoch_id, split_idx)
        output_iterator = self._output_iterator
        cond = self._demand_conds[split_idx]
        # Deliveries carry a sequence number; the receiver reorders them.
        seq = 0
        try:
            while not stop.is_set():
                # Wait for demand; a request wakes this immediately. A dead
                # consumer stops requesting, which parks this thread with the
                # split's remaining data kept in the executor.
                # TODO(push-split): let a replacement worker resume a parked
                # split mid-epoch (design validated at 48df73dd0e).
                with cond:
                    if self._demand_blocks[split_idx] <= 0:
                        cond.wait(self.DEMAND_WAIT_TIMEOUT_S)
                        continue

                # Blocks like a pull consumer would (preserving the
                # executor's backpressure signals); raises StopIteration at
                # end of stream. A multi-block bundle is sent whole.
                bundle = output_iterator.get_next(split_idx)
                for entry in bundle.blocks:
                    size_bytes = entry.metadata.size_bytes or 0
                    num_rows = entry.metadata.num_rows or 0
                    push_block(seq, entry, size_bytes, num_rows)
                    seq += 1
                    with cond:
                        self._demand_blocks[split_idx] -= 1
                    self._bytes_pushed[split_idx] += size_bytes
                self._update_external_consumer_bytes()
        except StopIteration:
            if not stop.is_set():
                logger.debug(
                    f"Split {split_idx} epoch {epoch_id} exhausted; sending EOF."
                )
                # EOF is sequenced too, so it cannot overtake blocks that
                # are still fetching their args.
                push_eof(seq)
            return
        except Exception as e:
            if not stop.is_set():
                logger.warning(f"Split {split_idx} epoch {epoch_id} pusher failed: {e}")
                try:
                    push_error(_ExecutorError(e))
                except Exception:
                    # e.g. unpicklable exception.
                    push_error(_ExecutorError(RuntimeError(repr(e))))
            return

    def _finish_split(self, epoch_id: int, split_idx: int) -> None:
        executor_to_shutdown = None
        with self._lock:
            if epoch_id != self._cur_epoch:
                return
            self._finished_splits.add(split_idx)
            # Zero this split's contribution to external consumer bytes.
            self._bytes_consumed_reported[split_idx] = self._bytes_pushed[split_idx]
            if (
                len(self._finished_splits) == self._n
                and self._current_executor is not None
            ):
                executor_to_shutdown = self._current_executor
        self._update_external_consumer_bytes()
        # Shut down outside the lock (joins the scheduling thread).
        if executor_to_shutdown is not None:
            logger.debug(
                f"All splits finished epoch {epoch_id}; shutting down executor."
            )
            executor_to_shutdown.shutdown(force=True)

    def _update_external_consumer_bytes(self) -> None:
        """Report bytes buffered at consumers; this paces the producer.

        Demand only gates the pushers. Without this feed (the analog of the
        pull model's prefetched-bytes reports), a fast producer runs the
        whole epoch ahead of slow consumers and spills.
        """
        executor = self._current_executor
        if executor is None:
            return
        total = sum(
            max(0, self._bytes_pushed[i] - self._bytes_consumed_reported[i])
            for i in range(self._n)
        )
        try:
            executor.set_external_consumer_bytes(total)
        except Exception:
            # The executor may be mid-shutdown during an epoch transition.
            pass


# ---------------------------------------------------------------------------
# Consumer side. Deliveries arrive via PushSplitReceiverMixin's actor methods
# and land in the process-local registry, where the PushBasedDataIterator
# (running on another thread in the same process) picks them up.
# ---------------------------------------------------------------------------

_RECEIVER_REGISTRY_LOCK = threading.Lock()
_RECEIVER_REGISTRY: Dict[str, "_PushReceiver"] = {}


class _PushReceiver:
    """Receive state for one (coordinator, split) pair: the local block
    queue plus the sequence-reorder buffer."""

    def __init__(self):
        self.queue: "queue.Queue[_QueueItem]" = queue.Queue()
        self.lock = threading.Lock()
        self.cur_epoch: Optional[int] = None
        self.reorder_epoch: Optional[int] = None
        self.reorder_next_seq = 0
        self.reorder_pending: Dict[int, _QueueItem] = {}

    def reset(self) -> None:
        """Reset before re-arriving at the epoch barrier.

        Swaps in a fresh queue object (instead of draining) so a lingering
        generator from an early-exited epoch cannot steal the new epoch's
        deliveries.
        """
        with self.lock:
            self.cur_epoch = None
            self.reorder_epoch = None
            self.reorder_next_seq = 0
            self.reorder_pending = {}
            self.queue = queue.Queue()

    def begin_epoch(self, epoch: int) -> None:
        with self.lock:
            # Set before the first request_block, so no delivery can arrive
            # while cur_epoch is stale.
            self.cur_epoch = epoch

    def deliver(
        self,
        epoch_id: int,
        seq: int,
        item: _SequencedItem,
        block: Optional[Block] = None,
    ) -> None:
        """Deliver one sequenced item, releasing items to the queue in seq
        order. Stale-epoch items are dropped."""
        if isinstance(item, _BlockPush):
            queue_item: _QueueItem = _BlockDelivery(
                block, item.size_bytes, item.num_rows
            )
        else:
            queue_item = item
        with self.lock:
            if epoch_id != self.cur_epoch:
                return
            if self.reorder_epoch != epoch_id:
                self.reorder_epoch = epoch_id
                self.reorder_next_seq = 0
                self.reorder_pending = {}
            self.reorder_pending[seq] = queue_item
            while self.reorder_next_seq in self.reorder_pending:
                self.queue.put(self.reorder_pending.pop(self.reorder_next_seq))
                self.reorder_next_seq += 1

    def deliver_error(self, epoch_id: int, error: _ExecutorError) -> None:
        """Deliver an _ExecutorError immediately (fail fast, unsequenced)."""
        with self.lock:
            if epoch_id == self.cur_epoch:
                self.queue.put(error)


class PushSplitReceiverMixin:
    """Receive methods for actors hosting PushBasedDataIterators; mix into
    the consumer's actor class (Ray Train's ``RayTrainWorker`` does).

    A stateless shim over the registry, so it needs no ``__init__``
    cooperation: one actor may host several shards, and the iterator reaches
    the same ``_PushReceiver`` state through the registry.
    """

    def _push_split_deliver(
        self,
        key: str,
        epoch_id: int,
        seq: int,
        item: _SequencedItem,
        block: Optional[Block] = None,
    ) -> None:
        receiver = _RECEIVER_REGISTRY.get(key)
        if receiver is not None:
            receiver.deliver(epoch_id, seq, item, block)

    def _push_split_deliver_error(
        self, key: str, epoch_id: int, error: _ExecutorError
    ) -> None:
        receiver = _RECEIVER_REGISTRY.get(key)
        if receiver is not None:
            receiver.deliver_error(epoch_id, error)


class _MaterializedBatchIterator(BatchIterator):
    """BatchIterator over already-materialized blocks: skips the ref-level
    prefetch/resolve stages, inherits everything else unchanged."""

    def _pipeline(self, resolved_blocks: Iterator[ResolvedBlock]):
        batch_iter = self._blocks_to_batches(resolved_blocks)
        batch_iter = self._format_batches(batch_iter)
        if self._preserve_order:
            batch_iter = self._restore_original_batch_order(batch_iter)
        batch_iter = self._finalize_batches(batch_iter)
        yield from batch_iter


class PushBasedDataIterator(DataIterator):
    """PROTOTYPE: DataIterator over one split of a push-based streaming split.

    Picklable; ship it into any actor whose class mixes in
    ``PushSplitReceiverMixin`` (e.g. a Ray Train worker). At iteration time
    it registers the hosting actor with the coordinator, then iterates the
    blocks the coordinator pushes into the local receiver queue.
    """

    @staticmethod
    def create(
        split_dataset: "Dataset",
        n: int,
    ) -> List["PushBasedDataIterator"]:
        """Create the coordinator and one iterator per split.

        ``split_dataset`` must already be wrapped in a ``StreamingSplit``
        logical op (see ``Dataset.streaming_split_push_based``).
        """
        coord_actor = PushSplitCoordinator.options(
            # n barrier-blocked start_epoch calls + headroom for other RPCs.
            max_concurrency=n + 2,
            label_selector={
                ray._raylet.RAY_NODE_ID_KEY: ray.get_runtime_context().get_node_id()
            },
        ).remote(split_dataset, n)
        return [PushBasedDataIterator(coord_actor, i, n) for i in range(n)]

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
        # Epoch this split is currently consuming (see
        # StreamSplitDataIterator._active_epoch for the threading protocol).
        self._active_epoch: Optional[int] = None
        # Prefetch window, refreshed by _create_batch_iterator from the
        # user's iter_batches() arguments (same knobs as the pull model).
        self._prefetch_batches = 1
        self._prefetch_batch_size: Optional[int] = None

    def _receiver_key(self) -> str:
        return f"{self._coord_actor._actor_id.hex()}:{self._output_split_idx}"

    def _to_ref_bundle_iterator(
        self,
    ) -> Tuple[Iterator[ResolvedBlock], Optional[DatasetStats], None]:
        # Deviates from the base contract on purpose: yields ResolvedBlock
        # instead of RefBundle (blocks arrive materialized); the paired
        # _create_batch_iterator override consumes them.
        def gen_blocks() -> Iterator[ResolvedBlock]:
            try:
                self_handle = ray.get_runtime_context().current_actor
                assert self_handle is not None
            except Exception as e:
                raise RuntimeError(
                    "PushBasedDataIterator must be iterated from inside a Ray "
                    "actor whose class mixes in PushSplitReceiverMixin (e.g. "
                    "a Ray Train worker): the coordinator pushes blocks to "
                    "the hosting actor's receiver methods."
                ) from e

            key = self._receiver_key()
            with _RECEIVER_REGISTRY_LOCK:
                receiver = _RECEIVER_REGISTRY.get(key)
                if receiver is None:
                    receiver = _PushReceiver()
                    _RECEIVER_REGISTRY[key] = receiver
            # Reset receiver state before the barrier; re-registering every
            # epoch is fine (idempotent overwrite).
            receiver.reset()
            ray.get(
                self._coord_actor.register.remote(
                    self._output_split_idx, self_handle, key=key
                )
            )
            epoch = ray.get(
                self._coord_actor.start_epoch.remote(self._output_split_idx)
            )
            self._active_epoch = epoch
            receiver.begin_epoch(epoch)

            # Prefetch window: keep enough one-block requests outstanding to
            # cover the same `prefetch_batches * batch_size` row window the
            # pull model holds prefetched, plus one pipelined request (the
            # pull model's 1-deep get()). Blocks the coordinator has pushed
            # but we have not consumed sit in the local receiver queue — that
            # queue IS the prefetch buffer, and it keeps several transfers in
            # flight instead of one.
            prefetch_batches = self._prefetch_batches
            window_rows = (
                prefetch_batches * self._prefetch_batch_size
                if prefetch_batches > 0 and self._prefetch_batch_size is not None
                else None
            )
            outstanding = 0  # requested but not yet popped
            rows_seen = 0
            blocks_seen = 0

            def target_outstanding() -> int:
                if prefetch_batches <= 0:
                    return 1
                if window_rows is None:
                    # No batch size: window in blocks, like the pull model.
                    return prefetch_batches + 1
                if blocks_seen == 0:
                    # Rows-per-block unknown until the first delivery.
                    return 2
                avg_rows = max(1.0, rows_seen / blocks_seen)
                return math.ceil(window_rows / avg_rows) + 1

            def report_and_top_up(consumed_bytes: int) -> None:
                # One RPC per consumed block: reports consumption (producer
                # pacing) and refills the request window.
                nonlocal outstanding
                num_blocks = max(0, target_outstanding() - outstanding)
                self._coord_actor.request_block.remote(
                    self._output_split_idx, epoch, consumed_bytes, num_blocks
                )
                outstanding += num_blocks

            report_and_top_up(0)
            # reset() gave this epoch a fresh queue, so a lingering
            # generator from an early-exited epoch can't steal deliveries;
            # the loop condition below reaps such generators.
            epoch_queue = receiver.queue

            while self._active_epoch == epoch:
                try:
                    item = epoch_queue.get(timeout=1.0)
                except queue.Empty:
                    continue
                if isinstance(item, _EndOfEpoch):
                    logger.debug(
                        f"Split {self._output_split_idx}: epoch {epoch} exhausted."
                    )
                    return
                if isinstance(item, _ExecutorError):
                    raise item.error
                assert isinstance(item, _BlockDelivery)
                outstanding -= 1
                blocks_seen += 1
                rows_seen += item.num_rows
                report_and_top_up(item.size_bytes)
                yield ResolvedBlock(block=item.block)

        return gen_blocks(), self._iter_stats, None

    def _create_batch_iterator(self, ref_bundles_iter, **kwargs):
        # Capture the prefetch window before iteration starts; gen_blocks
        # reads it lazily on its first next().
        self._prefetch_batches = kwargs.get("prefetch_batches", 1)
        self._prefetch_batch_size = kwargs.get("batch_size")
        # The iterator yields ResolvedBlocks (see _to_ref_bundle_iterator).
        return _MaterializedBatchIterator(ref_bundles_iter, **kwargs)

    def _on_iteration_end(self, executor) -> None:
        """Notify the coordinator on any end of iteration (exhaustion, early
        break, or exception); runs on the consumer thread."""
        epoch = self._active_epoch
        if epoch is None:
            return
        self._active_epoch = None
        self._coord_actor.notify_split_finished.remote(epoch, self._output_split_idx)

    def stats(self) -> str:
        stats = ray.get(self._coord_actor.stats.remote())
        summary = stats.to_summary()
        summary.iter_stats = self._iter_stats.to_summary().iter_stats
        return summary.to_string()

    def schema(self) -> Optional["Schema"]:
        return ray.get(self._coord_actor.get_dataset_schema.remote())

    def get_context(self) -> DataContext:
        return ray.get(self._coord_actor.get_dataset_context.remote())

    def world_size(self) -> int:
        return self._world_size

    def _get_dataset_tag(self) -> Dict[str, str]:
        return ray.get(self._coord_actor.get_dataset_tag.remote(self._output_split_idx))
