"""PROTOTYPE: push-based streaming_split.

Inverts the pull model of ``stream_split_iterator.py``: instead of each
consumer calling ``coordinator.get.remote(...)`` per block, the coordinator
runs one pusher thread per split that pulls bundles from the streaming
executor's output iterator and pushes them to the consumer actor. The
consumer can be ANY Ray actor (e.g. a Ray Train worker): delivery goes
the ``PushSplitReceiverMixin`` methods on the hosting actor class (Ray
Train's ``RayTrainWorker`` mixes it in) into a process-local receiver
registry. Blocks are pushed BY
VALUE (the block ref is a resolved top-level arg; only row/byte counters
ride in the message), so no ObjectRef ever crosses the wire and the consumer
never becomes a borrower of the executor's refs — the executor can free each
block as soon as its delivery completes. The consumer buffers the
materialized Blocks directly (zero-copy views for Arrow), and
``PushBasedDataIterator`` feeds them straight into the batching stages of
the standard pipeline via ``_MaterializedBatchIterator`` — the ref-level
prefetch and resolve stages are skipped entirely; batching, the
format/collate threadpool, order restore, and finalize (i.e. everything
``iter_torch_batches`` needs) are reused unchanged.

Flow control is demand-driven, one block at a time — the exact analog of
the pull model pulling one RefBundle per ``get``: the consumer requests one
block at iteration start, and one more after popping each block from its
local queue, so the next block is always in flight while the previous one is
consumed (pull's 1-deep pipelining). The coordinator accumulates per-split
demand and its pusher sends blocks while demand is positive, then sleeps on
a condition until the next request. Each request carries the popped block's
bytes, doubling as the consumption report that feeds producer pacing. There
is no polling: an idle or dead consumer simply stops requesting.

Ordering: the consumer actor may be multi-threaded, and multi-threaded actors
execute tasks out of order (dispatch follows argument readiness; see
``allow_out_of_order_execution`` in ``ray.actor``); an argless EOF delivery
could even overtake block deliveries. Every push therefore carries a
per-split sequence number (EOF included), and the receiver re-sequences items
through a reorder buffer before they enter the local queue — preserving the
pull path's per-split ordering and making EOF safe.

Pipeline overview::

                PushSplitCoordinator actor (pinned to the creating node)
    +--------------------------------------------------------------------+
    |  StreamingExecutor (recreated each epoch)                          |
    |                                                                    |
    |    read -> map -> ... -> OutputSplitter                            |
    |                              | bundles tagged with split idx       |
    |                              v                                     |
    |          per-split output queues (OpBufferQueue)                   |
    |           split 0         split 1      ...      split n-1          |
    |              |               |                     |               |
    |              v               v                     v               |
    |          pusher 0        pusher 1      ...     pusher n-1          |
    |  (one thread per split; blocks in output_iterator.get_next(i),     |
    |   which keeps the executor's num_waiting_consumers signal alive)   |
    +------------------|----------------^--------------------------------+
                       |                |
     data plane        |                |  flow control (demand-driven,
     (fire-and-forget) |                |  fire-and-forget)
       consumer._push_ |                |
         split_deliver(|                |  request_block(split, epoch,
         key, epoch,   |                |      popped bytes):
         seq, bytes,   |                |  (1) one at iteration start
         block BY      |                |  (2) one more after each popped
         VALUE)        |                |      block (bytes = consumption)
       EOF: same, seq'd|                |  pusher: send blocks while demand
       errors unseq'd  |                |  > 0, else wait on the condition
                       v                |
    +--------------------------------------------------------------------+
    |   consumer actor i (mixes in PushSplitReceiverMixin, e.g. Ray      |
    |   Train's RayTrainWorker)                                          |
    |                                                                    |
    |  actor task thread(s): _push_split_deliver -> reorder buffer       |
    |      (by seq) -> _PushReceiver.queue holding materialized Blocks   |
    |      (no refs at all, no ray.put); errors jump the queue           |
    |  iteration thread (e.g. Train's ThreadRunner):                     |
    |      PushBasedDataIterator: register(current_actor) ->             |
    |      start_epoch (barrier RPC, all n splits sync) ->               |
    |      request_block() -> pop queue -> request_block(popped bytes)   |
    |      -> _MaterializedBatchIterator: batch -> format/collate        |
    |      threadpool -> finalize                                        |
    |      finally: notify_split_finished(epoch, i) RPC -> coordinator   |
    +--------------------------------------------------------------------+

For contrast, the pull model (``stream_split_iterator.py``) has each consumer
repeatedly call ``coordinator.get.remote(epoch, split_idx, prefetched_bytes)``
and block on the result; here the RPC direction is inverted and the consumer's
iterator only reads its local queue.

Liveness note: the pusher threads block inside
``output_iterator.get_next(split_idx)`` (ultimately
``OpState.get_output_blocking``) exactly like today's pull consumers, so the
executor's ``_num_waiting_consumers`` / ``OutputBackpressureGuard`` machinery
keeps working unchanged.

If a consumer dies mid-epoch, it simply stops requesting, so its pusher
parks on the demand condition automatically — no death detection is needed,
and the split's remaining data stays queued in the executor (paced by
backpressure) rather than being drained, keeping a future mid-epoch
replacement worker easy to add (see the TODO in _pusher_loop). Until then,
recovery is Ray Train's group restart: the next epoch's barrier tears the
parked pusher down and fresh registrations take over.

Not implemented (prototype): stats/metrics export, locality-aware pushing,
mid-epoch consumer replacement.
"""

import logging
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
    """Wire format of one pushed block: its byte size only, NO ObjectRef.

    The block itself travels as a resolved top-level task arg (by value), so
    the consumer never becomes a borrower of the executor's block refs; it
    buffers the materialized Block directly.
    """

    size_bytes: int


@dataclass
class _BlockDelivery:
    """Local queue entry: one materialized Block + its byte size."""

    block: Block
    size_bytes: int


@dataclass
class _EndOfEpoch:
    epoch_id: int


@dataclass
class _ExecutorError:
    error: Exception


# What the pusher sends through the sequenced channel (_receiver_deliver).
_SequencedItem = Union[_BlockPush, _EndOfEpoch]
# What can appear on a receiver's queue (_ExecutorError arrives unsequenced).
_QueueItem = Union[_BlockDelivery, _EndOfEpoch, _ExecutorError]


@ray.remote(num_cpus=0)
class PushSplitCoordinator:
    """Coordinator actor that pushes split output to registered consumers.

    Runs a streaming executor locally (one per epoch, like SplitCoordinator)
    plus one pusher thread per split that pulls from the executor's output
    iterator and pushes to that split's consumer, gated by the consumer's
    outstanding demand (request_block).
    """

    # A pusher waiting for demand re-checks its stop event at this cadence
    # (requests wake it immediately via the condition; this only bounds how
    # long teardown can be ignored).
    DEMAND_WAIT_TIMEOUT_S = 0.5

    def __init__(self, dataset: "Dataset", n: int):
        # Deep copy so updates to the base dataset's context don't affect this
        # process's global DataContext (same as SplitCoordinator).
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
        # Epoch whose pre-epoch teardown (pusher join + executor shutdown)
        # has completed; barrier waiters spin until it matches their epoch.
        self._teardown_complete_for: Optional[int] = None
        self._finished_splits: Set[int] = set()
        self._gen_epoch_error: Optional[Exception] = None

        # split_idx -> (handle, key); see register().
        self._consumers: Dict[int, Tuple[ray.actor.ActorHandle, str]] = {}
        self._pusher_threads: List[threading.Thread] = []
        self._pusher_stop_events: Dict[int, threading.Event] = {}

        # Demand-driven flow control: request_block accumulates per-split
        # demand (in blocks) under that split's Condition lock; the split's
        # pusher sends blocks while demand is positive and waits on the
        # Condition otherwise (a request wakes it immediately).
        self._demand_conds: Dict[int, threading.Condition] = {
            i: threading.Condition() for i in range(n)
        }
        self._demand_blocks: Dict[int, int] = dict.fromkeys(range(n), 0)

        # Push accounting:
        # - _bytes_pushed: single writer = the split's pusher thread.
        # - _bytes_consumed_reported: accumulated by request_block under the
        #   split's demand-Condition lock (requests double as consumption
        #   reports); _finish_split additionally zeroes a finished split's
        #   contribution (benign last-writer-wins — no requests follow it).
        # - Cross-thread readers only compute the heuristic sum in
        #   _update_external_consumer_bytes, where GIL-atomic int reads and
        #   slight staleness across splits are acceptable.
        # - Keys are fixed (dict.fromkeys at reset), so dict iteration never
        #   races a resize; _reset_state reassigns only after the old
        #   pushers have been joined by _teardown_epoch.
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
        """Register the consumer actor for a split.

        The consumer must mix ``PushSplitReceiverMixin`` into its actor
        class (Ray Train's RayTrainWorker does); deliveries go through the
        mixin's methods into the process-local ``_RECEIVER_REGISTRY`` under
        ``key``. Re-registering the same split is allowed (once per epoch).
        """
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

    def request_block(self, split_idx: int, epoch_id: int, consumed_bytes: int) -> None:
        """Fire-and-forget demand from a consumer: "send me one more block".

        Sent once when iteration starts (consumed_bytes=0) and then after
        every block the consumer pops from its local queue — the exact
        analog of the pull model pulling one RefBundle per ``get``, with its
        1-deep pipelining (the next block is requested before the popped one
        is processed). It doubles as the consumption report: consumed_bytes
        feeds the external-consumer-bytes accounting that paces the
        producer. Stale-epoch requests are dropped.
        """
        cond = self._demand_conds[split_idx]
        with cond:
            # Epoch check INSIDE the cond, atomically with the apply: a stale
            # request from the previous epoch either fails this check (the
            # epoch was already bumped) or is wiped by _reset_state's
            # demand zeroing (which runs under the same cond AFTER the bump).
            # Otherwise phantom demand could survive into the new epoch and
            # make the pusher push seq 0 before the consumer's begin_epoch,
            # whose receiver would drop it — wedging the reorder buffer (and
            # the split) for the whole epoch.
            if epoch_id != self._cur_epoch:
                return
            self._demand_blocks[split_idx] += 1
            self._bytes_consumed_reported[split_idx] += consumed_bytes
            cond.notify()
        # No _update_external_consumer_bytes here: the pusher refreshes the
        # executor after every push with the latest consumed counters, and
        # calling it per request would contend on the resource-manager lock
        # in the producer's hot path. While a pusher is idle the report goes
        # stale-low on consumption, which only over-reports consumer-held
        # bytes (the permissive, bounded direction).

    def notify_split_finished(self, epoch_id: int, split_idx: int) -> None:
        """Called by a consumer when it stops iterating ``epoch_id``.

        Fire-and-forget from the consumer; stale epochs are ignored (the
        epoch may have advanced in the meantime, same as SplitCoordinator).
        """
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
            # Tear down the previous epoch (stop + join pushers, shutdown
            # executor) BEFORE releasing the other splits from the barrier.
            # Done outside self._lock so pushers can still finish in-flight
            # bookkeeping (e.g. _finish_split takes _lock) while we join them.
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

        Runs on exactly one barrier thread (the last arrival), with no locks
        held. Shutdown MUST precede join: it is what kicks a pusher out of a
        blocking get_output_blocking call.
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
            # Gates that the epoch is started exactly once. The bump MUST
            # precede _reset_state so a stale request_block from the old
            # epoch cannot re-add demand after the reset (see request_block).
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
                    # Register the external consumers with the executor's
                    # resource manager (same as SplitCoordinator); consumer
                    # requests keep the value updated.
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
        # Safe without a lock: the previous epoch's pushers were joined in
        # _teardown_epoch before the barrier released, so no other thread
        # touches these dicts here.
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
        """Build (push_block, push_eof, push_error) for a consumer.

        Targets the PushSplitReceiverMixin methods on the consumer actor;
        blocks are delivered by value (resolved top-level arg), so the
        consumer buffers materialized Blocks and holds no refs.
        """
        consumer, key = self._consumers[split_idx]

        def push_block(seq, entry, size_bytes):
            # entry.ref is a TOP-LEVEL arg, so Ray resolves it and delivers
            # the block BY VALUE; no ObjectRef crosses the wire, so the
            # consumer never borrows the executor's refs and the executor
            # can free the block as soon as the delivery task completes.
            # The consumer buffers the materialized Block directly and its
            # iterator feeds blocks straight into the batching stages
            # (prefetch/resolve are skipped; see _MaterializedBatchIterator).
            consumer._push_split_deliver.remote(
                key, epoch_id, seq, _BlockPush(size_bytes), entry.ref
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
        # Per-split push sequence number. The consumer may be a multi-threaded
        # actor, so Ray may execute receive tasks out of order (dispatch
        # follows argument readiness); the consumer re-sequences by ``seq``.
        seq = 0
        try:
            while not stop.is_set():
                # 1) Wait for demand. A request_block call wakes this
                # immediately; the timeout only bounds how long teardown's
                # `stop` can go unnoticed. A consumer that died simply stops
                # requesting, so waiting here doubles as the "park" on
                # consumer death: the split's remaining data stays queued in
                # the executor (paced by backpressure) rather than drained.
                #
                # TODO(push-split): support a replacement worker joining
                # mid-epoch. Sketch (validated once in git history):
                # register() bumps a per-split registration version whose
                # bump rebinds this thread via _make_consumer_ops and resets
                # seq + this split's accounting to 0, and start_epoch
                # fast-paths a split whose consumer was replaced into the
                # ACTIVE epoch instead of the next-epoch barrier. Rows
                # already delivered to the dead consumer need the
                # data-checkpointing integration to be re-read.
                with cond:
                    if self._demand_blocks[split_idx] <= 0:
                        cond.wait(self.DEMAND_WAIT_TIMEOUT_S)
                        continue

                # 2) Positive demand: send blocks until it is covered.
                # get_next is a BLOCKING pull from the executor; it bumps
                # _num_waiting_consumers, preserving the pull model's
                # liveness/backpressure semantics, and raises StopIteration
                # at end of stream. (A bundle usually holds one block; a
                # multi-block bundle is sent whole, overshooting demand.)
                bundle = output_iterator.get_next(split_idx)
                for entry in bundle.blocks:
                    size_bytes = entry.metadata.size_bytes or 0
                    push_block(seq, entry, size_bytes)
                    seq += 1
                    with cond:
                        self._demand_blocks[split_idx] -= 1
                    # Single-writer (this thread); see __init__.
                    self._bytes_pushed[split_idx] += size_bytes
                self._update_external_consumer_bytes()
        except StopIteration:
            if not stop.is_set():
                logger.debug(
                    f"Split {split_idx} epoch {epoch_id} exhausted; sending EOF."
                )
                # EOF takes the next seq so it cannot overtake pending blocks
                # (it has no object args, so it would otherwise always be
                # dispatchable first).
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
            # Races the split's own pusher at worst last-writer-wins; the
            # pusher is stopping at this point and the value is heuristic.
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
        """Report bytes in flight + buffered at consumers to the executor.

        Push-model analog of SplitCoordinator._report_prefetched_bytes_to_executor.
        This is what paces the PRODUCER: consumer demand only stops the
        pushers from popping the executor's output queues, not the read/map
        tasks from filling them. Without this feed,
        DownstreamCapacityBackpressurePolicy never engages for the terminal
        op and a producer that outruns slow consumers parks the whole epoch
        in the object store (observed: 50GB spilled in the full_training
        release benchmark). To trade more producer run-ahead for throughput,
        tune data_context.downstream_capacity_backpressure_ratio rather than
        disabling the feed.
        """
        executor = self._current_executor
        if executor is None:
            return
        # Lock-free heuristic sum; see the accounting comment in __init__.
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
# Consumer-side receiving: the hosting actor class mixes in
# PushSplitReceiverMixin (Ray Train's RayTrainWorker does), whose methods run
# ON the consumer actor and land deliveries in the process-local receiver
# registry — which is how the (plain-object) PushBasedDataIterator running in
# the same process reaches the received blocks.
# ---------------------------------------------------------------------------

_RECEIVER_REGISTRY_LOCK = threading.Lock()
_RECEIVER_REGISTRY: Dict[str, "_PushReceiver"] = {}


class _PushReceiver:
    """Process-local receive state for one (coordinator, split_idx) pair.

    Deliveries land on the hosting actor's task thread(s); the iterator
    drains ``queue`` from its own (fetch) thread — hence the lock.
    """

    def __init__(self):
        self.queue: "queue.Queue[_QueueItem]" = queue.Queue()
        self.lock = threading.Lock()
        self.cur_epoch: Optional[int] = None
        self.reorder_epoch: Optional[int] = None
        self.reorder_next_seq = 0
        self.reorder_pending: Dict[int, _QueueItem] = {}

    def reset(self) -> None:
        """Reset before re-arriving at the epoch barrier.

        Swaps in a FRESH queue object rather than draining the old one: a
        zombie generator from an early-exited previous epoch can still be
        blocked inside the old queue's get() (its fetch thread only unwinds
        lazily), and with a shared queue it would race the new epoch's
        generator and steal deliveries — losing both the block and the
        demand replenish for it. The zombie keeps the abandoned queue; new
        deliveries go only to the new one.
        """
        with self.lock:
            self.cur_epoch = None
            self.reorder_epoch = None
            self.reorder_next_seq = 0
            self.reorder_pending = {}
            self.queue = queue.Queue()

    def begin_epoch(self, epoch: int) -> None:
        with self.lock:
            # Set BEFORE the first request_block is sent: the coordinator
            # only pushes in response to requests, so no delivery can arrive
            # while cur_epoch is stale.
            self.cur_epoch = epoch

    def deliver(
        self,
        epoch_id: int,
        seq: int,
        item: _SequencedItem,
        block: Optional[Block] = None,
    ) -> None:
        """Deliver one sequenced item (_BlockPush or _EndOfEpoch) in order.

        For a _BlockPush, ``block`` is the materialized Block (the pusher
        passed its ref as a resolved top-level arg — no borrowed refs, no
        ray.put). The block is buffered as-is; for Arrow blocks this is a
        zero-copy view whose buffers pin the local object-store copy while
        queued. Items from a dead epoch are dropped; the reorder buffer
        resets lazily on the first item of a new epoch.
        """
        if isinstance(item, _BlockPush):
            queue_item: _QueueItem = _BlockDelivery(block, item.size_bytes)
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
    """Remote-callable receive surface for actors hosting PushBasedDataIterators.

    Mix into the actor class of any push-split consumer (Ray Train's
    ``RayTrainWorker`` mixes it in); the coordinator's register() validates
    that the handle exposes these methods. Deliberately a thin, STATELESS
    shim over the ``_PushReceiver`` in the process-local registry:

    - stateless, so it mixes into any actor class without cooperative
      ``__init__`` chaining;
    - separate from ``_PushReceiver`` because one actor hosts one receiver
      per dataset shard (e.g. Train's train + valid iterators), and because
      the iterator — a plain object with no reference to the actor instance —
      reaches the same state through the module-level registry.
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
    """BatchIterator over already-materialized blocks.

    The input iterator yields ``ResolvedBlock`` (not ``RefBundle``): pushed
    blocks arrive by value, so the ref-level prefetch and resolve stages are
    skipped, while batching/shuffle, the format/collate threadpool, order
    restore, and finalize are inherited from ``BatchIterator`` unchanged.
    """

    def _pipeline(self, resolved_blocks: Iterator[ResolvedBlock]):
        # Steps 1-2 (prefetch refs, ray.get) of BatchIterator._pipeline are
        # intentionally absent.
        batch_iter = self._blocks_to_batches(resolved_blocks)
        batch_iter = self._format_batches(batch_iter)
        if self._preserve_order:
            batch_iter = self._restore_original_batch_order(batch_iter)
        batch_iter = self._finalize_batches(batch_iter)
        yield from batch_iter


class PushBasedDataIterator(DataIterator):
    """PROTOTYPE: DataIterator over one split of a push-based streaming split.

    Picklable, and can be shipped into any Ray actor whose class mixes in
    ``PushSplitReceiverMixin`` — e.g. a Ray Train worker. At iteration time
    it registers the hosting actor's own handle
    (``ray.get_runtime_context().current_actor``) with the coordinator, and
    the coordinator delivers blocks BY VALUE via the mixin's methods into
    the process-local receiver registry — the consumer holds no ObjectRefs
    at all. Iteration feeds the materialized blocks into
    ``_MaterializedBatchIterator`` (batch -> format/collate -> finalize,
    including iter_torch_batches); the ref-level prefetch/resolve stages are
    skipped.
    """

    @staticmethod
    def create(
        split_dataset: "Dataset",
        n: int,
    ) -> List["PushBasedDataIterator"]:
        """Create the coordinator and one iterator per split.

        ``split_dataset`` must already be wrapped in a ``StreamingSplit``
        logical op — see ``Dataset.streaming_split_push_based``, which
        mirrors how ``Dataset.streaming_split`` wraps the dataset before
        calling ``StreamSplitDataIterator.create``.
        """
        coord_actor = PushSplitCoordinator.options(
            # n concurrent start_epoch calls blocked at the barrier + headroom
            # for register/notify calls. Pusher threads are plain
            # threading.Threads and do not occupy actor concurrency slots.
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
        # Epoch this split is currently consuming; set by the fetch thread
        # once start_epoch returns, cleared by _on_iteration_end on the
        # consumer thread (same lock-free protocol as
        # StreamSplitDataIterator._active_epoch).
        self._active_epoch: Optional[int] = None

    def _receiver_key(self) -> str:
        return f"{self._coord_actor._actor_id.hex()}:{self._output_split_idx}"

    def _to_ref_bundle_iterator(
        self,
    ) -> Tuple[Iterator[ResolvedBlock], Optional[DatasetStats], None]:
        # NOTE: deviates from the base contract on purpose — blocks arrive
        # materialized, so this yields ResolvedBlock instead of RefBundle;
        # the paired _create_batch_iterator override consumes them.
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
            # Reset from any previous epoch BEFORE arriving at the barrier;
            # stragglers are dropped by the epoch check in _receiver_deliver.
            receiver.reset()

            # Re-registering every epoch is fine (idempotent overwrite).
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
            # Demand-driven flow control: ask for one block up front, then
            # one more after each popped block below — the exact analog of
            # the pull model's 1-deep pipelined get (the next block is in
            # flight while the popped one is processed). The replenish
            # carries the popped bytes, doubling as the consumption report
            # for producer pacing.
            self._coord_actor.request_block.remote(self._output_split_idx, epoch, 0)
            # Capture THIS epoch's queue: reset() swaps in a fresh object per
            # epoch so a zombie generator from an early-exited epoch can
            # never steal the new epoch's deliveries.
            epoch_queue = receiver.queue

            while self._active_epoch == epoch:
                # The epoch check reaps zombie generators: after an early
                # break, _on_iteration_end clears _active_epoch (and the next
                # epoch overwrites it), so a stale generator still parked in
                # this loop exits within one timeout instead of looping
                # forever on an abandoned queue.
                try:
                    item = epoch_queue.get(timeout=1.0)
                except queue.Empty:
                    # Loop so a hung coordinator is debuggable.
                    continue
                if isinstance(item, _EndOfEpoch):
                    logger.debug(
                        f"Split {self._output_split_idx}: epoch {epoch} exhausted."
                    )
                    return
                if isinstance(item, _ExecutorError):
                    raise item.error
                assert isinstance(item, _BlockDelivery)
                self._coord_actor.request_block.remote(
                    self._output_split_idx, epoch, item.size_bytes
                )
                yield ResolvedBlock(block=item.block)

        return gen_blocks(), self._iter_stats, None

    def _create_batch_iterator(self, ref_bundles_iter, **kwargs):
        # The "ref bundles" iterator actually yields ResolvedBlocks (blocks
        # arrive materialized); use the pipeline variant that starts at the
        # batching stage.
        return _MaterializedBatchIterator(ref_bundles_iter, **kwargs)

    def _on_iteration_end(self, executor) -> None:
        """Runs on the consumer thread from _iter_batches' finally.

        Covers normal exhaustion, early ``break``, and exceptions —
        gen_bundles' own cleanup would be GC-delayed on early break (it runs
        on the fetch thread; see StreamSplitDataIterator._on_iteration_end).
        """
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
