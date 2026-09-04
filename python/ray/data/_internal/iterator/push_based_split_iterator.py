"""PROTOTYPE: push-based streaming_split.

Inverts the pull model of ``stream_split_iterator.py``: instead of each
consumer calling ``coordinator.get.remote(...)`` per block, the coordinator
runs one pusher thread per split that pulls bundles from the streaming
executor's output iterator and pushes them to the consumer actor. The
consumer can be ANY Ray actor (e.g. a Ray Train worker): delivery goes
through the actor's built-in ``__ray_call__`` into a process-local receiver
registry, so the actor class needs no new methods. Bundles are pushed with
their block refs NESTED (not materialized), and ``PushBasedDataIterator``
drains the receiver queue through the standard DataIterator pipeline
(prefetch -> resolve -> batch -> collate, including iter_torch_batches).

Flow control is poll-based: each pusher periodically polls the consumer to
learn how many rows it has drained from its queue and how many rows it wants
buffered (``target_buffer_rows``), and only pushes while
``rows_pushed - rows_consumed < target_buffer_rows``.

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
     data plane        |                |  flow control (per push burst)
     (fire-and-forget) |                |
       __ray_call__(   |                |  (1) __ray_call__(_receiver_poll)
         _receiver_    |                |      -> rows_consumed,
         deliver, key, |                |      target_buffer_rows, iterating
         epoch, seq,   |                |  (2) credit = target_buffer_rows
         bundle w/     |                |        - (rows_pushed - rows_consumed)
         nested refs)  |                |  (3) credit > 0: push bundles;
       EOF: same, seq'd|                |      else sleep POLL_INTERVAL_S,
       errors unseq'd  |                |      re-poll
                       v                |
    +--------------------------------------------------------------------+
    |      consumer actor i (any Ray actor, e.g. a Ray Train worker)     |
    |                                                                    |
    |  actor task thread(s): __ray_call__ deliveries -> reorder buffer   |
    |      (by seq) -> _PushReceiver.queue (bundles w/ nested refs);     |
    |      errors jump the queue; polls read the receiver counters       |
    |  iteration thread (e.g. Train's ThreadRunner):                     |
    |      PushBasedDataIterator: register(current_actor) ->             |
    |      start_epoch (barrier RPC, all n splits sync) ->               |
    |      pop queue -> standard DataIterator pipeline                   |
    |      (prefetch -> resolve/ray.get -> batch -> collate)             |
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

Not implemented (prototype): stats/metrics export, byte-based credit,
locality-aware pushing, replacing a dead consumer mid-run.
"""

import logging
import queue
import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Set, Tuple

import ray
from ray.data._internal.execution.interfaces import RefBundle
from ray.data._internal.stats import DatasetStats
from ray.data.context import DataContext
from ray.data.iterator import DataIterator
from ray.util.debug import log_once

if TYPE_CHECKING:
    from ray.data.dataset import Dataset, Schema

logger = logging.getLogger(__name__)

BLOCKED_CLIENT_WARN_TIMEOUT = 30


@dataclass
class _PollResponse:
    """Consumer state reported to the coordinator's pusher thread."""

    rows_consumed: int
    bytes_consumed: int
    target_buffer_rows: int
    iterating: bool


@dataclass
class _EndOfEpoch:
    epoch_id: int


@dataclass
class _ExecutorError:
    error: Exception


@ray.remote(num_cpus=0)
class PushSplitCoordinator:
    """Coordinator actor that pushes split output to registered consumers.

    Runs a streaming executor locally (one per epoch, like SplitCoordinator)
    plus one pusher thread per split that pulls from the executor's output
    iterator and pushes to that split's consumer, gated by poll-based credit.
    """

    # How long a pusher sleeps between polls while the consumer has no credit.
    POLL_INTERVAL_S = 0.05

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
        self._consumers: Dict[int, Any] = {}
        self._pusher_threads: List[threading.Thread] = []
        self._pusher_stop_events: Dict[int, threading.Event] = {}

        # Push accounting, deliberately lock-free:
        # - Each per-split entry has a single writer: its own pusher thread
        #   (_bytes_consumed_reported is additionally written once, benignly
        #   last-writer-wins, by _finish_split when zeroing a finished
        #   split's contribution).
        # - Cross-thread readers only compute the heuristic sum in
        #   _update_external_consumer_bytes, where GIL-atomic int stores and
        #   slight staleness across splits are acceptable.
        # - Keys are fixed (dict.fromkeys at reset), so dict iteration never
        #   races a resize; _reset_state reassigns only after the old
        #   pushers have been joined by _teardown_epoch.
        self._rows_pushed: Dict[int, int] = dict.fromkeys(range(n), 0)
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

        The consumer is ANY Ray actor (e.g. a Ray Train worker); delivery
        goes through the actor's built-in ``__ray_call__`` into the
        process-local ``_RECEIVER_REGISTRY`` under ``key``, and bundles are
        pushed with their refs NESTED (not materialized) so the standard
        DataIterator prefetch/resolve pipeline handles them.
        Re-registering the same split is allowed (e.g. once per epoch).
        """
        with self._lock:
            self._consumers[split_idx] = (consumer, key)
        logger.debug(f"Registered consumer for split {split_idx}.")

    def start_epoch(self, split_idx: int) -> int:
        """Barrier: blocks until all n splits arrive, then starts the epoch."""
        return self._barrier(split_idx)

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
            # Gates that the epoch is started exactly once.
            if self._cur_epoch == starting_epoch:
                self._reset_state()
                self._cur_epoch += 1
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
                    # TODO(push-split): external-consumer-bytes reporting is
                    # disabled for now — see _update_external_consumer_bytes.
                    # Registering with 0 and never updating would stall
                    # DownstreamCapacityBackpressurePolicy, so the
                    # registration is commented out together with the updates.
                    # self._current_executor.set_external_consumer_bytes(0)
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
        self._rows_pushed = dict.fromkeys(range(self._n), 0)
        self._bytes_pushed = dict.fromkeys(range(self._n), 0)
        self._bytes_consumed_reported = dict.fromkeys(range(self._n), 0)

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
        """Build (poll, push_block, push_eof, push_error) for a consumer.

        Targets any actor via __ray_call__ + the process-local receiver
        registry; blocks stay as NESTED refs inside a one-block RefBundle,
        so the consumer's standard prefetch/resolve pipeline handles them.
        """
        consumer, key = self._consumers[split_idx]

        def poll():
            return ray.get(
                consumer.__ray_call__.remote(_receiver_poll, key), timeout=30
            )

        def push_block(seq, entry, schema, num_rows, size_bytes):
            sub_bundle = RefBundle(blocks=(entry,), owns_blocks=False, schema=schema)
            consumer.__ray_call__.remote(
                _receiver_deliver,
                key,
                epoch_id,
                seq,
                (sub_bundle, num_rows, size_bytes),
            )

        def push_eof(seq):
            consumer.__ray_call__.remote(
                _receiver_deliver, key, epoch_id, seq, _EndOfEpoch(epoch_id)
            )

        def push_error(error):
            consumer.__ray_call__.remote(_receiver_deliver_error, key, epoch_id, error)

        return poll, push_block, push_eof, push_error

    def _pusher_loop(
        self, epoch_id: int, split_idx: int, stop: threading.Event
    ) -> None:
        poll, push_block, push_eof, push_error = self._make_consumer_ops(
            epoch_id, split_idx
        )
        output_iterator = self._output_iterator
        consumer_dead = False
        # Per-split push sequence number. The consumer may be a multi-threaded
        # actor, so Ray may execute receive tasks out of order (dispatch
        # follows argument readiness); the consumer re-sequences by ``seq``.
        seq = 0
        try:
            while not stop.is_set():
                # 1) Poll the consumer.
                try:
                    resp: _PollResponse = poll()
                except Exception as e:
                    # TODO(push-split): support replacing a dead consumer.
                    # Today the pusher drains and exits; instead it could
                    # park on the stop event (a waiting thread is cheap) and
                    # resume pushing when a replacement train worker
                    # re-registers for this split_idx.
                    logger.warning(
                        f"Split {split_idx} epoch {epoch_id}: consumer poll "
                        f"failed ({e}); treating consumer as dead."
                    )
                    consumer_dead = True
                    break

                self._bytes_consumed_reported[split_idx] = resp.bytes_consumed
                rows_in_flight = self._rows_pushed[split_idx] - resp.rows_consumed
                # TODO(push-split): disabled; see _update_external_consumer_bytes.
                # self._update_external_consumer_bytes()

                # 2) Compute credit. Our push counters are exact; the polled
                # consumed count is stale-low, so credit only under-sends.
                credit_rows = resp.target_buffer_rows - rows_in_flight
                if not resp.iterating or credit_rows <= 0:
                    stop.wait(self.POLL_INTERVAL_S)
                    continue

                # 3) Push burst: at most credit_rows rows, then re-poll.
                while credit_rows > 0 and not stop.is_set():
                    # Blocking pull from the executor; bumps
                    # _num_waiting_consumers, preserving liveness/backpressure
                    # semantics of the pull model. Raises StopIteration at
                    # end of stream.
                    bundle = output_iterator.get_next(split_idx)
                    for entry in bundle.blocks:
                        num_rows = (
                            entry.metadata.num_rows
                            if entry.metadata.num_rows is not None
                            else 1
                        )
                        size_bytes = entry.metadata.size_bytes or 0
                        push_block(seq, entry, bundle.schema, num_rows, size_bytes)
                        seq += 1
                        credit_rows -= num_rows
                        # Single-writer (this thread); see __init__.
                        self._rows_pushed[split_idx] += num_rows
                        self._bytes_pushed[split_idx] += size_bytes
                    # TODO(push-split): disabled; see
                    # _update_external_consumer_bytes.
                    # self._update_external_consumer_bytes()
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
        finally:
            if consumer_dead:
                # Drain this split's stream so its bundles don't pin memory
                # and sibling splits can finish, then mark it done.
                self._drain_split(split_idx, stop)
                self._finish_split(epoch_id, split_idx)

    def _drain_split(self, split_idx: int, stop: threading.Event) -> None:
        try:
            while not stop.is_set():
                self._output_iterator.get_next(split_idx)
        except Exception:
            pass

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
        # TODO(push-split): disabled; see _update_external_consumer_bytes.
        # self._update_external_consumer_bytes()
        # Shut down outside the lock (joins the scheduling thread).
        if executor_to_shutdown is not None:
            logger.debug(
                f"All splits finished epoch {epoch_id}; shutting down executor."
            )
            executor_to_shutdown.shutdown(force=True)

    def _update_external_consumer_bytes(self) -> None:
        """Report bytes in flight + buffered at consumers to the executor.

        Push-model analog of SplitCoordinator._report_prefetched_bytes_to_executor;
        would keep DownstreamCapacityBackpressurePolicy working.

        TODO(push-split): CURRENTLY DISABLED — every call site (and the
        set_external_consumer_bytes(0) registration in _try_start_new_epoch)
        is commented out to check whether this feed is needed at all under
        push: the pushers already gate consumption with credit and only block
        in get_next when a consumer wants data, so the executor's natural
        output-queue backpressure may suffice. Re-enable (registration +
        call sites together) if slow-consumer runs show unbounded producer
        run-ahead.
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
# Generic-consumer support: lets the coordinator push to ANY Ray actor (e.g.
# a Ray Train worker) without that actor's class defining receive methods.
# Delivery goes through the actor's built-in ``__ray_call__`` into a
# process-local receiver registry; the functions below run ON the consumer
# actor (their first argument is the actor instance, unused).
# ---------------------------------------------------------------------------

# Default consumer-side buffer target for the generic path. Must cover at
# least ~2 blocks or the credit protocol degenerates to stop-and-wait; a
# byte-based credit sized against target_max_block_size would be the real fix.
DEFAULT_GENERIC_TARGET_BUFFER_ROWS = 25_000

_RECEIVER_REGISTRY_LOCK = threading.Lock()
_RECEIVER_REGISTRY: Dict[str, "_PushReceiver"] = {}


class _PushReceiver:
    """Process-local receive state for one (coordinator, split_idx) pair.

    Deliveries land on the hosting actor's task thread(s); the iterator
    drains ``queue`` from its own (fetch) thread — hence the lock.
    """

    def __init__(self, target_buffer_rows: int):
        self.queue: "queue.Queue" = queue.Queue()
        self.lock = threading.Lock()
        self.target_buffer_rows = target_buffer_rows
        self.rows_consumed = 0
        self.bytes_consumed = 0
        self.iterating = False
        self.cur_epoch: Optional[int] = None
        self.reorder_epoch: Optional[int] = None
        self.reorder_next_seq = 0
        self.reorder_pending: Dict[int, Any] = {}

    def reset(self, target_buffer_rows: int) -> None:
        """Reset before re-arriving at the epoch barrier."""
        with self.lock:
            self.cur_epoch = None
            self.iterating = False
            self.target_buffer_rows = target_buffer_rows
            self.rows_consumed = 0
            self.bytes_consumed = 0
            self.reorder_epoch = None
            self.reorder_next_seq = 0
            self.reorder_pending = {}
            while True:
                try:
                    self.queue.get_nowait()
                except queue.Empty:
                    break

    def begin_epoch(self, epoch: int) -> None:
        with self.lock:
            # cur_epoch is set together with iterating; the pusher only
            # pushes after observing iterating=True in a poll.
            self.cur_epoch = epoch
            self.iterating = True

    def end_epoch(self) -> None:
        with self.lock:
            self.iterating = False

    def record_consumed(self, num_rows: int, size_bytes: int) -> None:
        with self.lock:
            self.rows_consumed += num_rows
            self.bytes_consumed += size_bytes


def _receiver_deliver(_actor: Any, key: str, epoch_id: int, seq: int, item: Any):
    """Deliver one sequenced item (bundle tuple or _EndOfEpoch) in order."""
    receiver = _RECEIVER_REGISTRY.get(key)
    if receiver is None:
        return
    with receiver.lock:
        if epoch_id != receiver.cur_epoch:
            return
        if receiver.reorder_epoch != epoch_id:
            receiver.reorder_epoch = epoch_id
            receiver.reorder_next_seq = 0
            receiver.reorder_pending = {}
        receiver.reorder_pending[seq] = item
        while receiver.reorder_next_seq in receiver.reorder_pending:
            receiver.queue.put(receiver.reorder_pending.pop(receiver.reorder_next_seq))
            receiver.reorder_next_seq += 1


def _receiver_deliver_error(_actor: Any, key: str, epoch_id: int, error: Any):
    """Deliver an _ExecutorError immediately (fail fast, unsequenced)."""
    receiver = _RECEIVER_REGISTRY.get(key)
    if receiver is None:
        return
    with receiver.lock:
        if epoch_id == receiver.cur_epoch:
            receiver.queue.put(error)


def _receiver_poll(_actor: Any, key: str) -> _PollResponse:
    receiver = _RECEIVER_REGISTRY.get(key)
    if receiver is None:
        # Not created yet (iterator hasn't started): report not-iterating so
        # the pusher waits.
        return _PollResponse(0, 0, 0, False)
    with receiver.lock:
        return _PollResponse(
            rows_consumed=receiver.rows_consumed,
            bytes_consumed=receiver.bytes_consumed,
            target_buffer_rows=receiver.target_buffer_rows,
            iterating=receiver.iterating,
        )


class PushBasedDataIterator(DataIterator):
    """PROTOTYPE: DataIterator over one split of a push-based streaming split.

    Picklable, and can be shipped into ANY Ray actor — e.g. a Ray Train
    worker. At iteration time it registers the hosting actor's own handle
    (``ray.get_runtime_context().current_actor``) with the coordinator, and
    the coordinator delivers bundles via the actor's built-in
    ``__ray_call__`` into the process-local receiver registry. Bundles carry
    NESTED block refs, so the standard DataIterator pipeline
    (prefetch -> resolve -> batch -> collate, including iter_torch_batches)
    is reused unchanged.
    """

    @staticmethod
    def create(
        split_dataset: "Dataset",
        n: int,
        target_buffer_rows: Optional[int] = None,
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
        return [
            PushBasedDataIterator(coord_actor, i, n, target_buffer_rows)
            for i in range(n)
        ]

    def __init__(
        self,
        coord_actor: ray.actor.ActorHandle,
        output_split_idx: int,
        world_size: int,
        target_buffer_rows: Optional[int] = None,
    ):
        self._coord_actor = coord_actor
        self._output_split_idx = output_split_idx
        self._world_size = world_size
        self._target_buffer_rows = (
            target_buffer_rows or DEFAULT_GENERIC_TARGET_BUFFER_ROWS
        )
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
    ) -> Tuple[Iterator[RefBundle], Optional[DatasetStats], None]:
        def gen_bundles() -> Iterator[RefBundle]:
            try:
                self_handle = ray.get_runtime_context().current_actor
                assert self_handle is not None
            except Exception as e:
                raise RuntimeError(
                    "PushBasedDataIterator must be iterated from inside a Ray "
                    "actor (e.g. a Ray Train worker): the coordinator pushes "
                    "blocks to the hosting actor via __ray_call__."
                ) from e

            key = self._receiver_key()
            with _RECEIVER_REGISTRY_LOCK:
                receiver = _RECEIVER_REGISTRY.get(key)
                if receiver is None:
                    receiver = _PushReceiver(self._target_buffer_rows)
                    _RECEIVER_REGISTRY[key] = receiver
            # Reset from any previous epoch BEFORE arriving at the barrier;
            # stragglers are dropped by the epoch check in _receiver_deliver.
            receiver.reset(self._target_buffer_rows)

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

            while True:
                try:
                    item = receiver.queue.get(timeout=1.0)
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
                bundle, num_rows, size_bytes = item
                receiver.record_consumed(num_rows, size_bytes)
                yield bundle

        return gen_bundles(), self._iter_stats, None

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
        receiver = _RECEIVER_REGISTRY.get(self._receiver_key())
        if receiver is not None:
            receiver.end_epoch()
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
