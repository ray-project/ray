"""PROTOTYPE: push-based streaming_split.

Inverts the pull model of ``stream_split_iterator.py``: instead of each
consumer calling ``coordinator.get.remote(...)`` per block, the coordinator
runs one pusher thread per split that pulls bundles from the streaming
executor's output iterator and pushes the blocks to a registered consumer
actor via ``consumer.receive_block.remote(...)``. The consumer buffers
received blocks in a local queue and iterates batches from it.

Flow control is poll-based: each pusher periodically calls
``consumer.poll.remote()`` to learn how many rows the consumer has drained
from its queue and how many rows it wants buffered (``target_buffer_rows``),
and only pushes while ``rows_pushed - rows_consumed < target_buffer_rows``.

Liveness note: the pusher threads block inside
``output_iterator.get_next(split_idx)`` (ultimately
``OpState.get_output_blocking``) exactly like today's pull consumers, so the
executor's ``_num_waiting_consumers`` / ``OutputBackpressureGuard`` machinery
keeps working unchanged.

Not implemented (prototype): stats/metrics export, collate/finalize fns,
preserve_order, locality-aware pushing, consumer re-registration.
"""

import logging
import queue
import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, Iterator, List, Optional, Set

import ray
from ray.data._internal.block_batching.interfaces import ResolvedBlock
from ray.data._internal.block_batching.util import blocks_to_batches, format_batches
from ray.data._internal.execution.interfaces import NodeIdStr
from ray.data.block import Block, DataBatch
from ray.util.debug import log_once

if TYPE_CHECKING:
    from ray.data.dataset import Dataset

logger = logging.getLogger(__name__)

BLOCKED_CLIENT_WARN_TIMEOUT = 30

# Concurrency groups a consumer actor must declare, e.g.:
#   @ray.remote(num_cpus=0, concurrency_groups=PUSH_CONSUMER_CONCURRENCY_GROUPS)
#   class MyConsumer(PushSplitConsumer): ...
# "data": receive_block/receive_eof/receive_error — single thread so the
#   pusher's calls execute in submission order.
# "flow": poll — isolated so flow control is never head-of-line blocked
#   behind a large block deserialization queued in "data".
# "consume": the caller-defined long-running iteration method.
PUSH_CONSUMER_CONCURRENCY_GROUPS = {"data": 1, "flow": 1, "consume": 1}


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


def create_push_split(
    base_dataset: "Dataset",
    n: int,
    *,
    equal: bool = False,
    locality_hints: Optional[List[NodeIdStr]] = None,
) -> ray.actor.ActorHandle:
    """Create a push-based split coordinator for ``base_dataset``.

    Replicates the split-dataset construction of ``Dataset.streaming_split``
    (see dataset.py) without touching it, then creates the coordinator actor
    pinned to the calling node. The caller is responsible for creating ``n``
    consumer actors (subclasses of ``PushSplitConsumer``), whose constructors
    register themselves with the returned coordinator.
    """
    from ray.data._internal.logical.interfaces import LogicalPlan
    from ray.data._internal.logical.operators.streaming_split_operator import (
        StreamingSplit,
    )
    from ray.data.dataset import Dataset

    op = StreamingSplit(
        num_splits=n,
        equal=equal,
        input_dependencies=[base_dataset._logical_plan.dag],
        locality_hints=locality_hints,
    )
    logical_plan = LogicalPlan(op, base_dataset.context)
    split_dataset = Dataset._from_parent(base_dataset, logical_plan)
    split_dataset._set_uuid(base_dataset._uuid)

    coord_actor = PushSplitCoordinator.options(
        # n concurrent start_epoch calls blocked at the barrier + headroom
        # for register/notify/poll-side calls. Pusher threads are plain
        # threading.Threads and do not occupy actor concurrency slots.
        max_concurrency=n + 2,
        label_selector={
            ray._raylet.RAY_NODE_ID_KEY: ray.get_runtime_context().get_node_id()
        },
    ).remote(split_dataset, n)
    return coord_actor


class PushSplitConsumer:
    """Base class for actors that receive pushed blocks and iterate batches.

    Subclass it and decorate the subclass with::

        @ray.remote(num_cpus=0, concurrency_groups=PUSH_CONSUMER_CONCURRENCY_GROUPS)

    The subclass's long-running iteration method should be decorated with
    ``@ray.method(concurrency_group="consume")`` and call
    ``self.iter_batches(...)`` locally.
    """

    def __init__(
        self,
        coord_actor: ray.actor.ActorHandle,
        split_idx: int,
        target_buffer_rows: Optional[int] = None,
    ):
        self._coord_actor = coord_actor
        self._split_idx = split_idx
        # Resolved lazily in iter_batches (default: 4 * batch_size).
        self._target_buffer_rows = target_buffer_rows
        self._effective_target_rows = target_buffer_rows or 0

        # Blocks pushed by the coordinator. Unbounded on purpose: boundedness
        # comes from the credit protocol (<= target_buffer_rows + one block of
        # overshoot), so receive_block never blocks the "data" thread.
        self._queue: "queue.Queue" = queue.Queue()

        # Written by the "consume" thread, read by the "flow" thread.
        self._counter_lock = threading.Lock()
        self._rows_consumed = 0
        self._bytes_consumed = 0
        self._queued_rows = 0
        self._queued_bytes = 0
        self._peak_queue_rows = 0
        self._peak_queue_bytes = 0

        self._iterating = False
        self._cur_epoch: Optional[int] = None

        ray.get(
            coord_actor.register.remote(
                split_idx, ray.get_runtime_context().current_actor
            )
        )

    # ------------------------------------------------------------------
    # Called by the coordinator's pusher thread.
    # ------------------------------------------------------------------

    @ray.method(concurrency_group="data")
    def receive_block(
        self, epoch_id: int, block: Block, num_rows: int, size_bytes: int
    ) -> None:
        """Receive one materialized block.

        The pusher passes the block's ObjectRef as a top-level task arg, so
        Ray resolves it and this method receives the materialized ``Block``
        (zero-copy for Arrow blocks in the local object store).
        """
        if epoch_id != self._cur_epoch:
            # Straggler push from a dead epoch; drop it.
            return
        with self._counter_lock:
            self._queued_rows += num_rows
            self._queued_bytes += size_bytes
            self._peak_queue_rows = max(self._peak_queue_rows, self._queued_rows)
            self._peak_queue_bytes = max(self._peak_queue_bytes, self._queued_bytes)
        self._queue.put((block, num_rows, size_bytes))

    @ray.method(concurrency_group="data")
    def receive_eof(self, epoch_id: int) -> None:
        if epoch_id == self._cur_epoch:
            self._queue.put(_EndOfEpoch(epoch_id))

    @ray.method(concurrency_group="data")
    def receive_error(self, epoch_id: int, error: "_ExecutorError") -> None:
        # ``error`` arrives wrapped in _ExecutorError: passing a bare
        # RayTaskError as a task arg would make Ray treat it as a failed
        # dependency and fail this task instead of executing it.
        if epoch_id == self._cur_epoch:
            self._queue.put(error)

    @ray.method(concurrency_group="flow")
    def poll(self) -> _PollResponse:
        with self._counter_lock:
            return _PollResponse(
                rows_consumed=self._rows_consumed,
                bytes_consumed=self._bytes_consumed,
                target_buffer_rows=self._effective_target_rows,
                iterating=self._iterating,
            )

    @ray.method(concurrency_group="flow")
    def debug_stats(self) -> Dict[str, int]:
        with self._counter_lock:
            return {
                "rows_consumed": self._rows_consumed,
                "bytes_consumed": self._bytes_consumed,
                "queued_rows": self._queued_rows,
                "peak_queue_rows": self._peak_queue_rows,
                "peak_queue_bytes": self._peak_queue_bytes,
            }

    # ------------------------------------------------------------------
    # Local iteration (runs on the "consume" thread via a subclass method).
    # ------------------------------------------------------------------

    def _gen_blocks(self) -> Iterator[ResolvedBlock]:
        # Reset state from any previous epoch before arriving at the barrier.
        # Stragglers that arrive between the drain and the epoch bump are
        # dropped by receive_block's epoch check (_cur_epoch is None here).
        self._cur_epoch = None
        while True:
            try:
                self._queue.get_nowait()
            except queue.Empty:
                break
        with self._counter_lock:
            self._rows_consumed = 0
            self._bytes_consumed = 0
            self._queued_rows = 0
            self._queued_bytes = 0

        epoch = ray.get(self._coord_actor.start_epoch.remote(self._split_idx))
        # Order matters: set _cur_epoch BEFORE _iterating. The pusher only
        # starts sending once it observes iterating=True in a poll, so no
        # block can arrive while _cur_epoch is stale.
        self._cur_epoch = epoch
        self._iterating = True
        try:
            while True:
                try:
                    item = self._queue.get(timeout=1.0)
                except queue.Empty:
                    # Loop (rather than block forever) so a hung coordinator
                    # is debuggable via thread dumps / logs.
                    continue
                if isinstance(item, _EndOfEpoch):
                    logger.debug(f"Split {self._split_idx}: epoch {epoch} exhausted.")
                    return
                if isinstance(item, _ExecutorError):
                    raise item.error
                block, num_rows, size_bytes = item
                with self._counter_lock:
                    self._rows_consumed += num_rows
                    self._bytes_consumed += size_bytes
                    self._queued_rows -= num_rows
                    self._queued_bytes -= size_bytes
                yield ResolvedBlock(block=block)
        finally:
            self._iterating = False
            self._coord_actor.notify_split_finished.remote(epoch, self._split_idx)

    def iter_batches(
        self,
        *,
        batch_size: Optional[int] = 256,
        batch_format: Optional[str] = "default",
        drop_last: bool = False,
        local_shuffle_buffer_size: Optional[int] = None,
        local_shuffle_seed: Optional[int] = None,
    ) -> Iterator[DataBatch]:
        """Iterate formatted batches for one epoch (call again for the next).

        Must be called from the "consume" concurrency group. Prefetch/resolve
        stages of the pull pipeline are intentionally absent: blocks arrive
        already materialized via receive_block. Stats plumbing is a prototype
        non-goal (stats=None below); a real implementation would thread a
        DatasetStats through and report to _StatsManager.
        """
        if self._target_buffer_rows is not None:
            self._effective_target_rows = self._target_buffer_rows
        else:
            self._effective_target_rows = 4 * (batch_size or 256)

        blocks = self._gen_blocks()
        try:
            batches = blocks_to_batches(
                blocks,
                stats=None,
                batch_size=batch_size,
                drop_last=drop_last,
                shuffle_buffer_min_size=local_shuffle_buffer_size,
                shuffle_seed=local_shuffle_seed,
            )
            for batch in format_batches(batches, batch_format=batch_format, stats=None):
                yield batch.data
        finally:
            # Ensure _gen_blocks' finally (notify_split_finished) runs
            # promptly on early break instead of at GC time.
            blocks.close()


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

        self._consumers: Dict[int, ray.actor.ActorHandle] = {}
        self._pusher_threads: List[threading.Thread] = []
        self._pusher_stop_events: Dict[int, threading.Event] = {}

        # Push accounting. Guarded by _push_lock ONLY (never nest _lock
        # inside a pusher thread — pushers must stay joinable while _lock is
        # held during epoch transitions).
        self._push_lock = threading.Lock()
        self._rows_pushed: Dict[int, int] = dict.fromkeys(range(n), 0)
        self._bytes_pushed: Dict[int, int] = dict.fromkeys(range(n), 0)
        self._bytes_consumed_reported: Dict[int, int] = dict.fromkeys(range(n), 0)

        logger.debug(f"PushSplitCoordinator created: {n=}")

    # ------------------------------------------------------------------
    # Control plane (actor tasks).
    # ------------------------------------------------------------------

    def register(self, split_idx: int, consumer: ray.actor.ActorHandle) -> None:
        with self._lock:
            self._consumers[split_idx] = consumer
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
            # Done outside self._lock so pushers can still take _push_lock /
            # finish in-flight bookkeeping while we join them.
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
                        f"PushSplitConsumer(epoch={starting_epoch}, "
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
        with self._push_lock:
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

    def _pusher_loop(
        self, epoch_id: int, split_idx: int, stop: threading.Event
    ) -> None:
        consumer = self._consumers[split_idx]
        output_iterator = self._output_iterator
        consumer_dead = False
        try:
            while not stop.is_set():
                # 1) Poll the consumer.
                try:
                    resp: _PollResponse = ray.get(consumer.poll.remote(), timeout=30)
                except Exception as e:
                    logger.warning(
                        f"Split {split_idx} epoch {epoch_id}: consumer poll "
                        f"failed ({e}); treating consumer as dead."
                    )
                    consumer_dead = True
                    break

                with self._push_lock:
                    self._bytes_consumed_reported[split_idx] = resp.bytes_consumed
                    rows_in_flight = self._rows_pushed[split_idx] - resp.rows_consumed
                self._update_external_consumer_bytes()

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
                        # Top-level ref arg: Ray resolves it, so the consumer
                        # receives the materialized Block.
                        consumer.receive_block.remote(
                            epoch_id, entry.ref, num_rows, size_bytes
                        )
                        credit_rows -= num_rows
                        with self._push_lock:
                            self._rows_pushed[split_idx] += num_rows
                            self._bytes_pushed[split_idx] += size_bytes
                    self._update_external_consumer_bytes()
        except StopIteration:
            if not stop.is_set():
                logger.debug(
                    f"Split {split_idx} epoch {epoch_id} exhausted; sending EOF."
                )
                consumer.receive_eof.remote(epoch_id)
            return
        except Exception as e:
            if not stop.is_set():
                logger.warning(f"Split {split_idx} epoch {epoch_id} pusher failed: {e}")
                try:
                    consumer.receive_error.remote(epoch_id, _ExecutorError(e))
                except Exception:
                    # e.g. unpicklable exception.
                    consumer.receive_error.remote(
                        epoch_id, _ExecutorError(RuntimeError(repr(e)))
                    )
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
            with self._push_lock:
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
        """Report bytes in flight + buffered at consumers to the executor.

        Push-model analog of SplitCoordinator._report_prefetched_bytes_to_executor;
        keeps DownstreamCapacityBackpressurePolicy working.
        """
        executor = self._current_executor
        if executor is None:
            return
        with self._push_lock:
            total = sum(
                max(0, self._bytes_pushed[i] - self._bytes_consumed_reported[i])
                for i in range(self._n)
            )
        try:
            executor.set_external_consumer_bytes(total)
        except Exception:
            # The executor may be mid-shutdown during an epoch transition.
            pass
