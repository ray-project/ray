"""Coordinator actor for the push-based streaming_split.

The coordinator runs a streaming executor over a ``StreamingSplit`` dataset,
recreated every epoch behind a barrier that all ``n`` splits must reach. It
tracks a row-based prefetch window per split: each consumer declares a
``target_rows`` window and reports what it consumes, and the coordinator
computes how many more rows the split may be sent. Bytes that are sent but
not yet consumed feed the executor's external-consumer backpressure, so a
fast producer can't run far ahead of slow consumers.

One pusher thread per split sends that split's blocks, by value and
sequence-numbered, to the registered consumer actor while its row window
has room (see ``push_based_split_iterator.py`` for the consumer side).
"""

import logging
import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, List, Optional, Set, Tuple, Union

import ray
from ray.data.context import DataContext
from ray.util.debug import log_once

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces import NodeIdStr
    from ray.data.dataset import Dataset

logger = logging.getLogger(__name__)

BLOCKED_CLIENT_WARN_TIMEOUT = 30


@dataclass
class _BlockPush:
    """Wire header for one pushed block; the block itself travels by value
    as a resolved top-level task arg."""

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


def _create_split_dataset(
    dataset: "Dataset",
    n: int,
    *,
    equal: bool = False,
    locality_hints: Optional[List["NodeIdStr"]] = None,
) -> "Dataset":
    """Wrap ``dataset`` in a ``StreamingSplit`` logical op, as
    :meth:`Dataset.streaming_split` does."""
    from ray.data._internal.logical.interfaces import LogicalPlan
    from ray.data._internal.logical.operators import StreamingSplit
    from ray.data.dataset import Dataset

    op = StreamingSplit(
        num_splits=n,
        equal=equal,
        input_dependencies=[dataset._logical_plan.dag],
        locality_hints=locality_hints,
    )
    split_dataset = Dataset._from_parent(dataset, LogicalPlan(op, dataset.context))
    split_dataset._set_uuid(dataset._uuid)
    return split_dataset


class _SplitFlow:
    """Row-window flow control and pacing counters for one split and epoch.

    The consumer declares ``target_rows`` and reports consumption; senders
    may send while ``target_rows - (rows_pushed - rows_consumed) > 0``.
    Whole blocks are sent, so one send may overshoot the window, which only
    delays the next send until consumption catches up.
    """

    def __init__(self):
        self.cond = threading.Condition()
        self.target_rows = 0
        self.rows_pushed = 0
        self.rows_consumed = 0
        self.bytes_pushed = 0
        self.bytes_consumed = 0
        # Observability (written by the split's pusher): time idle because
        # the window was full (consumer-bound) vs. blocked on the executor's
        # output (producer-bound).
        self.blocks_pushed = 0
        self.wait_demand_s = 0.0
        self.wait_output_s = 0.0

    def report(self, target_rows: int, consumed_rows: int, consumed_bytes: int):
        with self.cond:
            self.target_rows = target_rows
            self.rows_consumed += consumed_rows
            self.bytes_consumed += consumed_bytes
            self.cond.notify()

    def rows_to_send(self) -> int:
        with self.cond:
            return self.target_rows - (self.rows_pushed - self.rows_consumed)

    def wait_for_room(self, timeout: float) -> bool:
        """Block until the window has room (or ``timeout``); True if it has."""
        with self.cond:
            if self.rows_to_send() > 0:
                return True
            self.cond.wait(timeout)
            return self.rows_to_send() > 0

    def record_push(self, num_rows: int, size_bytes: int) -> None:
        with self.cond:
            self.rows_pushed += num_rows
            self.bytes_pushed += size_bytes
            self.blocks_pushed += 1

    def finish(self) -> None:
        """Drop this split's contribution to flow and pacing state."""
        with self.cond:
            self.rows_consumed = self.rows_pushed
            self.bytes_consumed = self.bytes_pushed
            self.target_rows = 0

    def in_flight_bytes(self) -> int:
        with self.cond:
            return max(0, self.bytes_pushed - self.bytes_consumed)


@ray.remote(num_cpus=0)
class PushSplitCoordinator:
    """Coordinator actor for a push-based streaming split.

    Runs the streaming executor (one per epoch) plus one pusher thread per
    split, gated by that split's row window.
    """

    # How often a pusher waiting for room re-checks its stop event; reports
    # wake it immediately.
    DEMAND_WAIT_TIMEOUT_S = 0.5

    def __init__(self, dataset: "Dataset", n: int):
        self._data_context = dataset.context.copy()
        ray.data.DataContext._set_current(self._data_context)

        self._base_dataset = dataset
        self._n = n

        # Guards epoch/barrier/finished-splits state.
        self._lock = threading.RLock()
        # Barrier waiters sleep on this until the epoch can start.
        self._barrier_cond = threading.Condition(self._lock)
        self._dataset_state_lock = threading.Lock()
        self._schema = None

        self._current_executor = None
        self._output_iterator = None
        self._cur_epoch = -1
        self._num_unarrived_splits_at_barrier = n
        # Barrier waiters wait until the last arrival's teardown completes.
        self._teardown_complete_for: Optional[int] = None
        self._finished_splits: Set[int] = set()
        self._gen_epoch_error: Optional[Exception] = None

        # split_idx -> (handle, key); see register().
        self._consumers: Dict[int, Tuple[ray.actor.ActorHandle, str]] = {}
        self._pusher_threads: List[threading.Thread] = []
        self._pusher_stop_events: Dict[int, threading.Event] = {}

        # Recreated every epoch, so a stale report can only land on the
        # previous epoch's discarded state.
        self._flows: Dict[int, _SplitFlow] = {i: _SplitFlow() for i in range(n)}
        # Serializes compute-and-report of external consumer bytes, so a
        # stale total can't overwrite a newer one.
        self._pacing_lock = threading.Lock()

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
        self._check_split_idx(split_idx)
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
        self._check_split_idx(split_idx)
        return self._barrier(split_idx)

    def request_rows(
        self,
        split_idx: int,
        epoch_id: int,
        target_rows: int,
        consumed_rows: int,
        consumed_bytes: int,
    ) -> None:
        """Fire-and-forget consumer report: "my prefetch window is
        ``target_rows`` rows, and I consumed ``consumed_rows`` more rows
        (``consumed_bytes`` bytes)".

        Sent once at iteration start and once per consumed block. Reports
        for any epoch other than the current one are ignored.
        """
        flow = self._flows[split_idx]
        with flow.cond:
            # Checked under the cond: the epoch bump precedes replacing the
            # flows, so a report either sees the new epoch and is dropped, or
            # lands on the old epoch's discarded state.
            if epoch_id != self._cur_epoch:
                return
            flow.report(target_rows, consumed_rows, consumed_bytes)

    def notify_split_finished(self, epoch_id: int, split_idx: int) -> None:
        """Consumer stopped iterating ``epoch_id``; stale epochs are ignored."""
        with self._lock:
            if epoch_id != self._cur_epoch:
                return
            stop_event = self._pusher_stop_events.get(split_idx)
        if stop_event is not None:
            stop_event.set()
        self._finish_split(epoch_id, split_idx)

    def debug_state(self) -> Dict[str, Dict[int, int]]:
        """Snapshot of per-split flow-control state, for debugging/tests."""
        flows = self._flows
        return {
            "target_rows": {i: f.target_rows for i, f in flows.items()},
            "rows_pushed": {i: f.rows_pushed for i, f in flows.items()},
            "rows_consumed": {i: f.rows_consumed for i, f in flows.items()},
            "bytes_pushed": {i: f.bytes_pushed for i, f in flows.items()},
            "bytes_consumed": {i: f.bytes_consumed for i, f in flows.items()},
            "blocks_pushed": {i: f.blocks_pushed for i, f in flows.items()},
            "wait_demand_s": {i: f.wait_demand_s for i, f in flows.items()},
            "wait_output_s": {i: f.wait_output_s for i, f in flows.items()},
        }

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

    def _check_split_idx(self, split_idx: int) -> None:
        if not 0 <= split_idx < self._n:
            raise ValueError(
                f"split_idx must be between 0 and {self._n - 1}, got {split_idx}."
            )

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
            # can still take it. The barrier is released even if teardown
            # fails: otherwise the other splits would wait forever, and a
            # retry would push the arrival count below zero.
            try:
                self._teardown_epoch()
            except Exception:
                logger.warning(
                    f"Failed to tear down epoch {starting_epoch}; starting the "
                    "next epoch anyway.",
                    exc_info=True,
                )
            finally:
                with self._barrier_cond:
                    self._teardown_complete_for = starting_epoch
                    self._barrier_cond.notify_all()

        start_time = time.time()
        with self._barrier_cond:
            while self._cur_epoch == starting_epoch and (
                self._num_unarrived_splits_at_barrier != 0
                or self._teardown_complete_for != starting_epoch
            ):
                if time.time() - start_time > BLOCKED_CLIENT_WARN_TIMEOUT:
                    if log_once(f"push_split_blocked_{split_idx}_{starting_epoch}"):
                        logger.warning(
                            f"Push-based split (epoch={starting_epoch}, "
                            f"split={split_idx}) blocked waiting on other "
                            f"clients for more than "
                            f"{BLOCKED_CLIENT_WARN_TIMEOUT}s. All clients must "
                            "iterate their splits at the same time."
                        )
                # Woken by the last arrival; the timeout only paces the
                # blocked-client warning.
                self._barrier_cond.wait(timeout=1.0)

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
            # _reset_state (see request_rows).
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
                    # manager.
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
        self._flows = {i: _SplitFlow() for i in range(self._n)}

    # ------------------------------------------------------------------
    # Pushers (plain threads, one per split per epoch).
    # ------------------------------------------------------------------

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
        flow = self._flows[split_idx]
        # Deliveries carry a sequence number; the receiver reorders them.
        seq = 0
        try:
            while not stop.is_set():
                # Wait for room in the window; a report wakes this
                # immediately. A dead consumer stops reporting, which parks
                # this thread with the split's remaining data kept in the
                # executor.
                # TODO(push-split): let a replacement worker resume a parked
                # split mid-epoch.
                t0 = time.monotonic()
                has_room = flow.wait_for_room(self.DEMAND_WAIT_TIMEOUT_S)
                if not has_room:
                    flow.wait_demand_s += time.monotonic() - t0
                    continue

                # Blocks in the executor's output queue, registering this
                # thread as a waiting consumer (preserving the executor's
                # backpressure signals); raises StopIteration at end of
                # stream. A multi-block bundle is sent whole.
                t0 = time.monotonic()
                bundle = output_iterator.get_next(split_idx)
                flow.wait_output_s += time.monotonic() - t0
                for entry in bundle.blocks:
                    size_bytes = entry.metadata.size_bytes or 0
                    num_rows = entry.metadata.num_rows
                    if num_rows is None:
                        # Unknown size: charge a full window so flow control
                        # still sends one block at a time. The consumer
                        # reports back this same count.
                        num_rows = max(1, flow.target_rows)
                    push_block(seq, entry, size_bytes, num_rows)
                    seq += 1
                    flow.record_push(num_rows, size_bytes)
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
            self._flows[split_idx].finish()
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
        """Report bytes sent to consumers but not yet consumed; this paces
        the producer.

        The row window only gates what's sent. Without this feed, a fast
        producer runs the whole epoch ahead of slow consumers and spills.
        """
        executor = self._current_executor
        if executor is None:
            return
        with self._pacing_lock:
            total = sum(flow.in_flight_bytes() for flow in self._flows.values())
            try:
                executor.set_external_consumer_bytes(total)
            except Exception:
                # The executor may be mid-shutdown during an epoch transition.
                pass
