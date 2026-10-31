"""Push-based streaming_split.

A coordinator actor runs the streaming executor and pushes each split's
blocks to the actor hosting that split's ``PushBasedDataIterator`` (e.g. a
Ray Train worker), which iterates them from a local queue.

- The coordinator runs one streaming executor per epoch, behind a barrier
  that all ``n`` splits must reach, plus one pusher thread per split.
- Flow control is demand-driven and measured in rows: each consumer
  declares a ``prefetch_batches * batch_size`` row prefetch window and
  reports what it consumes; the coordinator pushes whole blocks while
  ``target_rows - (rows_pushed - rows_consumed)`` is positive. The
  consumer's local queue stores the prefetched blocks.
- Bytes sent but not yet consumed feed the executor's external-consumer
  backpressure, so a fast producer can't run far ahead of slow consumers.
- Deliveries are sequence-numbered and reordered on arrival, so consumers
  work regardless of the hosting actor's concurrency (Ray executes a
  multi-threaded actor's tasks out of order).
- Consumers reuse the standard batching pipeline (batch -> format/collate ->
  finalize), so ``iter_torch_batches`` works unchanged; only the ref-level
  prefetch/resolve stages are skipped.
- A consumer is any actor that mixes in ``PushSplitReceiverMixin`` (e.g. a
  Ray Train worker).

Overview::

                  PushSplitCoordinator actor
    +--------------------------------------------------+
    |  StreamingExecutor:  read -> ... -> split(n)     |
    |      split 0       split 1     ...    split n-1  |
    |         |             |                  |       |
    |     pusher 0      pusher 1          pusher n-1   |
    +---------|-------------^--------------------------+
              | blocks      | request_rows()
              | (by value)  | (declares the row window,
              |             |  reports consumption per
              |             |  block)
              v             |
    +--------------------------------------------------+
    |  consumer actor i  (mixes PushSplitReceiverMixin)|
    |    deliveries -> reorder by seq -> local queue   |
    |    PushBasedDataIterator: pop -> batch           |
    +--------------------------------------------------+

This module holds the coordinator; the consumer side lives in
``push_based_split_iterator.py``.

In the next PRs: stats/metrics export, locality-aware pushing, mid-epoch
consumer replacement.
"""

import collections
import logging
import threading
import time
from typing import TYPE_CHECKING, Any, Deque, Dict, Optional, Set, Tuple

import ray
from ray.data._internal.stats import DatasetStats
from ray.data.context import DataContext

if TYPE_CHECKING:
    from ray.data.dataset import Dataset

logger = logging.getLogger(__name__)

BLOCKED_CLIENT_WARN_TIMEOUT = 30


class _SplitFlow:
    """Row-window flow control and pacing counters for one split and epoch.

    The consumer declares ``target_rows`` and reports consumption; senders
    may send while ``target_rows - (rows_pushed - rows_consumed) > 0``.
    Whole blocks are sent, so one send may overshoot the window, which only
    delays the next send until consumption catches up.

    Each sent block's ObjectRef is held until the consumer reports its bytes
    consumed. Blocks are sent by value, so the consumer holds no reference;
    holding one here keeps Ray Data counting delivered-but-unconsumed blocks
    against the operator's object-store usage, as it counts a pull
    consumer's prefetched refs. It adds no object-store memory: the consumer
    pins the object anyway.
    """

    def __init__(self):
        self.cond = threading.Condition()
        self.target_rows = 0
        self.rows_pushed = 0
        self.rows_consumed = 0
        self.bytes_pushed = 0
        self.bytes_consumed = 0
        self.finished = False
        # (block ref, size_bytes) of sent blocks not yet reported consumed,
        # in send order; consumers consume in send order.
        self._unconsumed_refs: Deque[Tuple[Any, int]] = collections.deque()
        self._released_bytes = 0

    def report(self, target_rows: int, consumed_rows: int, consumed_bytes: int):
        with self.cond:
            # Reports are fire-and-forget, so one sent before the consumer
            # finished can arrive after finish(); it must not reopen the
            # window.
            if self.finished:
                return
            self.target_rows = target_rows
            self.rows_consumed += consumed_rows
            self.bytes_consumed += consumed_bytes
            self._release_consumed_refs()
            self.cond.notify()

    def _release_consumed_refs(self) -> None:
        # Bytes are reported cumulatively, so a reordered report still
        # releases exactly the blocks consumed so far.
        while self._unconsumed_refs:
            _, size_bytes = self._unconsumed_refs[0]
            if self._released_bytes + size_bytes > self.bytes_consumed:
                break
            self._unconsumed_refs.popleft()
            self._released_bytes += size_bytes

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

    def record_push(
        self, num_rows: int, size_bytes: int, block_ref: Optional[Any] = None
    ) -> None:
        with self.cond:
            # A sender that was mid-fetch when the split finished must not
            # add bytes no consumer will read back.
            if self.finished:
                return
            self.rows_pushed += num_rows
            self.bytes_pushed += size_bytes
            if block_ref is not None:
                self._unconsumed_refs.append((block_ref, size_bytes))

    def num_unconsumed_refs(self) -> int:
        with self.cond:
            return len(self._unconsumed_refs)

    def finish(self) -> None:
        """Close the window and drop this split's contribution to pacing."""
        with self.cond:
            self.finished = True
            self.rows_consumed = self.rows_pushed
            self.bytes_consumed = self.bytes_pushed
            self.target_rows = 0
            self._unconsumed_refs.clear()

    def in_flight_bytes(self) -> int:
        # Consumed never exceeds pushed: senders record a push before sending
        # it, and consumers only report blocks they received. The max() is
        # only a guard.
        with self.cond:
            return max(0, self.bytes_pushed - self.bytes_consumed)


@ray.remote(num_cpus=0)
class PushSplitCoordinator:
    """Coordinator actor for a push-based streaming split.

    Runs the streaming executor (one per epoch) and tracks each split's row
    window, which gates how much may be sent to that split's consumer.
    """

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
        # Set if tearing down the previous epoch failed; the next epoch then
        # fails for every split, as SplitCoordinator does.
        self._teardown_error: Optional[Exception] = None

        # Recreated every epoch, so a stale report can only land on the
        # previous epoch's discarded state.
        self._flows: Dict[int, _SplitFlow] = {i: _SplitFlow() for i in range(n)}
        # Serializes compute-and-report of external consumer bytes, so a
        # stale total can't overwrite a newer one.
        self._pacing_lock = threading.Lock()

        # Time spent handling consumer calls, reported in stats().
        self._coordinator_overhead_s = 0.0

        logger.debug(f"PushSplitCoordinator created: {n=}")

    # ------------------------------------------------------------------
    # Control plane (actor tasks).
    # ------------------------------------------------------------------

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

        The external-consumer-bytes feed isn't refreshed here: sends refresh
        it, and between sends the reported total can only be higher than the
        real one (consumption lowers it), which the executor treats as more
        downstream capacity, never less.
        """
        start_time = time.perf_counter()
        flow = self._flows[split_idx]
        with flow.cond:
            # Checked under the cond: the epoch bump precedes replacing the
            # flows, so a report either sees the new epoch and is dropped, or
            # lands on the old epoch's discarded state.
            if epoch_id == self._cur_epoch:
                flow.report(target_rows, consumed_rows, consumed_bytes)
        self._coordinator_overhead_s += time.perf_counter() - start_time

    def notify_split_finished(self, epoch_id: int, split_idx: int) -> None:
        """Consumer stopped iterating ``epoch_id``; stale epochs are ignored."""
        self._finish_split(epoch_id, split_idx)

    def debug_state(self) -> Dict[str, Dict[int, int]]:
        """Snapshot of per-split flow-control state, for debugging/tests."""
        state: Dict[str, Dict[int, int]] = collections.defaultdict(dict)
        for i, flow in self._flows.items():
            state["target_rows"][i] = flow.target_rows
            state["rows_pushed"][i] = flow.rows_pushed
            state["rows_consumed"][i] = flow.rows_consumed
            state["bytes_pushed"][i] = flow.bytes_pushed
            state["bytes_consumed"][i] = flow.bytes_consumed
            state["unconsumed_refs"][i] = flow.num_unconsumed_refs()
        return dict(state)

    def get_dataset_schema(self):
        with self._dataset_state_lock:
            if self._schema is not None:
                return self._schema
            if self._current_executor is not None and self._current_executor.is_alive():
                raise RuntimeError(
                    "Cannot call schema() during active dataset execution. "
                    "Call schema() before or after iterating over the dataset, "
                    "or call schema() directly on the source Dataset object."
                )
            self._schema = self._base_dataset.schema()
            return self._schema

    def stats(self) -> DatasetStats:
        if self._current_executor:
            stats = self._current_executor.get_stats()
        else:
            stats = self._base_dataset._raw_stats()
        stats.streaming_split_coordinator_s.add(self._coordinator_overhead_s)
        return stats

    def get_dataset_context(self) -> "DataContext":
        return self._data_context

    def get_dataset_tag(self, output_split_idx: int) -> Dict[str, str]:
        # Fetched before the epoch starts; its executor is created lazily, so
        # the run index hasn't been incremented yet.
        return {
            "dataset": self._base_dataset._get_dataset_id_for_next_run(),
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
            # barrier releases. A teardown failure is raised to every split
            # (see _try_start_new_epoch), but the barrier is still released:
            # otherwise the other splits would wait forever, and a retry
            # would push the arrival count below zero.
            teardown_error = None
            try:
                self._teardown_epoch()
            except Exception as e:
                logger.warning(f"Failed to tear down epoch {starting_epoch}: {e}")
                teardown_error = e
            finally:
                with self._barrier_cond:
                    self._teardown_error = teardown_error
                    self._teardown_complete_for = starting_epoch
                    self._barrier_cond.notify_all()

        def released() -> bool:
            return self._cur_epoch != starting_epoch or (
                self._num_unarrived_splits_at_barrier == 0
                and self._teardown_complete_for == starting_epoch
            )

        with self._barrier_cond:
            if not self._barrier_cond.wait_for(
                released, timeout=BLOCKED_CLIENT_WARN_TIMEOUT
            ):
                logger.warning(
                    f"Push-based split (epoch={starting_epoch}, "
                    f"split={split_idx}) blocked waiting on other clients for "
                    f"more than {BLOCKED_CLIENT_WARN_TIMEOUT}s. All clients "
                    "must iterate their splits at the same time."
                )
                self._barrier_cond.wait_for(released)

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
        """Force-shutdown the previous epoch's executor."""
        if self._current_executor is not None:
            self._current_executor.shutdown(force=True)

    def _try_start_new_epoch(self, starting_epoch: int) -> None:
        with self._lock:
            # Start the epoch exactly once. The bump must precede
            # _reset_state (see request_rows).
            if self._cur_epoch == starting_epoch:
                self._cur_epoch += 1
                self._reset_state()
                try:
                    if self._teardown_error is not None:
                        raise self._teardown_error
                    ds = self._base_dataset
                    self._current_executor = ds._create_executor()
                    self._output_iterator = ds._build_bundle_iterator(
                        self._current_executor
                    )
                    # Register the external consumers with the resource
                    # manager.
                    self._current_executor.set_external_consumer_bytes(0)
                    logger.debug(
                        f"Starting epoch {self._cur_epoch} (all {self._n} "
                        "clients synced)."
                    )
                except Exception as e:
                    logger.warning(f"Error starting epoch {self._cur_epoch}: {e}")
                    self._gen_epoch_error = e
                finally:
                    self._teardown_error = None

        if self._gen_epoch_error is not None:
            raise self._gen_epoch_error

    def _reset_state(self) -> None:
        self._num_unarrived_splits_at_barrier = self._n
        self._finished_splits.clear()
        self._gen_epoch_error = None
        self._flows = {i: _SplitFlow() for i in range(self._n)}

    def _finish_split(self, epoch_id: int, split_idx: int) -> None:
        executor_to_shutdown = None
        with self._lock:
            # notify_split_finished is fire-and-forget and this actor runs
            # calls concurrently, so a consumer's notify for epoch e can run
            # after its own start_epoch call has helped release the barrier
            # into epoch e + 1. The barrier already tore epoch e down, so
            # there is nothing left to finish.
            if epoch_id != self._cur_epoch:
                return
            self._finished_splits.add(split_idx)
            flow = self._flows[split_idx]
            logger.debug(
                f"Split {split_idx} epoch {epoch_id} finished; sent "
                f"{flow.rows_pushed} rows, {flow.rows_consumed} consumed."
            )
            flow.finish()
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
