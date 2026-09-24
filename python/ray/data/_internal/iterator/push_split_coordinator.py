"""Coordinator actor for the push-based streaming_split.

The coordinator runs a streaming executor over a ``StreamingSplit`` dataset,
recreated every epoch behind a barrier that all ``n`` splits must reach. It
tracks a row-based prefetch window per split: each consumer declares a
``target_rows`` window and reports what it consumes, and the coordinator
computes how many more rows the split may be sent. Bytes that are sent but
not yet consumed feed the executor's external-consumer backpressure, so a
fast producer can't run far ahead of slow consumers.
"""

import logging
import threading
import time
from typing import TYPE_CHECKING, Dict, List, Optional, Set

import ray
from ray.data.context import DataContext
from ray.util.debug import log_once

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces import NodeIdStr
    from ray.data.dataset import Dataset

logger = logging.getLogger(__name__)

BLOCKED_CLIENT_WARN_TIMEOUT = 30


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
            # barrier releases.
            self._teardown_epoch()
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
