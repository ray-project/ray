"""Background metadata prefetch for the streaming executor.

`DataOpTask.on_data_ready` defers every pulled ``(block_ref, meta_ref)`` pair
(it never calls ``ray.get`` itself). The ``MetadataPrefetcher`` fetches those
``meta_ref``s on a dedicated thread so the executor thread never blocks on
``ray.get(meta_refs)``.

Flow (executor thread): each scheduling iteration calls :meth:`submit` (enqueue
the new pairs' meta_refs for fetching and append them to per-operator FIFOs)
then :meth:`drain` (emit the pairs whose metadata is now available, in per-op
append order). The background thread blocks on ``ray.get`` and publishes the
fetched bytes.

Ordering: per-operator FIFOs are emitted front-first and stop at the first pair
whose metadata isn't back yet — so each operator's ``RefBundle`` emission order
is preserved exactly, and an operator whose next pair is still in flight is
simply skipped this round and retried next (matching the synchronous
break-and-retry behavior). Operators are independent, so one operator waiting
on metadata never blocks another.
"""

import logging
import queue as queue_module
import threading
import time
from collections import OrderedDict, deque
from typing import Any, Dict, List

import ray
from ray.data._internal.execution.interfaces.physical_operator import (
    DataOpTask,
    DeferredEmit,
    _emit_deferred_entry,
    _fire_task_done,
)

logger = logging.getLogger(__name__)

# Result-store sentinels.
_FETCH_FAILED = object()  # ``ray.get`` failed for this ref
_NOT_READY = object()  # ref not yet fetched

# How long ``stop`` waits for the fetch thread to exit.
_FETCH_THREAD_JOIN_TIMEOUT_S = 5.0


class MetadataPrefetcher:
    """Fetches deferred ``meta_ref``s on a background thread; emits in per-op
    order on the executor thread. See module docstring."""

    def __init__(self):
        # executor -> fetch thread: each item is a list[ObjectRef] (or None
        # sentinel to stop). thread-safe.
        self._request_q: "queue_module.Queue" = queue_module.Queue()
        # fetch thread -> executor: meta_ref -> bytes (or _FETCH_FAILED).
        self._results: Dict["ray.ObjectRef", Any] = {}
        self._results_lock = threading.Lock()

        # Executor-thread-only state below.
        # Per-operator (keyed by the caller's op key) FIFO of pairs awaiting
        # metadata, in append (= emission) order.
        self._fifos: "OrderedDict[Any, deque]" = OrderedDict()
        # Tasks whose end-of-stream callback is postponed until all of their
        # deferred pairs have been emitted (``_pending_emit_count == 0``).
        self._done_pending: List[DataOpTask] = []

        self._thread = threading.Thread(
            target=self._run, name="ray-data-metadata-prefetch", daemon=True
        )
        self._started = False
        self._stopped = False

    def start(self) -> None:
        if not self._started:
            self._started = True
            self._thread.start()

    def stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        self._request_q.put(None)  # sentinel
        if self._started:
            self._thread.join(timeout=_FETCH_THREAD_JOIN_TIMEOUT_S)

    def submit(
        self,
        op_key: Any,
        deferred: List[DeferredEmit],
        ready_tasks: List[DataOpTask],
    ) -> None:
        """Queue ``deferred`` pairs (from one operator) for fetching + emission,
        and register any end-of-stream tasks for a postponed done callback.

        Must be called on the executor thread.
        """
        if deferred:
            fifo = self._fifos.get(op_key)
            if fifo is None:
                fifo = deque()
                self._fifos[op_key] = fifo
            for d in deferred:
                d.task._pending_emit_count += 1
                fifo.append(d)
            self._request_q.put([d.meta_ref for d in deferred])
        for task in ready_tasks:
            if task._task_done_pending and task not in self._done_pending:
                self._done_pending.append(task)

    def drain(self) -> None:
        """Emit every pair whose metadata is now available, in per-op append
        order, then fire postponed done callbacks for fully-drained tasks.

        Must be called on the executor thread.
        """
        for fifo in self._fifos.values():
            while fifo:
                d = fifo[0]
                meta_bytes = self._pop_result(d.meta_ref)
                if meta_bytes is _NOT_READY:
                    # Preserve order: stop at the first pair still in flight;
                    # this operator is retried next drain.
                    break
                fifo.popleft()
                d.task._pending_emit_count -= 1
                if meta_bytes is _FETCH_FAILED:
                    logger.warning(
                        "Metadata fetch failed for a block from operator "
                        "'%s'; skipping its RefBundle.",
                        d.task._operator_name,
                    )
                    continue
                _emit_deferred_entry(d, meta_bytes)

        if self._done_pending:
            still_pending: List[DataOpTask] = []
            for task in self._done_pending:
                if task._pending_emit_count <= 0:
                    _fire_task_done(task)
                else:
                    still_pending.append(task)
            self._done_pending = still_pending

    def flush(self, timeout_s: float = 30.0) -> None:
        """Drain repeatedly until every submitted pair has been emitted and
        all postponed done callbacks have fired.

        Blocks the calling thread, so this is for tests (and other one-shot
        callers) that need the deferred emits to have landed before
        asserting; the executor itself just calls :meth:`drain` once per
        scheduling iteration.

        Must be called on the executor thread.
        """
        deadline = time.monotonic() + timeout_s
        while True:
            self.drain()
            if not any(self._fifos.values()) and not self._done_pending:
                return
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    "MetadataPrefetcher.flush timed out after " f"{timeout_s} seconds."
                )
            time.sleep(0.001)

    def _pop_result(self, ref: "ray.ObjectRef") -> Any:
        with self._results_lock:
            return self._results.pop(ref, _NOT_READY)

    def _run(self) -> None:
        """Fetch-thread loop: drain the request queue (coalescing queued
        batches into one ``ray.get``) and publish results until stopped."""
        while True:
            item = self._request_q.get()
            if item is None:
                return
            batch = list(item)
            stop_after = False
            # Coalesce any other already-queued batches into a single ray.get.
            while True:
                try:
                    nxt = self._request_q.get_nowait()
                except queue_module.Empty:
                    break
                if nxt is None:
                    stop_after = True
                    break
                batch.extend(nxt)
            self._fetch(batch)
            if stop_after:
                return

    def _fetch(self, batch: List["ray.ObjectRef"]) -> None:
        try:
            values = ray.get(batch)
            results: Dict["ray.ObjectRef", Any] = dict(zip(batch, values))
        except Exception:
            # Batched fetch failed; retry per-ref so one bad ref doesn't drop
            # the rest. Refs that still fail are marked so drain can skip them.
            results = {}
            for ref in batch:
                try:
                    results[ref] = ray.get(ref)
                except Exception:
                    results[ref] = _FETCH_FAILED
        with self._results_lock:
            self._results.update(results)
