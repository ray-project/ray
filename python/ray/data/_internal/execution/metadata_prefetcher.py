"""Background metadata prefetch for the streaming executor.

`DataOpTask.on_data_ready` defers every pulled ``(block_ref, meta_ref)`` pair
(it never calls ``ray.get`` itself). The ``MetadataPrefetcher`` fetches those
``meta_ref``s on a dedicated thread so the executor thread never blocks on
``ray.get(meta_refs)``.

Flow (executor thread): each scheduling iteration calls :meth:`submit` (enqueue
the new pairs' meta_refs for fetching and append them to per-operator FIFOs)
then :meth:`drain` (emit the pairs whose metadata is now available, in per-op
append order). The two threads communicate through one thread-safe queue
(``_request_q``); fetched bytes come back via ``_results``.

The background thread fetches the refs ``ray.wait(fetch_local=True)`` reports
ready; a ref stuck on a bad node merely stays pending instead of wedging the
thread. The wait may block up to its timeout when a straggler is in flight,
but this runs off the scheduling thread, so it only delays when fetched
metadata lands in ``_results`` — never the scheduling loop itself.

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
from collections import defaultdict, deque
from typing import Any, Dict, List, Set, Tuple

import ray
from ray.data._internal.execution.interfaces.physical_operator import (
    DataOpTask,
    DeferredEmit,
    _emit_deferred_entry,
    _fire_task_done,
)

logger = logging.getLogger(__name__)

# Sentinel for "ref not yet fetched" in the result store. A fetched result is
# either the metadata bytes or an ``Exception`` captured during fetch.
_NOT_READY = object()

# How long ``stop`` waits for the fetch thread to exit.
_FETCH_THREAD_JOIN_TIMEOUT_S = 5.0

# How long the fetch thread's ``ray.wait`` blocks each pass — bounds the
# busy-wait when nothing is ready, and how long a straggler can delay a batch.
_FETCH_WAIT_TIMEOUT_S = 0.1


class MetadataPrefetcher:
    """Fetches deferred ``meta_ref``s on a background thread; emits in per-op
    order on the executor thread. See module docstring."""

    def __init__(self):
        # executor -> fetch thread: each item is a list[ObjectRef] (or None
        # sentinel to stop). thread-safe.
        self._request_q: "queue_module.Queue" = queue_module.Queue()
        # fetch thread -> executor: meta_ref -> bytes (or captured Exception).
        self._results: Dict["ray.ObjectRef", Any] = {}
        self._results_lock = threading.Lock()
        # Set by the fetch thread whenever it publishes results; lets a
        # ``drain(block_timeout_s>0)`` sleep until there's something to emit
        # instead of the scheduling loop spinning while metadata is in flight.
        self._published = threading.Event()

        # Executor-thread-only state below.
        # Per-operator (keyed by the caller's op key) FIFO of pairs awaiting
        # metadata, in append (= emission) order. ``defaultdict`` is a ``dict``
        # subclass, so it preserves insertion order (guaranteed since Python
        # 3.7; Ray requires >= 3.10) and ops are drained in submit order.
        self._fifos: "defaultdict[Any, deque]" = defaultdict(deque)
        # Tasks whose end-of-stream callback is postponed until all of their
        # deferred pairs have been emitted. A set so a task that is re-seen on
        # a later iteration (still pending) isn't registered — or fired —
        # twice.
        self._done_pending: Set[DataOpTask] = set()

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
            fifo = self._fifos[op_key]
            for d in deferred:
                d.mark_pending()
                fifo.append(d)
            self._request_q.put([d.meta_ref for d in deferred])
        for task in ready_tasks:
            if task.is_done_pending():
                # ``set`` dedupes: a still-pending task re-seen next iteration
                # must not be registered (or later fired) twice.
                self._done_pending.add(task)

    def drain(self, block_timeout_s: float = 0.0) -> List[Tuple[str, BaseException]]:
        """Emit every pair whose metadata is now available, in per-op append
        order, then fire postponed done callbacks for fully-drained tasks.

        If ``block_timeout_s > 0`` and there are pending pairs but the fetch
        thread hasn't published anything new, block up to that long for the
        first result before emitting. This paces the scheduling loop to
        metadata availability — without it the loop spins (re-running
        per-iteration work like resource accounting and actor-state refresh)
        while metadata is in flight. It only blocks when the iteration would
        otherwise emit nothing; if results are already ready it returns at
        once.

        Returns ``(operator_name, exception)`` for each pair whose metadata
        fetch failed. Those pairs are accounted as emitted (so the task can
        still complete) but their block is dropped; the caller applies the
        executor's ``max_errored_blocks`` accounting — matching the inline
        path, where a metadata-fetch error propagated out of ``on_data_ready``
        and was counted (and optionally ignored) there. Surfacing the error
        as a return value rather than raising keeps that tolerance intact.

        Must be called on the executor thread.
        """
        if (
            block_timeout_s > 0
            and not self._published.is_set()
            and any(self._fifos.values())
        ):
            # Pending pairs but nothing freshly published — wait for the fetch
            # thread rather than returning an empty drain.
            self._published.wait(block_timeout_s)
        self._published.clear()

        failures: List[Tuple[str, BaseException]] = []
        for fifo in self._fifos.values():
            while fifo:
                d = fifo[0]
                result = self._pop_result(d.meta_ref)
                if result is _NOT_READY:
                    # Preserve order: stop at the first pair still in flight;
                    # this operator is retried next drain.
                    break
                fifo.popleft()
                d.mark_emitted()
                if isinstance(result, BaseException):
                    failures.append((d.task.operator_name, result))
                    continue
                try:
                    _emit_deferred_entry(d, result)
                except Exception as e:
                    # Deserializing/emitting the fetched metadata can also fail
                    # (e.g. ``pickle.loads`` raising on a corrupt object). Treat
                    # it as a block-level error and route it through the same
                    # accounting, rather than letting it escape ``drain``.
                    failures.append((d.task.operator_name, e))

        if self._done_pending:
            fired = [t for t in self._done_pending if not t.has_pending_emits()]
            for task in fired:
                # A failed task fires its done-callback with the error (below);
                # also surface it so the caller counts it toward
                # ``max_errored_blocks``, like the inline failure path did.
                if task.task_error is not None:
                    failures.append((task.operator_name, task.task_error))
                _fire_task_done(task)
            self._done_pending.difference_update(fired)

        return failures

    def has_pending_work(self) -> bool:
        """Whether any submitted pair has yet to be emitted, or any postponed
        done callback has yet to fire. Lets callers poll to completion.

        Must be called on the executor thread.
        """
        return any(self._fifos.values()) or bool(self._done_pending)

    def _pop_result(self, ref: "ray.ObjectRef") -> Any:
        with self._results_lock:
            return self._results.pop(ref, _NOT_READY)

    def _run(self) -> None:
        """Fetch-thread loop: accumulate requested meta_refs into a pending
        set, fetch the ones that are locally available, and publish them.

        ``ray.get`` is only issued on refs that ``ray.wait(fetch_local=True)``
        reports ready, so a ref stuck on a bad/dead node never blocks the
        thread — it just stays pending until Ray resolves or fails it. The
        wait itself blocks up to ``_FETCH_WAIT_TIMEOUT_S`` (also serving to
        avoid busy-spin); a straggler can delay a batch by up to that timeout,
        but only here on the background thread, never on the scheduling loop.
        """
        pending: List["ray.ObjectRef"] = []
        while True:
            # Pull new requests. Block only when nothing is in flight;
            # otherwise poll so pending refs keep making progress.
            if pending:
                try:
                    item = self._request_q.get_nowait()
                except queue_module.Empty:
                    item = ()
            else:
                item = self._request_q.get()
            if item is None:
                return
            pending.extend(item)
            # Coalesce any other already-queued batches.
            while True:
                try:
                    nxt = self._request_q.get_nowait()
                except queue_module.Empty:
                    break
                if nxt is None:
                    return
                pending.extend(nxt)

            if not pending:
                continue

            ready, pending = ray.wait(
                pending,
                num_returns=len(pending),
                timeout=_FETCH_WAIT_TIMEOUT_S,
                fetch_local=True,
            )
            if ready:
                self._fetch(ready)

    def _fetch(self, batch: List["ray.ObjectRef"]) -> None:
        """Fetch refs that ``ray.wait`` reported ready and publish them.

        The refs are locally available, so ``ray.get`` returns without
        blocking on data transfer; it can still raise if a ref resolved to a
        task error. Capture that exception per-ref so ``drain`` can surface it
        on the executor thread rather than silently dropping the block.
        """
        try:
            values = ray.get(batch)
            results: Dict["ray.ObjectRef", Any] = dict(zip(batch, values))
        except Exception:
            # A batched get raises on the first error and hides which ref
            # failed; retry per-ref to isolate it and keep the rest.
            results = {}
            for ref in batch:
                try:
                    results[ref] = ray.get(ref)
                except Exception as e:
                    results[ref] = e
        with self._results_lock:
            self._results.update(results)
        # Wake a scheduling thread blocked in ``drain(block_timeout_s>0)``.
        self._published.set()
