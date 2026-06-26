"""Background metadata prefetch for the streaming executor.

`DataOpTask.on_data_ready` defers every pulled ``(block_ref, meta_ref)`` pair
(it never calls ``ray.get`` itself). The ``MetadataPrefetcher`` fetches those
``meta_ref``s on a dedicated thread so the executor thread never blocks on
``ray.get(meta_refs)``.

Flow (executor thread), per scheduling iteration:
- :meth:`submit` enqueues an operator's new pairs' meta_refs for fetching and
  appends them to that operator's FIFO,
- :meth:`register_drained_tasks` records end-of-stream/failed tasks whose
  done-callback must be postponed until their pairs have emitted,
- :meth:`emit_ready` emits the pairs whose metadata is now available (per-op
  append order), and
- :meth:`fire_done_callbacks` fires the postponed done-callbacks for tasks
  whose pairs have all emitted.

The two threads communicate through one thread-safe queue (``_request_q``);
fetched bytes come back via ``_results``.

The background thread fetches the refs ``ray.wait(fetch_local=True)`` reports
ready; a ref stuck on a bad node merely stays pending instead of wedging the
thread. The wait may block up to its timeout when a straggler is in flight,
but this runs off the scheduling thread, so it only delays when fetched
metadata lands in ``_results`` — never the scheduling loop itself.

Ordering: per-operator FIFOs are emitted front-first and stop at the first pair
whose metadata isn't back yet — so each operator's ``RefBundle`` emission order
is preserved exactly, and an operator whose next pair is still in flight is
simply skipped this round and retried next. Operators are independent, so one
operator waiting on metadata never blocks another.

Set ``RAY_DATA_METADATA_PREFETCH_ON_THREAD=0`` to disable the background thread
and fetch metadata synchronously on the executor thread instead (a kill switch
for the threaded path).
"""

import logging
import queue as queue_module
import threading
from collections import defaultdict, deque
from collections.abc import Hashable
from enum import Enum
from typing import Any, Dict, List, Set, Tuple, Union

import ray
from ray._common.utils import env_bool
from ray.data._internal.execution.interfaces.physical_operator import (
    DataOpTask,
    DeferredEmit,
)
from ray.exceptions import GetTimeoutError
from ray.util.debug import log_once

logger = logging.getLogger(__name__)

# How long ``stop`` waits for the fetch thread to exit.
_FETCH_THREAD_JOIN_TIMEOUT_S = 5.0

# How long the fetch thread's ``ray.wait`` blocks each pass — bounds the
# busy-wait when nothing is ready, and how long a straggler can delay a batch.
_FETCH_WAIT_TIMEOUT_S = 0.1

# Kill switch: when false, skip the background thread and fetch metadata
# synchronously on the executor thread (the pre-threading behavior).
_PREFETCH_ON_THREAD = env_bool("RAY_DATA_METADATA_PREFETCH_ON_THREAD", True)


class _Signal(Enum):
    """Sentinels used by the prefetcher.

    ``STOP`` is enqueued on the request queue to tell the fetch thread to exit;
    ``NOT_READY`` marks "ref not fetched yet" in the result store. Members of a
    single enum so identity checks narrow cleanly under type checkers.
    """

    STOP = "stop"
    NOT_READY = "not_ready"


# A request-queue item: a batch of meta_refs to fetch, or the stop sentinel.
_Request = Union[List["ray.ObjectRef"], _Signal]


class MetadataPrefetcher:
    def __init__(self):
        self._request_q: "queue_module.Queue[_Request]" = queue_module.Queue()
        # fetch thread -> executor: meta_ref -> bytes (or captured Exception).
        self._results: Dict["ray.ObjectRef", Any] = {}
        self._results_lock = threading.Lock()

        # Executor-thread-only state below.
        # Per-operator (keyed by the caller's op key) FIFO of pairs awaiting
        # metadata, in append (= emission) order. Each op's deque is drained
        # front-first so that op's RefBundle emission order is preserved.
        self._fifos: "defaultdict[Hashable, deque[DeferredEmit]]" = defaultdict(deque)
        # Drained (end-of-stream/failed) tasks whose done-callback is postponed
        # until all of their deferred pairs have been emitted. A set so a task
        # re-seen on a later iteration (still pending) isn't registered — or
        # fired — twice.
        self._drained_tasks: Set[DataOpTask] = set()

        self._use_thread = _PREFETCH_ON_THREAD
        self._thread = threading.Thread(
            target=self._run, name="ray-data-metadata-prefetch", daemon=True
        )
        self._started = False
        self._stopped = False

    def start(self) -> None:
        if self._use_thread and not self._started:
            self._started = True
            self._thread.start()

    def stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        if self._use_thread:
            self._request_q.put(_Signal.STOP)
            if self._started:
                self._thread.join(timeout=_FETCH_THREAD_JOIN_TIMEOUT_S)

    def submit(self, op_key: Hashable, deferred: List[DeferredEmit]) -> None:
        """Queue one operator's newly-pulled pairs for metadata fetch + emission.

        Appends them to the operator's FIFO (preserving emission order) and
        hands their ``meta_ref``s to the fetch thread. Must be called on the
        executor thread.
        """
        if not deferred:
            return
        fifo = self._fifos[op_key]
        for d in deferred:
            d.task.mark_pending()
            fifo.append(d)
        if self._use_thread:
            self._request_q.put([d.meta_ref for d in deferred])

    def register_drained_tasks(self, tasks: List[DataOpTask]) -> None:
        """Record end-of-stream/failed tasks so their done-callback fires once
        all of their deferred pairs have emitted (see :meth:`fire_done_callbacks`).

        Must be called on the executor thread.
        """
        for task in tasks:
            if task.is_drained():
                self._drained_tasks.add(task)

    def emit_ready(self) -> List[Tuple[str, BaseException]]:
        """Emit every pair whose metadata is now available, in per-op append
        order.

        Returns ``(operator_name, exception)`` for each pair whose metadata
        fetch failed. Those pairs are accounted as emitted (so the task can
        still complete) but their block is dropped; the caller applies the
        executor's ``max_errored_blocks`` accounting — matching the inline
        path, where a metadata-fetch error propagated out of ``on_data_ready``
        and was counted (and optionally ignored) there. Surfacing the error as
        a return value rather than raising keeps that tolerance intact.

        Must be called on the executor thread.
        """
        if not self._use_thread:
            # No background thread: fetch synchronously on this thread first.
            self._fetch_pending_sync()

        failures: List[Tuple[str, BaseException]] = []
        for fifo in self._fifos.values():
            while fifo:
                d = fifo[0]
                result = self._pop_result(d.meta_ref)
                if result is _Signal.NOT_READY:
                    # Preserve order: stop at the first pair still in flight;
                    # this operator is retried next call.
                    break
                fifo.popleft()
                d.task.mark_emitted()
                if isinstance(result, BaseException):
                    failures.append((d.task.operator_name, result))
                    continue
                try:
                    d.task.emit_block(d.block_ref, result)
                except Exception as e:
                    # Deserializing/emitting the fetched metadata can also fail
                    # (e.g. ``pickle.loads`` raising on a corrupt object). Treat
                    # it as a block-level error and route it through the same
                    # accounting, rather than letting it escape.
                    failures.append((d.task.operator_name, e))
        return failures

    def fire_done_callbacks(self) -> List[Tuple[str, BaseException]]:
        """Fire postponed done-callbacks for drained tasks whose pairs have all
        emitted. Call after :meth:`emit_ready`.

        A failed task fires its done-callback with the error; the error is also
        surfaced in the return value so the caller counts it toward
        ``max_errored_blocks`` (like the inline failure path did).

        Must be called on the executor thread.
        """
        if not self._drained_tasks:
            return []
        failures: List[Tuple[str, BaseException]] = []
        fired = [t for t in self._drained_tasks if not t.has_pending_emits()]
        for task in fired:
            if task.task_error is not None:
                failures.append((task.operator_name, task.task_error))
            task.mark_done()
        self._drained_tasks.difference_update(fired)
        return failures

    def has_pending_work(self) -> bool:
        """Whether any submitted pair has yet to be emitted, or any postponed
        done callback has yet to fire. Lets callers poll to completion.

        Must be called on the executor thread.
        """
        return any(self._fifos.values()) or bool(self._drained_tasks)

    def _pop_result(self, ref: "ray.ObjectRef") -> Any:
        with self._results_lock:
            return self._results.pop(ref, _Signal.NOT_READY)

    def _fetch_pending_sync(self) -> None:
        """Synchronously fetch metadata for the currently-pending refs. Used
        only when the background thread is disabled; runs on the executor
        thread (so it can block up to ``_FETCH_WAIT_TIMEOUT_S``)."""
        with self._results_lock:
            already = set(self._results)
        pending = [
            d.meta_ref
            for fifo in self._fifos.values()
            for d in fifo
            if d.meta_ref not in already
        ]
        if pending:
            self._fetch(pending)

    def _run(self) -> None:
        """Fetch-thread loop: accumulate requested meta_refs into a pending
        set and hand them to ``_fetch``, which fetches the locally-available
        ones and returns those still in flight.
        """
        pending: List["ray.ObjectRef"] = []
        while True:
            # Block on the queue only when idle; while refs are in flight,
            # don't block here — get back to ``_fetch`` to keep them moving.
            try:
                item = self._request_q.get(block=not pending)
            except queue_module.Empty:
                item = None
            # Drain whatever else is already queued into a single fetch batch.
            while item is not None:
                if isinstance(item, _Signal):
                    # Fast teardown: drop any in-flight refs and exit. ``stop``
                    # runs after the scheduling loop (which feeds us) is joined,
                    # so there's nothing left to emit.
                    return
                pending.extend(item)
                try:
                    item = self._request_q.get_nowait()
                except queue_module.Empty:
                    item = None

            if pending:
                pending = self._fetch(pending)

    def _fetch(self, pending: List["ray.ObjectRef"]) -> List["ray.ObjectRef"]:
        """Fetch the locally-available refs in ``pending`` and publish them;
        return the refs still awaiting fetch (to be retried next pass).

        ``ray.get`` is only issued on refs that ``ray.wait(fetch_local=True)``
        reports ready, so a ref stuck on a bad/dead node never blocks the
        caller — it just stays pending. The wait blocks up to
        ``_FETCH_WAIT_TIMEOUT_S`` (also avoiding busy-spin); a straggler can
        delay a batch by up to that timeout.

        Ready refs are local, so every ``ray.get`` uses ``timeout=0`` and never
        blocks on transfer. A ref that resolved to a task error is still
        available and raises that error immediately (not ``GetTimeoutError``),
        so it's captured per-ref for ``emit_ready`` to surface rather than
        silently dropping the block. A ref that raced out of the local store
        raises ``GetTimeoutError`` and is returned for retry — neither published
        nor counted as a block error.
        """
        ready, not_ready = ray.wait(
            pending,
            num_returns=len(pending),
            timeout=_FETCH_WAIT_TIMEOUT_S,
            fetch_local=True,
        )
        if not ready:
            return not_ready

        retry: List["ray.ObjectRef"] = []
        try:
            values = ray.get(ready, timeout=0)
            results: Dict["ray.ObjectRef", Any] = dict(zip(ready, values))
        except Exception:
            # A batched get raises on the first error and hides which ref
            # failed; retry per-ref to isolate it and keep the rest.
            results = {}
            for ref in ready:
                try:
                    results[ref] = ray.get(ref, timeout=0)
                except GetTimeoutError:
                    # ray.wait reported it ready but it's no longer local
                    # (e.g. a raced eviction). Re-queue rather than treating it
                    # as a block-level error. Shouldn't be common — log once so
                    # we know if we're hitting this path.
                    if log_once("ray_data_metadata_prefetch_not_local"):
                        logger.warning(
                            "A metadata object reported ready by ray.wait was "
                            "not locally available for ray.get; re-queuing it. "
                            "If this repeats, the object store may be under "
                            "memory pressure (objects evicted/spilled)."
                        )
                    retry.append(ref)
                except Exception as e:
                    results[ref] = e
        if results:
            with self._results_lock:
                self._results.update(results)
        return not_ready + retry
