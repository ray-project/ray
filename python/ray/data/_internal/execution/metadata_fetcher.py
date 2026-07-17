"""Metadata-fetch strategy for the streaming executor.

``DataOpTask.on_data_ready`` pulls ``(block_ref, meta_ref)`` pairs from a task's
streaming generator; a ``MetadataFetcher`` turns each pair into an emitted
``RefBundle``. Two modes, selected by ``RAY_DATA_METADATA_PREFETCH_ON_THREAD``
(default on):

- :class:`ThreadedMetadataFetcher` (default): defer every pair and fetch its
  metadata on a dedicated background thread, so the scheduling loop never blocks
  on ``ray.get(meta_ref)``. The output-budget size comes from the block's local
  ``object_size`` (no RPC); completion is postponed until the task's deferred
  pairs have emitted, and the per-operator FIFO preserves emission order.
- :class:`InlineMetadataFetcher`: fetch each pair's metadata inline with
  ``ray.get`` and emit the ``RefBundle`` immediately, budgeting off
  ``meta.size_bytes``; completion and task-failure are handled inline by
  ``on_data_ready``.
"""

import logging
import pickle
import queue as queue_module
import threading
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from collections.abc import Hashable
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

import ray
import ray.exceptions
from ray._common.utils import env_bool
from ray.data._internal.execution.interfaces.physical_operator import (
    METADATA_GET_TIMEOUT_S,
    METADATA_WAIT_TIMEOUT_S,
    DataOpTask,
    DeferredEmit,
)
from ray.data.block import BlockMetadataWithSchema
from ray.exceptions import GetTimeoutError
from ray.experimental.locations import get_local_object_locations
from ray.util.debug import log_once

logger = logging.getLogger(__name__)

# How long ``ThreadedMetadataFetcher.stop`` waits for the fetch thread to exit.
_FETCH_THREAD_JOIN_TIMEOUT_S = 5.0

# How long the fetch thread's ``ray.wait`` blocks each pass — bounds the
# busy-wait when nothing is ready, and how long a straggler can delay a batch.
_FETCH_WAIT_TIMEOUT_S = 0.1

# Selects the mode (see module docstring). Threaded by default; set to 0/false to
# fall back to the synchronous inline path.
_PREFETCH_ON_THREAD = env_bool("RAY_DATA_METADATA_PREFETCH_ON_THREAD", True)


class MetadataFetcher(ABC):
    def start(self) -> None:
        """Start any background machinery."""

    def stop(self) -> None:
        """Stop any background machinery."""

    @abstractmethod
    def in_data_ready_get_object_size(self, task: DataOpTask) -> Optional[int]:
        """Handle one pulled pair inside the ``on_data_ready`` loop. The pair's
        refs are read off the task (``task.pending_block_ref`` /
        ``task.pending_meta_ref``).

        Returns the output-budget bytes for this pair (0 if the size is
        unknown), or ``None`` to mean "the metadata isn't available yet — stop
        and retry next iteration" (the caller breaks, leaving the refs set).

        ``None`` must be returned ONLY when the pair was NOT consumed: the
        caller will hand the same pair back on the next call, so returning
        ``None`` after emitting/deferring it would emit the block twice.
        """

    def in_data_ready_done(self, task: DataOpTask) -> None:
        """Called once a task is drained (generator exhausted/failed)."""

    def submit(self, op_key: Hashable, tasks: List[DataOpTask]) -> None:
        """Hand the operator's deferred pairs off for processing, and record
        any end-of-stream/failed tasks whose completion is postponed."""

    def emit_ready_and_fire_done_callbacks(self) -> List[Tuple[str, BaseException]]:
        """Run once at the end of ``process_completed_tasks``. Returns
        ``(operator_name, exception)`` for each block-level fetch failure, for
        the caller's ``max_errored_blocks`` accounting. Default: nothing to do."""
        return []


class InlineMetadataFetcher(MetadataFetcher):
    """Synchronous mode: fetch metadata inline and emit immediately. Holds no
    state and starts no thread."""

    def in_data_ready_get_object_size(self, task: DataOpTask) -> Optional[int]:
        # The ref resolves to pickled metadata bytes, not a BlockMetadata.
        meta_ref: "ray.ObjectRef[Any]" = task.pending_meta_ref
        try:
            # The timeout includes the time to ship the metadata to this node,
            # so a 0 timeout could cancel an in-flight download. Use a small
            # non-zero value to avoid that.
            meta_bytes: bytes = ray.get(meta_ref, timeout=METADATA_GET_TIMEOUT_S)
        except ray.exceptions.GetTimeoutError:
            # We have refs to the block and its metadata, but the metadata
            # object isn't available. This can happen if the node dies. Leave
            # the pair pending and retry next iteration.
            logger.warning(
                f"Timed out ({METADATA_GET_TIMEOUT_S}s) waiting for metadata from "
                f"operator '{task.operator_name}' "
                f"(metadata_ref={meta_ref.hex()}). "
                f"Possible causes include a worker crash, node preemption, or an "
                f"overloaded worker or head node. Will retry next iteration. "
                f"If this repeats, check the Ray dashboard and logs for worker "
                f"crashes, node preemption, or overload."
            )
            return None
        return task.produce_block(task.pending_block_ref, meta_bytes)

    def in_data_ready_done(self, task: DataOpTask) -> None:
        # Inline mode fires the done-callback the moment the generator drains:
        # all of the task's pairs have already emitted inline, so
        # ``_pending_emit_count`` is 0. A task failure is re-raised after the
        # callback.
        task.mark_done()
        if task.task_error is not None:
            raise task.task_error from None


class _Signal(Enum):
    """Sentinels used by :class:`ThreadedMetadataFetcher`.

    ``STOP`` is enqueued on the request queue to tell the fetch thread to exit;
    ``NOT_READY`` marks "ref not fetched yet" in the result store. Members of a
    single enum so identity checks narrow cleanly under type checkers.
    """

    STOP = "stop"
    NOT_READY = "not_ready"


# A request-queue item: a batch of meta_refs to fetch, or the stop sentinel.
_Request = Union[List["ray.ObjectRef"], _Signal]


class ThreadedMetadataFetcher(MetadataFetcher):
    """Asynchronous mode: defer every pulled pair and fetch its metadata on a
    dedicated background thread, so the scheduling (executor) thread never blocks
    on ``ray.get(meta_refs)``.

    The two threads communicate through one thread-safe queue (``_request_q``);
    fetched bytes come back via ``_results``. The background thread fetches the
    refs ``ray.wait(fetch_local=True)`` reports ready; a ref stuck on a bad node
    merely stays pending instead of wedging the thread.

    Data flow (for a single operator)::

        Executor thread                              Fetch thread
        ---------------                              ------------
        on_data_ready  --defer-->  _pending_deferred
        submit(op)     --meta_refs-->  _request_q  ----->  ray.wait(ready)
                                                                + ray.get
                                                                   |
                            _results  <----- fetched bytes --------+
        emit_ready_and_fire_done_callbacks():
            _fifos[op]:  head [d0] -> [d1] -> [d2] tail   (append = yield order)
                              |
                              `- emit front-first while its bytes are in
                                 _results; stop at the first pair not back yet,
                                 so this op's RefBundle order is preserved.

    Operators each get their own FIFO and are independent, so one operator
    waiting on metadata never blocks another.
    """

    def __init__(
        self,
        *,
        get_objects: Optional[Callable] = None,
        wait_for_objects: Optional[Callable] = None,
        get_object_locations: Optional[Callable] = None,
    ):
        """Create a ThreadedMetadataFetcher.

        Args:
            get_objects: Fetches object values by ref, like ``ray.get``
                (called as ``get_objects(refs, timeout=...)``). Injectable so
                tests can drive the fetch path without a real cluster.
            wait_for_objects: Reports which refs are locally available, like
                ``ray.wait`` (called with ``fetch_local=True``).
            get_object_locations: Returns per-ref location info (including
                ``object_size``), like ``get_local_object_locations``.
        """
        self._get_objects: Callable = get_objects or ray.get
        self._wait_for_objects: Callable = wait_for_objects or ray.wait
        self._get_object_locations: Callable = (
            get_object_locations or get_local_object_locations
        )

        self._request_q: "queue_module.Queue[_Request]" = queue_module.Queue()
        # fetch thread -> executor: meta_ref -> bytes (or captured Exception).
        self._results: Dict["ray.ObjectRef", Any] = {}
        self._results_lock = threading.Lock()

        # Executor-thread-only state below.
        # Pairs deferred by ``in_data_ready_get_object_size`` for the current operator,
        # flushed into the FIFOs by ``submit``.
        self._pending_deferred: List[DeferredEmit] = []
        # Per-operator (keyed by the caller's op key) FIFO of pairs awaiting
        # metadata, in append (= emission) order. Each op's deque is drained
        # front-first so that op's RefBundle emission order is preserved.
        self._fifos: "defaultdict[Hashable, deque[DeferredEmit]]" = defaultdict(deque)
        # Drained (end-of-stream/failed) tasks whose done-callback is postponed
        # until all of their deferred pairs have been emitted. A set so a task
        # re-seen on a later iteration (still pending) isn't registered — or
        # fired — twice.
        self._drained_tasks: Set[DataOpTask] = set()

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
        self._request_q.put(_Signal.STOP)
        if self._started:
            try:
                self._thread.join(timeout=_FETCH_THREAD_JOIN_TIMEOUT_S)
                if self._thread.is_alive():
                    logger.warning(
                        "Metadata-fetch thread did not exit within "
                        f"{_FETCH_THREAD_JOIN_TIMEOUT_S}s; leaving the daemon "
                        "thread behind."
                    )
            except Exception:
                logger.warning(
                    "Failed to join the metadata-fetch thread.", exc_info=True
                )

    def in_data_ready_get_object_size(self, task: DataOpTask) -> Optional[int]:
        block_ref = task.pending_block_ref
        meta_ref = task.pending_meta_ref
        # Output-budget size from the block's local object_size (no RPC).
        # Normally known: the driver owns the just-yielded block ref, so the
        # value (which matches ``meta.size_bytes``) is in the local object
        # directory.
        # TODO: ``object_size`` is the object-store size of the block, which can
        # differ from ``meta.size_bytes`` (the in-memory/logical size). We should
        # add an explicit ``object_size_bytes`` to ``BlockMetadata`` and use it
        # directly, so the fallback below doesn't conflate the two.
        info: Optional[Dict[str, Any]] = self._get_object_locations([block_ref]).get(
            block_ref
        )
        object_size: Optional[int] = (
            info.get("object_size") if info is not None else None
        )
        if object_size is None:
            # Rare: no local size record. Fall back to a short metadata
            # ``ray.get`` for the size. Log once to flag the path without
            # spamming if it recurs.
            if log_once(f"data_object_size_unavailable_{task.operator_name}"):
                logger.warning(
                    "Local object_size unavailable for a block from operator "
                    "'%s'; falling back to its metadata for the output-budget "
                    "size.",
                    task.operator_name,
                )
            try:
                meta_with_schema: BlockMetadataWithSchema = pickle.loads(
                    self._get_objects(meta_ref, timeout=METADATA_WAIT_TIMEOUT_S)
                )
            except ray.exceptions.GetTimeoutError:
                # Metadata isn't local yet either. Leave this pair pending and
                # retry next iteration.
                return None
            # Coalesce a missing size to 0: None is reserved for the "pair not
            # consumed, retry" signal above, and this pair IS consumed
            # (deferred) below — returning None here would defer it twice.
            object_size = meta_with_schema.metadata.size_bytes or 0

        self._pending_deferred.append(
            DeferredEmit(task=task, block_ref=block_ref, meta_ref=meta_ref)
        )
        return object_size

    def submit(self, op_key: Hashable, tasks: List[DataOpTask]) -> None:
        """Queue the current operator's deferred pairs for metadata fetch +
        emission — append them to the op's FIFO (preserving emission order) and
        hand their ``meta_ref``s to the fetch thread — and record any drained
        (end-of-stream/failed) tasks so their done-callback fires once all of
        their deferred pairs have emitted. Must run on the executor thread."""
        deferred = self._pending_deferred
        self._pending_deferred = []
        if deferred:
            fifo = self._fifos[op_key]
            for d in deferred:
                d.task.add_pending_metadata_ref()
                fifo.append(d)
            self._request_q.put([d.meta_ref for d in deferred])
        for task in tasks:
            if task.is_drained():
                self._drained_tasks.add(task)

    def emit_ready_and_fire_done_callbacks(self) -> List[Tuple[str, BaseException]]:
        """Emit whatever's ready (per-op order) then fire postponed done
        callbacks. Returns ``(operator_name, exception)`` for each pair whose
        metadata fetch failed, for the caller's ``max_errored_blocks``
        accounting. Must run on the executor thread."""
        return self._emit_ready() + self._fire_done_callbacks()

    def _emit_ready(self) -> List[Tuple[str, BaseException]]:
        # Emit every pair whose metadata is now available, in per-op append
        # order. A failed fetch is accounted as emitted (so the task can still
        # complete) but its block is dropped and the error is surfaced to the
        # caller rather than raised.
        failures: List[Tuple[str, BaseException]] = []
        for fifo in self._fifos.values():
            while fifo:
                d = fifo[0]
                result = self._pop_result(d.meta_ref)
                if result is _Signal.NOT_READY:
                    # Preserve order: stop at the first pair still in flight;
                    # this operator is retried next call.
                    # TODO: order only needs to be preserved when
                    # ``DataContext.get_current().execution_options.preserve_order``
                    # is True; otherwise we could skip past in-flight pairs and
                    # emit any ready ones.
                    break
                fifo.popleft()
                d.task.mark_emitted()
                if isinstance(result, BaseException):
                    failures.append((d.task.operator_name, result))
                    continue
                try:
                    d.task.produce_block(d.block_ref, result)
                except Exception as e:
                    # Deserializing/emitting the fetched metadata can also fail
                    # (e.g. ``pickle.loads`` raising on a corrupt object). Treat
                    # it as a block-level error and route it through the same
                    # accounting, rather than letting it escape.
                    failures.append((d.task.operator_name, e))
        return failures

    def _fire_done_callbacks(self) -> List[Tuple[str, BaseException]]:
        # Fire postponed done-callbacks for drained tasks whose pairs have all
        # emitted. A failed task fires with its error, which is also surfaced for
        # ``max_errored_blocks`` accounting.
        if not self._drained_tasks:
            return []
        failures: List[Tuple[str, BaseException]] = []
        to_mark_done = [t for t in self._drained_tasks if not t.has_pending_emits()]
        for task in to_mark_done:
            if task.task_error is not None:
                failures.append((task.operator_name, task.task_error))
            task.mark_done()
        self._drained_tasks.difference_update(to_mark_done)
        return failures

    def _pop_result(self, ref: "ray.ObjectRef") -> Any:
        with self._results_lock:
            return self._results.pop(ref, _Signal.NOT_READY)

    def _run(self) -> None:
        # Fetch-thread loop: accumulate requested meta_refs into a pending set
        # and hand them to ``_fetch``, which fetches the locally-available ones
        # and returns those still in flight.
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
                    assert item is _Signal.STOP
                    # ``stop()`` enqueued the STOP sentinel: fast teardown —
                    # drop any in-flight refs and exit. ``stop`` runs after the
                    # scheduling loop (which feeds us) is joined, so there's
                    # nothing left to emit.
                    return
                pending.extend(item)
                try:
                    item = self._request_q.get_nowait()
                except queue_module.Empty:
                    item = None

            if pending:
                pending = self._fetch(pending)

    def _fetch(self, pending: List["ray.ObjectRef"]) -> List["ray.ObjectRef"]:
        """One fetch pass over ``pending``:

        1. ``ray.wait(fetch_local=True)`` pulls the metadata objects to this
           (driver) node and reports which are locally available.
        2. ``ray.get`` the ready refs in one batch (``timeout=0`` — they're
           local) and publish the bytes to ``_results``.
        3. If the batched get raises (it hides which ref failed), fall back to
           per-ref gets to isolate the failure and keep the rest.
        4. Return the refs to retry next pass: the not-yet-local ones, plus any
           that raced out of the local store. A ref that resolved to an error is
           published as that exception for ``_emit_ready`` to surface.
        """
        ready, not_ready = self._wait_for_objects(
            pending,
            num_returns=len(pending),
            timeout=_FETCH_WAIT_TIMEOUT_S,
            fetch_local=True,
        )
        if not ready:
            return not_ready

        retry: List["ray.ObjectRef"] = []
        try:
            values = self._get_objects(ready, timeout=0)
            results: Dict["ray.ObjectRef", Any] = dict(zip(ready, values))
        except Exception:
            # A batched get raises on the first error and hides which ref
            # failed; retry per-ref to isolate it and keep the rest.
            results = {}
            for ref in ready:
                try:
                    results[ref] = self._get_objects(ref, timeout=0)
                except GetTimeoutError:
                    # ray.wait reported it ready but it's no longer local (e.g. a
                    # raced eviction). Re-queue rather than treating it as a
                    # block-level error. Shouldn't be common — log once.
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


def make_metadata_fetcher() -> MetadataFetcher:
    """Build the metadata fetcher for the configured mode (see module
    docstring)."""
    if _PREFETCH_ON_THREAD:
        return ThreadedMetadataFetcher()
    return InlineMetadataFetcher()
