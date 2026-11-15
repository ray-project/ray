import logging
import sys
import threading
import weakref
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Set, Tuple, Union

from ray.experimental.rdt.tensor_transport_manager import (
    CommunicatorMetadata,
    TensorTransportMetadata,
)
from ray.experimental.rdt.util import (
    device_match_transport,
    get_tensor_transport_manager,
)

if TYPE_CHECKING:
    import torch


logger = logging.getLogger(__name__)

# Native int64 upper bound. Per-device logical byte totals are published to the
# core worker as int64; a total outside this range is published as -1 (unknown)
# rather than overflowing the native conversion.
_INT64_MAX = (1 << 63) - 1

# Interval between retries of failed memory-info publishes. A single lazily
# scheduled daemon ``threading.Timer`` is started only while at least one
# publish failure is pending; it retries each pending object once per tick.
# Retries are best-effort: a row is eventually recovered only if the callback
# starts succeeding again (there is no deadline or success guarantee).
_RETRY_INTERVAL_SECONDS = 1.0


def _retry_pending_publishes(store_ref, generation):
    """Weakref trampoline for the retry timer.

    The timer holds only a weak reference to the store (so a standalone store
    with a pending failing publish can still be garbage-collected) and a
    generation token that lets the tick ignore a stale, already-replaced timer.
    """
    store = store_ref()
    if store is not None:
        store._retry_publish_tick(generation)


def _tensor_device_and_bytes(tensor: Any) -> Tuple[str, int]:
    """Return ``(device, logical_bytes)`` for a single tensor.

    The logical byte size is ``numel() * element_size()`` for ``torch.Tensor``
    and ``nbytes`` for ``numpy.ndarray``. The tensor is only inspected if the
    corresponding library is already imported (via ``sys.modules``), so
    importing this module never pulls in optional third-party dependencies.

    The size is an add-time snapshot: a mutable tensor resized after being
    queued is still reported at its queued size. An unrecognized object or a
    metadata read failure is reported as ``("unknown", -1)`` (never 0, never
    raising), so introspection cannot fail a transfer.

    ``sys.modules`` may contain a mocked or partially-initialized ``torch`` or
    ``numpy`` whose ``Tensor``/``ndarray`` attribute is missing, is not a type,
    or raises on access. The whole lookup is therefore guarded so that neither
    attribute access nor ``isinstance`` can escape. Optional dependencies are
    never imported here.
    """
    torch = sys.modules.get("torch")
    if torch is not None:
        try:
            torch_tensor = getattr(torch, "Tensor", None)
            if torch_tensor is not None and isinstance(tensor, torch_tensor):
                num_bytes = int(tensor.numel()) * int(tensor.element_size())
                return str(tensor.device), num_bytes
        except Exception:
            return "unknown", -1

    numpy = sys.modules.get("numpy")
    if numpy is not None:
        try:
            ndarray = getattr(numpy, "ndarray", None)
            if ndarray is not None and isinstance(tensor, ndarray):
                return "cpu", int(tensor.nbytes)
        except Exception:
            return "unknown", -1

    return "unknown", -1


def _compute_copy_memory_info(
    tensors: List[Any],
) -> Tuple[Dict[str, int], Dict[str, int], int]:
    """Compute the per-copy scalar memory contribution of a tensor list.

    Returns ``(device_bytes, device_counts, num_unknown)``: ``device_bytes``
    maps an actual local device string to summed logical bytes (a device with
    only zero-sized tensors has value 0), ``device_counts`` maps device -> number
    of tensors on that device (keeping zero-byte devices present), and
    ``num_unknown`` counts tensors whose device/size could not be determined.
    Aliases and views are counted per copy (logical, not deduplicated).
    """
    device_bytes: Dict[str, int] = {}
    device_counts: Dict[str, int] = {}
    num_unknown = 0
    for tensor in tensors:
        device, num_bytes = _tensor_device_and_bytes(tensor)
        if num_bytes < 0:
            num_unknown += 1
        else:
            device_bytes[device] = device_bytes.get(device, 0) + num_bytes
            device_counts[device] = device_counts.get(device, 0) + 1
    return device_bytes, device_counts, num_unknown


def __ray_send__(
    self,
    obj_id: str,
    tensor_transport_meta: TensorTransportMetadata,
    communicator_meta: CommunicatorMetadata,
    backend: str,
):
    """Helper function that runs on the src actor to send tensors to the dst actor."""
    from ray._private.worker import global_worker

    rdt_store = global_worker.rdt_manager._rdt_store
    assert rdt_store.has_object(obj_id), f"obj_id={obj_id} not found in RDT store"

    tensors = rdt_store.get_object(obj_id)

    tensor_transport_manager = get_tensor_transport_manager(backend)
    tensor_transport_manager.send_multiple_tensors(
        tensors,
        tensor_transport_meta,
        communicator_meta,
    )


def validate_tensor_buffers(
    tensor_buffers: List["torch.Tensor"],
    tensor_meta: List[Tuple["torch.Size", "torch.dtype"]],
):
    if len(tensor_buffers) != len(tensor_meta):
        raise ValueError(
            f"Length of tensor_buffers ({len(tensor_buffers)}) does not match length from object metadata ({len(tensor_meta)})."
        )

    def tensor_buffer_mismatch_msg(prop, idx, actual, expected):
        return f"{prop} of tensor_buffer at index {idx} ({actual}) does not match {prop.lower()} from object metadata ({expected})."

    for idx, single_buffer in enumerate(tensor_buffers):
        shape, dtype = tensor_meta[idx]
        if single_buffer.shape != shape:
            raise ValueError(
                tensor_buffer_mismatch_msg("Shape", idx, single_buffer.shape, shape)
            )
        if single_buffer.dtype != dtype:
            raise ValueError(
                tensor_buffer_mismatch_msg("Dtype", idx, single_buffer.dtype, dtype)
            )
        if not single_buffer.is_contiguous():
            raise ValueError(f"Tensor buffer at index {idx} is not contiguous.")


def __ray_recv__(
    self,
    obj_id: str,
    tensor_transport_meta: TensorTransportMetadata,
    communicator_meta: CommunicatorMetadata,
    backend: str,
):
    """Helper function that runs on the dst actor to receive tensors from the src actor."""
    from ray._private.worker import global_worker

    rdt_store = global_worker.rdt_manager.rdt_store
    try:
        tensor_transport_manager = get_tensor_transport_manager(backend)
        tensors = tensor_transport_manager.recv_multiple_tensors(
            obj_id,
            tensor_transport_meta,
            communicator_meta,
        )
        assert len(tensors) == len(tensor_transport_meta.tensor_meta)
        rdt_store.add_object(obj_id, tensors)
    except Exception as e:
        # Store the error as an RDT object if the recv fails, so waiters will raise the error.
        rdt_store.add_object(obj_id, e)


def __ray_abort_transport__(
    self, obj_id: str, communicator_meta: CommunicatorMetadata, backend: str
):
    """Helper function that can run on an actor doing a send or recv to abort the transport."""
    tensor_transport_manager = get_tensor_transport_manager(backend)
    tensor_transport_manager.abort_transport(obj_id, communicator_meta)


def __ray_free__(
    self,
    obj_id: str,
    tensor_transport_backend: str,
    tensor_transport_meta: TensorTransportMetadata,
):
    try:
        from ray._private.worker import global_worker

        rdt_manager = global_worker.rdt_manager
        rdt_store = rdt_manager.rdt_store

        if not rdt_store.has_object(obj_id):
            return
        tensors = rdt_store.get_object(obj_id)
        try:
            tensor_transport_manager = get_tensor_transport_manager(
                tensor_transport_backend
            )
            tensor_transport_manager.garbage_collect(
                obj_id, tensor_transport_meta, tensors
            )
        except Exception:
            logger.exception(
                "Failed to garbage collect RDT object %s with transport %s.",
                obj_id,
                tensor_transport_backend,
            )
        finally:
            rdt_store.pop_object(obj_id)
    except AssertionError:
        # This could fail if this is a retry and it's already been freed.
        pass


def __ray_fetch_rdt_object__(self, obj_id: str):
    """Helper function that runs on the src actor to fetch tensors from the RDT store via the object store."""
    from ray._private.worker import global_worker

    rdt_store = global_worker.rdt_manager.rdt_store
    rdt_object = rdt_store.wait_and_get_object(obj_id)
    return rdt_object


@dataclass
class _RDTObject:
    # A list of tensors representing the RDT object.
    data: List[Any]
    # Whether the RDT object is the primary copy.
    is_primary: bool
    # If a recv failed, we store the error here.
    error: Optional[Exception] = None
    # Scalar-only memory contribution of this copy, computed once at add time
    # from the actual tensor data, so pop never re-reads (possibly freed or
    # resized) tensors. ``device_bytes`` may contain 0-byte devices; the
    # matching ``device_counts`` entry keeps them present.
    device_bytes: Optional[Dict[str, int]] = None
    device_counts: Optional[Dict[str, int]] = None
    # Number of tensors in this copy whose device/size could not be determined.
    num_unknown: int = 0


@dataclass
class _ObjectMemoryStats:
    """Scalar-only, per-object aggregate that mirrors the native snapshot.

    Holds integers only (never tensors or ObjectRefs). ``device_counts`` tracks
    how many tensors live on each device so a device populated only by
    zero-sized tensors stays present (known 0 bytes) until the last such tensor
    is popped. Device byte totals are kept as full Python ints and only clamped
    to int64 at publish time, so popping a large copy recovers the known
    remaining bytes.
    """

    device_bytes: Dict[str, int] = field(default_factory=dict)
    device_counts: Dict[str, int] = field(default_factory=dict)
    num_unknown: int = 0
    num_copies: int = 0
    num_errors: int = 0


class RDTStore:
    """This class is thread-safe. The GPU object store is meant to be read and
    written by the following threads:
    1. The main thread, which is executing user code. This thread may get, put,
    and pop objects.
    2. The background _ray_system thread, which executes data transfers. This
    thread may get and put objects.
    3. The background CoreWorker server thread, which executes garbage
    collection callbacks that pop objects that are no longer in use.
    4. A lazily-scheduled daemon retry timer thread, which exists only while a
    memory-info publish failure is pending and republishes those objects.
    """

    def __init__(self, memory_info_callback: Optional[Callable] = None):
        # A dictionary that maps from an object ID to a queue of tensor lists.
        #
        # Note: Currently, `_rdt_store` is only supported for Ray Actors.
        self._rdt_store: Dict[str, deque[_RDTObject]] = defaultdict(deque)
        # Mapping from tensor data pointer to the IDs of objects that contain it.
        self._tensor_to_object_ids: Dict[int, Set[str]] = defaultdict[int, Set[str]](
            set
        )
        # Synchronization for the RDT store.
        self._lock = threading.RLock()
        # Signal when an object becomes present in the object store.
        self._object_present_cv = threading.Condition(self._lock)
        # Signal when an object is freed from the object store.
        self._object_freed_cv = threading.Condition(self._lock)

        # Optional callback that publishes scalar-only RDT residency metadata to
        # the core worker (``CoreWorker.set_rdt_object_memory_info``). ``None``
        # disables publishing, which keeps standalone RDTStore usage free of any
        # core-worker dependency and skips tensor introspection entirely.
        self._memory_info_callback = memory_info_callback

        # Per-object scalar aggregates, mutated under `self._lock`. A zero-copy/
        # zero-error aggregate is a "tombstone" retained until the native erase
        # publish succeeds, so a failed final erase can be retried.
        self._object_memory_stats: Dict[str, _ObjectMemoryStats] = {}

        # Object IDs whose most recent publish failed and whose aggregate (live
        # or tombstone) must be republished once the callback recovers. Only
        # scalar object-ID strings are retained here — never tensor data or
        # ObjectRefs. Mutated under `self._lock`.
        self._pending_publish_ids: Set[str] = set()

        # The single lazily-scheduled retry timer, or None when idle. Guarded by
        # `self._lock`; its callback holds only a weak reference to this store.
        self._retry_timer: Optional[threading.Timer] = None
        # Monotonic token identifying the currently scheduled timer generation.
        # A timer tick that fires after it was cancelled and replaced observes a
        # stale token and becomes a no-op (guarded by `self._lock`).
        self._retry_generation: int = 0

    def disable_memory_info_callback(self) -> None:
        """Irrevocably disable memory-info publishing and drop the aggregate.

        Called by ``RDTManager.shutdown`` to detach the publisher from the core
        worker before the core worker is torn down. After this returns, add/pop
        still mutate the store (so native GC callbacks can drain held objects)
        but never call the callback again, and a lazily-created store after
        shutdown starts with no callback so it cannot re-enable publishing.

        Takes ``self._lock`` so it synchronizes with any in-flight add/pop: a
        callback that is mid-publish finishes before the callback is cleared.
        Also cancels the outstanding retry timer and clears pending/tombstone
        state so no late publish can write into a torn-down (or reinitialized)
        runtime. The timer is cancelled, not joined, while holding the lock.
        """
        with self._lock:
            self._memory_info_callback = None
            self._object_memory_stats.clear()
            self._pending_publish_ids.clear()
            self._cancel_retry_timer_locked()

    def has_object(self, obj_id: str) -> bool:
        with self._lock:
            existed = obj_id in self._rdt_store
            if existed:
                return len(self._rdt_store[obj_id]) > 0
            return existed

    def has_tensor(self, tensor: Any) -> bool:
        # Method only used for testing.
        with self._lock:
            return id(tensor) in self._tensor_to_object_ids

    def get_object(self, obj_id: str) -> Optional[List[Any]]:
        with self._lock:
            if self._rdt_store[obj_id][0].error:
                raise self._rdt_store[obj_id][0].error
            return self._rdt_store[obj_id][0].data

    def add_object(
        self,
        obj_id: str,
        rdt_object: Union[List[Any], Exception],
        is_primary: bool = False,
    ):
        """Add an RDT object to the RDT store.

        Args:
            obj_id: The object ID of the RDT object.
            rdt_object: A list of tensors representing the RDT object.
            is_primary: Whether the RDT object is the primary copy.
        """
        with self._object_present_cv:
            if isinstance(rdt_object, Exception):
                self._rdt_store[obj_id].append(
                    _RDTObject([], is_primary, error=rdt_object)
                )
                if self._memory_info_callback is not None:
                    self._ensure_stats(obj_id).num_errors += 1
            else:
                for tensor in rdt_object:
                    self._tensor_to_object_ids[id(tensor)].add(obj_id)
                device_bytes = device_counts = None
                num_unknown = 0
                if self._memory_info_callback is not None:
                    # Compute the scalar contribution once, from the actual
                    # tensor data, and cache it so pop never re-reads tensors.
                    (
                        device_bytes,
                        device_counts,
                        num_unknown,
                    ) = _compute_copy_memory_info(rdt_object)
                    stats = self._ensure_stats(obj_id)
                    stats.num_copies += 1
                    for device, num_bytes in device_bytes.items():
                        stats.device_bytes[device] = (
                            stats.device_bytes.get(device, 0) + num_bytes
                        )
                        stats.device_counts[device] = (
                            stats.device_counts.get(device, 0) + device_counts[device]
                        )
                    stats.num_unknown += num_unknown
                # Append to the queue instead of overwriting
                self._rdt_store[obj_id].append(
                    _RDTObject(
                        rdt_object,
                        is_primary,
                        device_bytes=device_bytes,
                        device_counts=device_counts,
                        num_unknown=num_unknown,
                    )
                )
            self._publish_memory_info(obj_id)
            self._object_present_cv.notify_all()

    def add_object_primary(
        self, obj_id: str, tensors: List[Any], tensor_transport: str
    ) -> TensorTransportMetadata:
        with self._object_present_cv:
            # A primary entry may already exist from a prior attempt of the
            # same task (e.g., a task that succeeded and populated the RDT
            # store but whose reply was lost, then got retried). Keep the
            # existing primary — do not re-store — and return metadata
            # derived from it so the metadata matches what `__ray_send__`
            # will actually transmit.
            queue = self._rdt_store.get(obj_id)
            if queue:
                tensors_to_describe = queue[0].data
            else:
                self.add_object(obj_id, tensors, is_primary=True)
                tensors_to_describe = tensors

        tensor_transport_manager = get_tensor_transport_manager(tensor_transport)
        tensor_transport_meta = (
            tensor_transport_manager.extract_tensor_transport_metadata(
                obj_id, tensors_to_describe
            )
        )

        if tensor_transport_meta.tensor_meta and not device_match_transport(
            tensor_transport_meta.tensor_device, tensor_transport
        ):
            raise ValueError(
                f"Tensor transport backend {tensor_transport} does not support "
                f"tensor transfer on device {tensor_transport_meta.tensor_device}."
            )

        return tensor_transport_meta

    def is_primary_copy(self, obj_id: str) -> bool:
        with self._lock:
            return self.has_object(obj_id) and self._rdt_store[obj_id][0].is_primary

    def wait_and_get_object(
        self, obj_id: str, timeout: Optional[float] = None
    ) -> List[Any]:
        """Atomically waits for the RDT object to be present in the RDT
        store, then gets it. If the object is not present after the optional
        timeout, raise a TimeoutError.

        Args:
            obj_id: The object ID to wait for.
            timeout: The maximum time in seconds to wait for the object to be
                present in the RDT store. If not specified, wait indefinitely.

        Returns:
            The tensors in the RDT object.
        """
        with self._lock:
            self._wait_object(obj_id, timeout)
            return self.get_object(obj_id)

    def wait_and_pop_object(
        self, obj_id: str, timeout: Optional[float] = None
    ) -> List[Any]:
        """Atomically waits for the RDT object to be present in the RDT
        store, then pops it.  If the object is not present after the optional
        timeout, raise a TimeoutError.

        Args:
            obj_id: The object ID to wait for.
            timeout: The maximum time in seconds to wait for the object to be
                present in the RDT store. If not specified, wait indefinitely.

        Returns:
            The RDT object.
        """
        with self._lock:
            self._wait_object(obj_id, timeout)
            return self.pop_object(obj_id)

    def _wait_object(self, obj_id: str, timeout: Optional[float] = None) -> None:
        """Helper method to wait for the RDT object to be present in the RDT store.
        If the object is not present after the optional timeout, raise a
        TimeoutError.

        Args:
            obj_id: The object ID to wait for.
            timeout: The maximum time in seconds to wait for the object to be
                present in the RDT store. If not specified, wait indefinitely.
        """
        with self._object_present_cv:
            if not self._object_present_cv.wait_for(
                lambda: self.has_object(obj_id),
                timeout=timeout,
            ):
                raise TimeoutError(
                    f"ObjectRef({obj_id}) not found in RDT object store after {timeout}s, transfer may have failed. Please report this issue on GitHub: https://github.com/ray-project/ray/issues/new/choose"
                )

    def pop_object(self, obj_id: str) -> List[Any]:
        with self._lock:
            queue = self._rdt_store.get(obj_id)
            assert queue is not None, f"obj_id={obj_id} not found in RDT store"
            rdt_object = queue.popleft()
            if len(queue) == 0:
                del self._rdt_store[obj_id]
            if rdt_object.error:
                # Publish the removal and notify waiters before raising, so a
                # waiter never observes a queued error as still resident.
                self._pop_object_stats(obj_id, rdt_object)
                self._object_freed_cv.notify_all()
                raise rdt_object.error
            for tensor in rdt_object.data:
                self._tensor_to_object_ids[id(tensor)].remove(obj_id)
                if len(self._tensor_to_object_ids[id(tensor)]) == 0:
                    self._tensor_to_object_ids.pop(id(tensor))
            self._pop_object_stats(obj_id, rdt_object)
            self._object_freed_cv.notify_all()
            return rdt_object.data

    def _ensure_stats(self, obj_id: str) -> _ObjectMemoryStats:
        stats = self._object_memory_stats.get(obj_id)
        if stats is None:
            stats = _ObjectMemoryStats()
            self._object_memory_stats[obj_id] = stats
        return stats

    def _pop_object_stats(self, obj_id: str, rdt_object: _RDTObject) -> None:
        """Decrement the per-object aggregate for a popped copy, then publish.

        ``rdt_object.device_bytes`` / ``device_counts`` / ``num_unknown`` are the
        values cached at add time, so this never re-reads (possibly freed or
        resized) tensors. Once the last copy and last error are gone the
        aggregate reaches its zero-copy/zero-error state, which the next publish
        erases from the native snapshot. That zero-state tombstone is retained
        here (not deleted) so a failed final erase can be retried; it is dropped
        by :meth:`_publish_memory_info` once the erase succeeds.

        ``rdt_object.device_bytes`` / ``device_counts`` may be ``None`` only when
        publishing was disabled at add time. The callback is constructor-injected
        and the lifecycle only ever disables it (one-way shutdown, never
        re-enables), so the ``callback is None`` early return above runs first in
        that state and the fields are never accessed; no per-field ``None`` guard
        is needed.
        """
        if self._memory_info_callback is None:
            return
        stats = self._object_memory_stats.get(obj_id)
        if stats is None:
            return
        if rdt_object.error:
            stats.num_errors -= 1
        else:
            stats.num_copies -= 1
            for device, num_bytes in rdt_object.device_bytes.items():
                remaining_count = stats.device_counts.get(device, 0) - (
                    rdt_object.device_counts[device]
                )
                if remaining_count <= 0:
                    stats.device_counts.pop(device, None)
                    stats.device_bytes.pop(device, None)
                else:
                    stats.device_counts[device] = remaining_count
                    stats.device_bytes[device] -= num_bytes
            stats.num_unknown -= rdt_object.num_unknown
        self._publish_memory_info(obj_id)

    def _publish_memory_info(self, obj_id: str) -> None:
        """Publish the current per-object aggregate to the core worker.

        Called while holding ``self._lock`` so concurrent add/pop (and the retry
        timer) can't reorder native snapshots. The callback receives scalar
        values only and takes its own native mutex without re-entering Python,
        so holding the store lock here is safe. Per-device totals outside int64
        range are published as -1 (unknown). When both counts are zero the
        native entry is erased.

        Publishing is best-effort local metadata: a failing callback is logged
        (once, on the transition into the failed state) and swallowed so it can
        neither fail a store mutation that already succeeded nor block the
        condition-variable notification that follows. On failure the aggregate
        is retained and ``obj_id`` is marked pending so the lazily-scheduled
        retry timer republishes it (eventual consistency only if the callback
        recovers). A successful zero-state publish drops the tombstone.
        """
        if self._memory_info_callback is None:
            # Publishing is disabled: nothing to retry.
            self._pending_publish_ids.discard(obj_id)
            return
        stats = self._object_memory_stats.get(obj_id)
        if stats is None:
            device_bytes = []
            num_copies = 0
            num_errors = 0
        else:
            device_bytes = []
            for device, total in stats.device_bytes.items():
                if total < 0 or total > _INT64_MAX:
                    device_bytes.append((device, -1))
                else:
                    device_bytes.append((device, total))
            if stats.num_unknown > 0:
                device_bytes.append(("unknown", -1))
            num_copies = stats.num_copies
            num_errors = stats.num_errors
        object_id_bytes = bytes.fromhex(obj_id)
        try:
            self._memory_info_callback(
                object_id_bytes, device_bytes, num_copies, num_errors
            )
        except Exception as exc:
            # Retain the aggregate (live or tombstone) and mark the object for
            # retry. Log only on the transition into the failed state so a
            # persistently failing callback doesn't spam one line per tick. Log
            # the message, not the traceback, so a retained log record can't
            # keep caller frames (and any tensor data) alive via ``f_back``.
            if obj_id not in self._pending_publish_ids:
                logger.error(
                    "Failed to publish RDT memory info for object %s: %s; "
                    "will retry while the store is alive.",
                    obj_id,
                    str(exc),
                )
            self._pending_publish_ids.add(obj_id)
            self._schedule_retry_locked()
            return
        # Publish succeeded.
        if obj_id in self._pending_publish_ids:
            self._pending_publish_ids.discard(obj_id)
            logger.debug("Recovered RDT memory info publish for object %s.", obj_id)
        if num_copies == 0 and num_errors == 0:
            # A successful zero-state (erase) publish drops the tombstone; a
            # live aggregate is retained for future add/pop.
            self._object_memory_stats.pop(obj_id, None)
        if not self._pending_publish_ids:
            # No failures remain: the timer (if any) is no longer needed.
            self._cancel_retry_timer_locked()
        else:
            # Other failures may still be pending with no timer armed (e.g. an
            # earlier timer start failed): re-arm it. No-ops if one is active.
            self._schedule_retry_locked()

    def _schedule_retry_locked(self) -> None:
        """Schedule the single retry timer if one is needed.

        Caller holds ``self._lock``. At most one timer exists at a time; it is a
        daemon so it never blocks interpreter shutdown, and it holds only a weak
        reference to this store. Starting the thread can fail (e.g. interpreter
        shutdown); that failure is non-fatal: the scalar pending state is kept so
        a later mutation retries scheduling.
        """
        if self._retry_timer is not None or not self._pending_publish_ids:
            return
        self._retry_generation += 1
        generation = self._retry_generation
        try:
            timer = threading.Timer(
                _RETRY_INTERVAL_SECONDS,
                _retry_pending_publishes,
                args=(weakref.ref(self), generation),
            )
            timer.daemon = True
            self._retry_timer = timer
            timer.start()
        except RuntimeError as exc:
            self._retry_timer = None
            logger.error(
                "Failed to start the RDT memory-info retry timer: %s", str(exc)
            )

    def _cancel_retry_timer_locked(self) -> None:
        """Cancel and drop the retry timer without joining it.

        Caller holds ``self._lock``. If the timer is already past its interval it
        may still run one final tick, but that tick is a stale generation (or
        sees empty pending / a cleared callback) and becomes a no-op.
        """
        if self._retry_timer is not None:
            self._retry_timer.cancel()
            self._retry_timer = None

    def _retry_publish_tick(self, generation: int) -> None:
        """Run one retry tick on the timer thread.

        Holds ``self._lock`` for the whole tick so pending/timer state stays
        serialized with normal mutations. A tick whose generation token no
        longer matches the active timer, or that fires after its timer was
        cancelled without replacement, is stale and no-ops. Otherwise each
        pending object is republished once, re-reading its current aggregate
        under the lock (so a reused object ID publishes its latest live state,
        never a stale erase); a successor timer is scheduled only after this
        tick finishes.
        """
        with self._lock:
            if generation != self._retry_generation or self._retry_timer is None:
                return
            for obj_id in list(self._pending_publish_ids):
                self._publish_memory_info(obj_id)
            # This tick's timer has now fired; schedule a successor only if
            # pending IDs remain.
            self._retry_timer = None
            self._schedule_retry_locked()

    def wait_tensor_freed(self, tensor: Any, timeout: Optional[float] = None) -> None:
        """Wait for the object to be freed from the RDT store."""
        with self._object_freed_cv:
            if not self._object_freed_cv.wait_for(
                lambda: id(tensor) not in self._tensor_to_object_ids,
                timeout=timeout,
            ):
                raise TimeoutError(
                    f"Tensor {tensor} not freed from RDT object store after {timeout}s. The tensor will not be freed until all ObjectRefs containing the tensor have gone out of scope."
                )

    def get_num_objects(self) -> int:
        """Return the number of objects in the RDT store."""
        with self._lock:
            # Count total objects across all queues
            return sum(len(queue) for queue in self._rdt_store.values())
