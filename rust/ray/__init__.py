# Copyright 2024 The Ray Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0

"""Ray Python API for the Rust backend.

Provides the standard ``import ray`` interface backed by the Rust
``_raylet`` extension module.  Supports:

    ray.init()
    @ray.remote       -- functions and classes
    ray.method()      -- method options (tensor_transport)
    ray.put / get / wait
    ray.get_actor
    ray.kill
    ray.shutdown

Example::

    import ray
    ray.init(num_task_workers=4)

    @ray.remote
    def square(x):
        return x * x

    @ray.remote
    class Counter:
        def __init__(self):
            self.n = 0
        def incr(self, delta=1):
            self.n += delta
            return self.n

    print(ray.get(square.remote(5)))         # 25
    c = Counter.remote()
    print(ray.get(c.incr.remote(10)))        # 10
    ray.shutdown()
"""

from __future__ import annotations

import multiprocessing as _mp
import pickle as _pickle
import threading as _threading
import weakref
from typing import Any, List, Optional, Sequence, Union

from _raylet import (
    start_cluster as _start_cluster,
    PyCoreWorker as _PyCoreWorker,
    PyGcsClient as _PyGcsClient,
    PyWorkerID as _PyWorkerID,
)

# Re-export _raylet as ray._raylet for convenience
from _raylet import (
    start_cluster,
    PyCoreWorker,
    PyGcsClient,
    PyWorkerID,
    PyObjectID,
    PyActorID,
    PyTaskID,
    PyNodeID,
    PyJobID,
    PyObjectRef,
    PyPlacementGroupID,
)

# Import exceptions sub-module so ray.exceptions.X works
from ray import exceptions

__version__ = "3.0.0.dev0"

__all__ = [
    "__version__",
    "init",
    "shutdown",
    "is_initialized",
    "remote",
    "method",
    "put",
    "get",
    "wait",
    "get_actor",
    "kill",
    "ObjectRef",
    "ActorHandle",
    "exceptions",
]


# =====================================================================
# ray.method() decorator
# =====================================================================


def method(*, tensor_transport=None, concurrency_group=None):
    """Decorator for actor methods to specify transport options.

    Usage::

        @ray.method(tensor_transport="nixl")
        def echo(self, data, device):
            return data.to(device)
    """
    def decorator(fn):
        if tensor_transport:
            fn.__ray_tensor_transport__ = tensor_transport.upper()
        if concurrency_group:
            fn.__ray_concurrency_group__ = concurrency_group
        return fn
    return decorator


# =====================================================================
# ObjectRef
# =====================================================================


class ObjectRef:
    """Reference to an object in the distributed object store."""

    __slots__ = (
        "_binary", "_owner",
        # RDT metadata (set only for tensor-transport results)
        "_rdt_source",      # ActorHandle that produced the tensor
        "_rdt_transport",   # e.g. "NIXL"
        "_rdt_meta",        # NixlTransportMetadata or dict
        "_rdt_obj_id",      # hex string object ID for NIXL tracking
        "_target_tensors",  # pre-allocated buffers for set_target_for_ref
        "_rdt_pending",     # threading.Event for async metadata resolution
        "_rdt_error",       # Exception if metadata resolution failed
        "_rdt_result_info", # dict with is_dict, dict_keys, etc.
        "_ref_id",          # stable unique ID assigned at creation (never changes)
        "__weakref__",
    )

    def __init__(self, binary, owner: _PyCoreWorker):
        # Normalize to bytes (PyO3 Vec<u8> may come back as list)
        self._binary = bytes(binary) if isinstance(binary, list) else binary
        self._owner = owner
        self._rdt_source = None
        self._rdt_transport = None
        self._rdt_meta = None
        self._rdt_obj_id = None
        self._target_tensors = None
        self._rdt_pending = None   # threading.Event for async metadata resolution
        self._rdt_error = None     # Exception if metadata resolution failed
        self._rdt_result_info = None  # dict with is_dict, dict_keys, etc.
        # Stable unique ID that never changes (for hex(), GC tracking, etc.)
        self._ref_id = self._binary

    def hex(self) -> str:
        """Return a stable hex ID (assigned at creation, never changes)."""
        b = bytes(self._ref_id) if isinstance(self._ref_id, list) else self._ref_id
        return b.hex()

    def __repr__(self) -> str:
        b = bytes(self._binary) if isinstance(self._binary, list) else self._binary
        return f"ObjectRef({b[:8].hex()}...)"

    def __eq__(self, other: object) -> bool:
        return isinstance(other, ObjectRef) and self._binary == other._binary

    def __hash__(self) -> int:
        return hash(self._binary)

    def __getstate__(self):
        # If metadata resolution is pending, wait for it
        if self._rdt_pending is not None:
            self._rdt_pending.wait(timeout=120)
        return {
            "binary": self._binary,
            "rdt_meta": self._rdt_meta,
            "rdt_transport": self._rdt_transport,
            "rdt_obj_id": self._rdt_obj_id,
            "rdt_result_info": self._rdt_result_info,
        }

    def __setstate__(self, state):
        if isinstance(state, dict):
            self._binary = state["binary"]
            self._rdt_meta = state.get("rdt_meta")
            self._rdt_transport = state.get("rdt_transport")
            self._rdt_obj_id = state.get("rdt_obj_id")
            self._rdt_result_info = state.get("rdt_result_info")
        else:
            self._binary = state
            self._rdt_meta = None
            self._rdt_transport = None
            self._rdt_obj_id = None
            self._rdt_result_info = None
        self._owner = None
        self._rdt_source = None
        self._target_tensors = None
        self._rdt_pending = None
        self._rdt_error = None
        self._ref_id = self._binary

    def _wait_pending(self, timeout=120):
        """Wait for pending metadata resolution."""
        if self._rdt_pending is not None:
            self._rdt_pending.wait(timeout=timeout)
        if self._rdt_error is not None:
            from ray.exceptions import ActorDiedError
            raise self._rdt_error

    def __del__(self):
        # Trigger GC on source actor when ref goes out of scope
        if self._rdt_obj_id is not None and self._rdt_source is not None:
            try:
                _schedule_gc(self._rdt_obj_id, self._rdt_source, self._rdt_meta,
                             ref_hex=self.hex())
            except Exception:
                pass
        elif (self._rdt_transport == "NIXL" and self._rdt_source is not None
              and self._rdt_pending is not None):
            # Metadata resolution is pending — schedule delayed GC
            pending = self._rdt_pending
            source = self._rdt_source
            ref_hex = self.hex()

            def _delayed_gc():
                try:
                    pending.wait(timeout=120)
                    rt = _runtime
                    if not rt._initialized:
                        return
                    driver = rt.driver
                    if driver is None:
                        return
                    # Get real binary from mapping (set by _submit_and_resolve)
                    with _ref_id_to_binary_lock:
                        binary = _ref_id_to_binary.pop(ref_hex, None)
                    if binary is None:
                        return
                    # Fetch metadata from object store to get rdt_obj_id
                    data = driver.get([binary], 10000)
                    if data and data[0] is not None:
                        result = _pickle.loads(bytes(data[0][0]))
                        if isinstance(result, dict) and result.get("__nixl_rdt__"):
                            rdt_obj_id = result.get("rdt_obj_id")
                            if rdt_obj_id:
                                _schedule_gc(rdt_obj_id, source, None,
                                             ref_hex=ref_hex)
                except Exception:
                    pass

            t = _threading.Thread(target=_delayed_gc, daemon=True)
            t.start()


def _schedule_gc(obj_id_hex, source_handle, meta, ref_hex=None):
    """Schedule garbage collection on the source actor for an RDT object."""
    # Check if we're in the worker that owns this object (same-process GC)
    from ray._private.worker import global_worker
    if global_worker.rdt_manager is not None:
        rdt_mgr = global_worker.rdt_manager
        if rdt_mgr.is_managed_object(obj_id_hex):
            # Same-process: free directly
            rdt_mgr.free_object(obj_id_hex)
            return

    rt = _runtime
    if not rt._initialized:
        return
    driver = rt.driver
    if driver is None:
        return

    def _do_gc():
        try:
            ser = [_pickle.dumps(obj_id_hex), _pickle.dumps(meta)]
            oid = driver.submit_actor_method(
                source_handle._actor_id, "__ray_free__", ser
            )
            driver.get([oid.binary()], 10000)
        except Exception:
            pass
        # Remove from driver rdt_manager
        if ref_hex:
            try:
                if global_worker.rdt_manager is not None:
                    global_worker.rdt_manager.free_driver_tracking(ref_hex)
            except Exception:
                pass

    t = _threading.Thread(target=_do_gc, daemon=True)
    t.start()


# Map ref_id hex → real binary for delayed GC (set when async submit completes)
_ref_id_to_binary: dict = {}
_ref_id_to_binary_lock = _threading.Lock()

# Shared thread pool for async actor method dispatch
from concurrent.futures import ThreadPoolExecutor as _ThreadPoolExecutor
_dispatch_pool = _ThreadPoolExecutor(max_workers=256, thread_name_prefix="ray-dispatch")

# Track pending metadata resolutions per actor for kill() signaling
_pending_nixl_refs: dict = {}  # actor_id_key -> list of (event, ref_weakref)
_pending_nixl_lock = _threading.Lock()


# =====================================================================
# RemoteFunction
# =====================================================================


class RemoteFunction:
    """Callable wrapper returned by ``@ray.remote`` on a function."""

    def __init__(self, func, options: dict | None = None):
        self._func = func
        self._options = options or {}
        self.__name__ = func.__name__
        self.__doc__ = func.__doc__
        self.__module__ = func.__module__

    def remote(self, *args: Any) -> ObjectRef:
        """Submit the function as a remote task."""
        rt = _runtime
        serialized = [_pickle.dumps(a) for a in args]
        _, _, _, task_driver = rt._pick_task_worker()
        num_returns = self._options.get("num_returns", 1)
        max_retries = self._options.get("max_retries", 0)
        refs = task_driver.submit_task(
            self._func.__name__, serialized,
            num_returns=num_returns, max_retries=max_retries,
        )
        if num_returns == 1:
            return ObjectRef(refs[0].binary(), task_driver)
        return [ObjectRef(r.binary(), task_driver) for r in refs]

    def options(self, **kwargs) -> RemoteFunction:
        """Return a copy with overridden options."""
        merged = {**self._options, **kwargs}
        return RemoteFunction(self._func, merged)


# =====================================================================
# Actor classes
# =====================================================================


class _ActorMethodHandle:
    """Proxy for ``actor.method`` that provides ``.remote()``."""

    __slots__ = ("_handle", "_method", "_options")

    def __init__(self, handle: ActorHandle, method: str, options: dict = None):
        self._handle = handle
        self._method = method
        self._options = options or {}

    def _ensure_actor_setup(self, driver):
        """Ensure the target actor is set up on this core_worker."""
        h = self._handle
        if h._port is not None and _runtime.driver is None:
            # Worker process — need to register the target actor endpoint
            try:
                driver.setup_actor(
                    h._actor_id, h._name, h._namespace or "default",
                    "127.0.0.1", h._port, h._node_id, h._worker_id,
                )
            except Exception:
                pass  # May already be set up

    def remote(self, *args: Any) -> ObjectRef:
        driver = _runtime.driver
        if driver is None:
            # Worker process — use the worker's core_worker
            from ray._private.worker import global_worker
            driver = global_worker._core_worker
            self._ensure_actor_setup(driver)

        # Check if any argument is an ObjectRef with RDT transport (NIXL)
        rdt_refs = []
        deferred_rdt = False
        for i, arg in enumerate(args):
            if isinstance(arg, ObjectRef) and arg._rdt_transport == "NIXL":
                if arg._rdt_meta is not None:
                    # Metadata already resolved → use NIXL dispatch
                    rdt_refs.append((i, arg))
                elif arg._rdt_pending is not None:
                    # Don't block — use deferred dispatch so ray.kill() can proceed
                    deferred_rdt = True
                    rdt_refs.append((i, arg))

        if rdt_refs:
            if deferred_rdt:
                return self._remote_rdt_nixl_deferred(args, rdt_refs, driver)
            return self._remote_rdt_nixl(args, rdt_refs, driver)

        # Check for old-style NCCL RDT refs
        nccl_refs = []
        for i, arg in enumerate(args):
            if isinstance(arg, ObjectRef) and arg._rdt_transport == "NCCL":
                nccl_refs.append((i, arg))

        if nccl_refs:
            return self._remote_rdt_nccl(args, nccl_refs, driver)

        # Resolve non-NIXL ObjectRef args on driver side
        # (workers can't cross-fetch from each other's object stores)
        resolved_args = list(args)
        for i, arg in enumerate(args):
            if isinstance(arg, ObjectRef):
                try:
                    resolved_args[i] = _runtime.get(arg, timeout=60.0)
                except Exception:
                    pass
        args = tuple(resolved_args)

        serialized = [_pickle.dumps(a) for a in args]

        # Check if this method has tensor_transport
        cls = self._handle._remote_cls
        method_fn = getattr(cls, self._method, None) if cls is not None else None
        transport = getattr(method_fn, "__ray_tensor_transport__", None) if method_fn else None
        is_gpu_actor = cls is not None and getattr(
            cls, "__ray_enable_tensor_transport__", False
        )

        if is_gpu_actor and transport and transport == "NIXL":
            # NIXL method on GPU actor: submit + resolve in one background thread
            import os as _os
            ref = ObjectRef(_os.urandom(16), None)
            ref._rdt_source = self._handle
            ref._rdt_transport = "NIXL"
            event = _threading.Event()
            ref._rdt_pending = event
            ref._rdt_result_info = None
            handle = self._handle

            actor_id_key = bytes(handle._actor_id.binary()) if hasattr(handle._actor_id, 'binary') else handle._actor_id
            if not isinstance(actor_id_key, bytes):
                actor_id_key = bytes(actor_id_key)
            ref_wr = weakref.ref(ref)
            with _pending_nixl_lock:
                _pending_nixl_refs.setdefault(actor_id_key, []).append(
                    (event, ref_wr)
                )

            _ref_id_hex = ref.hex()  # stable hex for tracking

            def _submit_and_resolve():
                try:
                    oid = driver.submit_actor_method(
                        handle._actor_id, self._method, serialized
                    )
                    real_binary = oid.binary()
                    ref_hex = _ref_id_hex
                    # Store mapping for delayed GC (even if ref is dead)
                    with _ref_id_to_binary_lock:
                        _ref_id_to_binary[ref_hex] = real_binary

                    r = ref_wr()
                    if r is not None:
                        r._binary = real_binary
                        r._owner = driver

                    # Now fetch the NIXL metadata from the result
                    data = driver.get([real_binary], 120000)
                    if data and data[0] is not None:
                        result = _pickle.loads(bytes(data[0][0]))
                        if isinstance(result, dict) and result.get("__nixl_rdt__"):
                            from ray.experimental.rdt.nixl_tensor_transport import (
                                NixlTransportMetadata,
                            )
                            meta = NixlTransportMetadata(
                                tensor_meta=result["tensor_meta"],
                                tensor_device=result.get("tensor_device"),
                                nixl_serialized_descs=result.get("nixl_serialized_descs"),
                                nixl_agent_meta=result.get("nixl_agent_meta"),
                                nixl_agent_name=result.get("nixl_agent_name"),
                                nixl_agent_meta_version=result.get("nixl_agent_meta_version", 0),
                            )
                            r2 = ref_wr()
                            if r2 is not None:
                                r2._rdt_meta = meta
                                r2._rdt_obj_id = result.get("rdt_obj_id")
                                r2._rdt_result_info = {
                                    "is_dict": result.get("is_dict"),
                                    "dict_keys": result.get("dict_keys"),
                                    "is_single_tensor": result.get("is_single_tensor"),
                                }
                            # Track in driver rdt_manager (even if ref is dead)
                            from ray._private.worker import global_worker
                            if global_worker.rdt_manager is not None:
                                global_worker.rdt_manager.track_driver_object(ref_hex, result.get("rdt_obj_id"))
                    else:
                        r2 = ref_wr()
                        if r2 is not None:
                            r2._rdt_error = exceptions.ActorDiedError()
                except Exception:
                    r3 = ref_wr()
                    if r3 is not None:
                        r3._rdt_error = exceptions.ActorDiedError()
                finally:
                    event.set()
                    with _pending_nixl_lock:
                        entries = _pending_nixl_refs.get(actor_id_key, [])
                        _pending_nixl_refs[actor_id_key] = [
                            (e, w) for e, w in entries if e is not event
                        ]

            _dispatch_pool.submit(_submit_and_resolve)
            return ref

        elif is_gpu_actor:
            # Non-NIXL method on GPU actor: async dispatch
            import os as _os
            ref = ObjectRef(_os.urandom(16), None)
            ref._rdt_pending = _threading.Event()
            handle = self._handle
            method = self._method

            actor_id_key = bytes(handle._actor_id.binary()) if hasattr(handle._actor_id, 'binary') else handle._actor_id
            if not isinstance(actor_id_key, bytes):
                actor_id_key = bytes(actor_id_key)
            ref_wr = weakref.ref(ref)
            with _pending_nixl_lock:
                _pending_nixl_refs.setdefault(actor_id_key, []).append(
                    (ref._rdt_pending, ref_wr)
                )

            _gpu_ref_hex = ref.hex()  # stable hex

            def _async_submit():
                try:
                    oid = driver.submit_actor_method(
                        handle._actor_id, method, serialized
                    )
                    r = ref_wr()
                    if r is not None:
                        r._binary = oid.binary()
                        r._owner = driver
                        with _ref_id_to_binary_lock:
                            _ref_id_to_binary[_gpu_ref_hex] = oid.binary()
                except Exception:
                    r = ref_wr()
                    if r is not None:
                        r._rdt_error = exceptions.ActorDiedError()
                finally:
                    ref._rdt_pending.set()
                    with _pending_nixl_lock:
                        entries = _pending_nixl_refs.get(actor_id_key, [])
                        _pending_nixl_refs[actor_id_key] = [
                            (ev, w) for ev, w in entries
                            if ev is not ref._rdt_pending
                        ]

            _dispatch_pool.submit(_async_submit)
        else:
            # All actor method calls are async to match Ray API semantics.
            # submit_actor_method is synchronous (blocks until the method
            # completes), so we dispatch in a background thread and return
            # an ObjectRef immediately.
            import os as _os
            ref = ObjectRef(_os.urandom(16), None)
            ref._rdt_pending = _threading.Event()
            handle = self._handle
            method = self._method
            ref_wr = weakref.ref(ref)

            _cpu_ref_hex = ref.hex()  # stable hex

            def _async_cpu_submit():
                try:
                    oid = driver.submit_actor_method(
                        handle._actor_id, method, serialized
                    )
                    r = ref_wr()
                    if r is not None:
                        r._binary = oid.binary()
                        r._owner = driver
                        with _ref_id_to_binary_lock:
                            _ref_id_to_binary[_cpu_ref_hex] = oid.binary()
                except Exception:
                    r = ref_wr()
                    if r is not None:
                        r._rdt_error = exceptions.ActorDiedError()
                finally:
                    ref._rdt_pending.set()

            _dispatch_pool.submit(_async_cpu_submit)

        # Check if this method has tensor_transport → mark the ref as RDT
        # (already handled for GPU actor NIXL methods above)
        if cls is not None and not (is_gpu_actor and transport and transport == "NIXL"):
            transport = getattr(method_fn, "__ray_tensor_transport__", None) if method_fn else None
            if transport and transport == "NIXL":
                ref._rdt_source = self._handle
                ref._rdt_transport = "NIXL"
                # Capture the original pending event (from _async_cpu_submit)
                # BEFORE overwriting it with the metadata resolution event
                submit_event = ref._rdt_pending
                # Resolve metadata in background thread (non-blocking)
                event = _threading.Event()
                ref._rdt_pending = event
                ref_hex = ref.hex()
                handle = self._handle

                # Track for kill() signaling
                actor_id_key = bytes(handle._actor_id.binary()) if hasattr(handle._actor_id, 'binary') else handle._actor_id
                ref_wr = weakref.ref(ref)
                with _pending_nixl_lock:
                    _pending_nixl_refs.setdefault(actor_id_key, []).append((event, ref_wr))

                def _resolve_meta():
                    try:
                        # Wait for the real binary to be set by _async_submit
                        if submit_event is not None:
                            submit_event.wait(timeout=120)
                        r0 = ref_wr()
                        if r0 is None:
                            return
                        binary = r0._binary
                        data = driver.get([binary], 120000)
                        r = ref_wr()
                        if r is None:
                            # Ref was already GC'd, but still need to track metadata
                            if data and data[0] is not None:
                                result = _pickle.loads(bytes(data[0][0]))
                                if isinstance(result, dict) and result.get("__nixl_rdt__"):
                                    # Store in driver rdt_manager for tracking
                                    from ray._private.worker import global_worker
                                    if global_worker.rdt_manager is not None:
                                        rdt_obj_id = result.get("rdt_obj_id")
                                        global_worker.rdt_manager.track_driver_object(ref_hex, rdt_obj_id)
                            return
                        if data and data[0] is not None:
                            result = _pickle.loads(bytes(data[0][0]))
                            if isinstance(result, dict) and result.get("__nixl_rdt__"):
                                from ray.experimental.rdt.nixl_tensor_transport import (
                                    NixlTransportMetadata,
                                )
                                meta = NixlTransportMetadata(
                                    tensor_meta=result["tensor_meta"],
                                    tensor_device=result.get("tensor_device"),
                                    nixl_serialized_descs=result.get("nixl_serialized_descs"),
                                    nixl_agent_meta=result.get("nixl_agent_meta"),
                                    nixl_agent_name=result.get("nixl_agent_name"),
                                    nixl_agent_meta_version=result.get("nixl_agent_meta_version", 0),
                                )
                                r._rdt_meta = meta
                                r._rdt_obj_id = result.get("rdt_obj_id")
                                r._rdt_result_info = {
                                    "is_dict": result.get("is_dict"),
                                    "dict_keys": result.get("dict_keys"),
                                    "is_single_tensor": result.get("is_single_tensor"),
                                }
                                # Track in driver rdt_manager
                                from ray._private.worker import global_worker
                                if global_worker.rdt_manager is not None:
                                    global_worker.rdt_manager.track_driver_object(ref_hex, result.get("rdt_obj_id"))
                        else:
                            # Get returned None — actor likely died
                            r._rdt_error = exceptions.ActorDiedError()
                    except Exception:
                        r2 = ref_wr()
                        if r2 is not None:
                            r2._rdt_error = exceptions.ActorDiedError()
                    finally:
                        event.set()
                        # Clean up pending tracking
                        with _pending_nixl_lock:
                            entries = _pending_nixl_refs.get(actor_id_key, [])
                            _pending_nixl_refs[actor_id_key] = [
                                (e, w) for e, w in entries if e is not event
                            ]

                t = _threading.Thread(target=_resolve_meta, daemon=True)
                t.start()

            elif transport and transport == "NCCL":
                # Legacy NCCL path — wait for async submit to complete,
                # then fetch NCCL metadata from the result
                from ray.rdt import _is_rdt_meta
                if ref._rdt_pending is not None:
                    ref._rdt_pending.wait(timeout=60)
                if ref._binary is not None:
                    data = driver.get([ref._binary], 60000)
                    if data and data[0] is not None:
                        result = _pickle.loads(bytes(data[0][0]))
                        if _is_rdt_meta(result):
                            ref._rdt_source = self._handle
                            ref._rdt_transport = transport
                            ref._rdt_meta = result
        return ref

    def _remote_rdt_nixl_deferred(self, args, rdt_refs, driver):
        """Non-blocking NIXL dispatch when metadata is still pending.

        Returns an ObjectRef immediately and does the actual dispatch in a
        background thread. This prevents blocking the main thread so that
        ray.kill() can proceed for abort tests.
        """
        idx, rdt_ref = rdt_refs[0]

        # Create a deferred result ref
        import os as _os
        result_ref = ObjectRef(_os.urandom(16), None)
        result_ref._rdt_pending = _threading.Event()
        result_ref._rdt_error = None
        result_ref._rdt_result_info = None

        handle = self._handle
        method = self._method
        pending = rdt_ref._rdt_pending

        # Capture source actor info for liveness check
        src_handle = rdt_ref._rdt_source
        src_actor_id_key = None
        if src_handle is not None:
            aid = src_handle._actor_id
            raw = aid.binary() if hasattr(aid, 'binary') else aid
            src_actor_id_key = bytes(raw) if not isinstance(raw, bytes) else raw

        def _deferred():
            try:
                if pending is not None:
                    pending.wait(timeout=120)
                if rdt_ref._rdt_error is not None:
                    result_ref._rdt_error = rdt_ref._rdt_error
                elif rdt_ref._rdt_meta is not None:
                    # Metadata resolved — do the real dispatch
                    real_ref = self._remote_rdt_nixl(args, [(idx, rdt_ref)], driver)
                    result_ref._binary = real_ref._binary
                    result_ref._owner = real_ref._owner
                    result_ref._rdt_source = real_ref._rdt_source
                    result_ref._rdt_transport = real_ref._rdt_transport
                    result_ref._rdt_meta = real_ref._rdt_meta
                    result_ref._rdt_obj_id = real_ref._rdt_obj_id
                    result_ref._rdt_result_info = real_ref._rdt_result_info
                else:
                    result_ref._rdt_error = exceptions.ActorDiedError()
            except Exception as e:
                result_ref._rdt_error = e
            finally:
                result_ref._rdt_pending.set()

        _dispatch_pool.submit(_deferred)
        return result_ref

    def _remote_rdt_nixl(self, args, rdt_refs, driver):
        """Handle a method call where one or more args are NIXL RDT ObjectRefs.

        For NIXL (one-sided), the DESTINATION actor initiates a READ from the
        source actor's registered memory. No coordination with source needed.
        """
        # We handle the single-RDT-arg case
        idx, rdt_ref = rdt_refs[0]
        dst_handle = self._handle

        meta = rdt_ref._rdt_meta
        rdt_obj_id = rdt_ref._rdt_obj_id
        target_tensors = rdt_ref._target_tensors
        result_info = rdt_ref._rdt_result_info

        # Build args: method_name, meta, rdt_obj_id, target_tensors, result_info, other_args...
        other_args = [a for i, a in enumerate(args) if i != idx]

        serialized = [
            _pickle.dumps(self._method),
            _pickle.dumps(meta),
            _pickle.dumps(rdt_obj_id),
            _pickle.dumps(target_tensors),
            _pickle.dumps(result_info),
        ] + [_pickle.dumps(a) for a in other_args]

        oid = driver.submit_actor_method(
            dst_handle._actor_id, "__ray_recv_and_call__", serialized
        )
        ref = ObjectRef(oid.binary(), driver)

        # Check if the called method itself has tensor_transport
        cls = dst_handle._remote_cls
        if cls is not None:
            method_fn = getattr(cls, self._method, None)
            transport = getattr(method_fn, "__ray_tensor_transport__", None)
            if transport and transport == "NIXL":
                # The method returns a tensor too — wait for metadata
                data = driver.get([oid.binary()], 60000)
                if data and data[0] is not None:
                    result = _pickle.loads(bytes(data[0][0]))
                    if isinstance(result, dict) and result.get("__nixl_rdt__"):
                        from ray.experimental.rdt.nixl_tensor_transport import (
                            NixlTransportMetadata,
                        )
                        nmeta = NixlTransportMetadata(
                            tensor_meta=result["tensor_meta"],
                            tensor_device=result.get("tensor_device"),
                            nixl_serialized_descs=result.get("nixl_serialized_descs"),
                            nixl_agent_meta=result.get("nixl_agent_meta"),
                            nixl_agent_name=result.get("nixl_agent_name"),
                            nixl_agent_meta_version=result.get("nixl_agent_meta_version", 0),
                        )
                        ref._rdt_source = dst_handle
                        ref._rdt_transport = "NIXL"
                        ref._rdt_meta = nmeta
                        ref._rdt_obj_id = result.get("rdt_obj_id")
                        ref._rdt_result_info = {
                            "is_dict": result.get("is_dict"),
                            "dict_keys": result.get("dict_keys"),
                            "is_single_tensor": result.get("is_single_tensor"),
                        }
        return ref

    def _remote_rdt_nccl(self, args, rdt_refs, driver):
        """Handle a method call where one or more args are NCCL RDT ObjectRefs.

        Legacy NCCL path: coordinates send+recv between actors.
        """
        from ray.experimental.collective import get_actor_rank

        idx, rdt_ref = rdt_refs[0]
        src_handle = rdt_ref._rdt_source
        dst_handle = self._handle
        meta = rdt_ref._rdt_meta

        src_rank = get_actor_rank(src_handle)
        dst_rank = get_actor_rank(dst_handle)

        if src_rank is None or dst_rank is None:
            raise RuntimeError(
                "RDT transfer requires actors in a collective group. "
                f"src_rank={src_rank}, dst_rank={dst_rank}"
            )

        obj_id = meta["obj_id"]
        shape = meta["shape"]
        dtype = meta["dtype"]

        errors = [None, None]

        def _do_send():
            try:
                ser = [_pickle.dumps(obj_id), _pickle.dumps(dst_rank)]
                oid = driver.submit_actor_method(
                    src_handle._actor_id, "__ray_send__", ser
                )
                data = driver.get([oid.binary()], 60000)
                if data and data[0] is not None:
                    result = _pickle.loads(bytes(data[0][0]))
                    if result.get("status") != "ok":
                        errors[0] = result.get("msg", "send failed")
                else:
                    errors[0] = "send: get returned None"
            except Exception as e:
                errors[0] = str(e)

        def _do_recv():
            try:
                ser = [
                    _pickle.dumps(obj_id),
                    _pickle.dumps(shape),
                    _pickle.dumps(dtype),
                    _pickle.dumps(src_rank),
                ]
                oid = driver.submit_actor_method(
                    dst_handle._actor_id, "__ray_recv__", ser
                )
                data = driver.get([oid.binary()], 60000)
                if data and data[0] is not None:
                    result = _pickle.loads(bytes(data[0][0]))
                    if result.get("status") != "ok":
                        errors[1] = result.get("msg", "recv failed")
                else:
                    errors[1] = "recv: get returned None"
            except Exception as e:
                errors[1] = str(e)

        t_send = _threading.Thread(target=_do_send)
        t_recv = _threading.Thread(target=_do_recv)
        t_send.start()
        t_recv.start()
        t_send.join(timeout=120)
        t_recv.join(timeout=120)

        for err in errors:
            if err is not None:
                raise RuntimeError(f"RDT transfer failed: {err}")

        fetch_args = [self._method, obj_id]
        for i, arg in enumerate(args):
            if i != idx:
                fetch_args.append(arg)

        serialized = [_pickle.dumps(a) for a in fetch_args]
        oid = driver.submit_actor_method(
            dst_handle._actor_id, "__rdt_fetch__", serialized
        )
        return ObjectRef(oid.binary(), driver)

    def options(self, **kwargs):
        """Return a copy with overridden per-call options."""
        merged = {**self._options, **kwargs}
        return _ActorMethodHandle(self._handle, self._method, merged)


class ActorHandle:
    """Handle to a live remote actor.  Methods accessed as attributes."""

    __slots__ = (
        "_actor_id", "_name", "_namespace", "_remote_cls",
        "_port", "_worker_id", "_node_id",
    )

    def __init__(self, actor_id, name: str, namespace: str = "default",
                 remote_cls=None, port=None, worker_id=None, node_id=None):
        self._actor_id = actor_id
        self._name = name
        self._namespace = namespace
        self._remote_cls = remote_cls
        self._port = port
        self._worker_id = worker_id
        self._node_id = node_id

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(name)
        return _ActorMethodHandle(self, name)

    def __repr__(self) -> str:
        return f"Actor({self._name})"

    def __reduce__(self):
        """Make ActorHandle picklable for passing to worker processes."""
        wid = None
        if self._worker_id is not None:
            wid = self._worker_id.hex() if hasattr(self._worker_id, 'hex') else self._worker_id
        nid = None
        if self._node_id is not None:
            raw_nid = self._node_id.binary() if hasattr(self._node_id, 'binary') else self._node_id
            nid = bytes(raw_nid) if not isinstance(raw_nid, bytes) else raw_nid
        raw_aid = self._actor_id.binary() if hasattr(self._actor_id, 'binary') else self._actor_id
        aid = bytes(raw_aid) if not isinstance(raw_aid, bytes) else raw_aid
        return (_reconstruct_actor_handle, (
            aid,
            self._name,
            self._namespace,
            self._port,
            wid,
            nid,
        ))


def _reconstruct_actor_handle(actor_id_binary, name, namespace,
                               port=None, worker_id_hex=None,
                               node_id_binary=None):
    """Reconstruct an ActorHandle from pickled state."""
    try:
        actor_id = PyActorID(actor_id_binary)
    except Exception:
        actor_id = actor_id_binary
    worker_id = None
    if worker_id_hex is not None:
        try:
            worker_id = PyWorkerID.py_from_hex(worker_id_hex)
        except Exception:
            pass
    node_id = None
    if node_id_binary is not None:
        try:
            node_id = PyNodeID(node_id_binary)
        except Exception:
            pass
    return ActorHandle(actor_id, name, namespace, port=port,
                       worker_id=worker_id, node_id=node_id)


class RemoteClass:
    """Factory returned by ``@ray.remote`` on a class."""

    _counter = 0

    def __init__(self, cls, options: dict | None = None):
        self._cls = cls
        self._options = options or {}
        # Stamp the class so _ActorMethodHandle.remote() can detect GPU actors
        if self._options.get("enable_tensor_transport"):
            cls.__ray_enable_tensor_transport__ = True
        elif self._options.get("num_gpus", 0) > 0:
            # Auto-detect: if any method has tensor_transport, enable it
            for attr_name in dir(cls):
                attr = getattr(cls, attr_name, None)
                if callable(attr) and getattr(attr, "__ray_tensor_transport__", None):
                    cls.__ray_enable_tensor_transport__ = True
                    self._options["enable_tensor_transport"] = True
                    break

    def remote(self, *args: Any, **kwargs: Any) -> ActorHandle:
        """Instantiate the actor on a new worker."""
        rt = _runtime
        num_gpus = self._options.get("num_gpus", 0)

        if num_gpus > 0:
            return self._remote_gpu(rt, args, kwargs)
        return self._remote_cpu(rt, args, kwargs)

    def _remote_cpu(self, rt, args, kwargs) -> ActorHandle:
        """Original in-process actor creation path."""
        import asyncio
        instance = self._cls(*args, **kwargs)
        max_concurrency = self._options.get("max_concurrency", 1)

        wid = _PyWorkerID.py_from_random()
        cluster = rt.cluster
        worker = _PyCoreWorker(
            0, "127.0.0.1", cluster.gcs_address(), 1,
            worker_id=wid, node_id=cluster.node_id(),
        )

        # Check if the actor has async methods
        has_async = any(
            asyncio.iscoroutinefunction(getattr(instance, name))
            for name in dir(instance)
            if not name.startswith("_") and callable(getattr(instance, name, None))
        )

        if has_async:
            # Create a persistent event loop in a background thread
            # so multiple async tasks can run concurrently (e.g. wait + send)
            loop = asyncio.new_event_loop()
            _loop_thread = _threading.Thread(
                target=loop.run_forever, daemon=True
            )
            _loop_thread.start()

            async def _run_sync(fn, args):
                return fn(*args)

            def callback(method_name, raw_args, num_returns=1):
                deserialized = [_pickle.loads(a) for a in raw_args]
                fn = getattr(instance, method_name)
                # Run ALL methods on the event loop so sync methods can
                # safely interact with asyncio primitives used by async methods
                if asyncio.iscoroutinefunction(fn):
                    coro = fn(*deserialized)
                else:
                    coro = _run_sync(fn, deserialized)
                future = asyncio.run_coroutine_threadsafe(coro, loop)
                result = future.result(timeout=120)
                return _pickle.dumps(result)
        else:
            def callback(method_name, raw_args, num_returns=1):
                deserialized = [_pickle.loads(a) for a in raw_args]
                result = getattr(instance, method_name)(*deserialized)
                return _pickle.dumps(result)

        worker.set_task_callback(callback)
        port = worker.start_grpc_server()

        RemoteClass._counter += 1
        namespace = self._options.get("namespace", "default")
        name = self._options.get(
            "name", f"{self._cls.__name__}_{RemoteClass._counter}"
        )
        actor_id = rt.gcs.register_actor(name, namespace)

        rt.driver.setup_actor(
            actor_id, name, namespace, "127.0.0.1", port,
            cluster.node_id(), wid,
        )

        rt._actor_workers.append(worker)
        return ActorHandle(actor_id, name, namespace, remote_cls=self._cls,
                           port=port, worker_id=wid, node_id=cluster.node_id())

    def _remote_gpu(self, rt, args, kwargs) -> ActorHandle:
        """Spawn a GPU actor in a separate child process.

        Each GPU actor gets its own process with CUDA_VISIBLE_DEVICES
        pinned, so NCCL/NIXL can run correctly.
        """
        from ray.rdt import gpu_worker_main_from_bytes
        import cloudpickle as _cpickle

        gpu_id = rt._allocate_gpu()
        cluster = rt.cluster
        node_id_bytes = bytes(cluster.node_id().binary())
        gcs_address = cluster.gcs_address()

        # Serialize class and args with cloudpickle
        cls_bytes = _cpickle.dumps(self._cls)
        args_bytes = _cpickle.dumps(args)
        kwargs_bytes = _cpickle.dumps(kwargs)
        options_bytes = _cpickle.dumps(self._options)

        # Use 'spawn' to avoid CUDA fork issues
        ctx = _mp.get_context("spawn")
        parent_conn, child_conn = ctx.Pipe()

        proc = ctx.Process(
            target=gpu_worker_main_from_bytes,
            args=(
                child_conn,
                gpu_id,
                gcs_address,
                node_id_bytes,
                cls_bytes,
                args_bytes,
                kwargs_bytes,
                options_bytes,
            ),
            daemon=True,
        )
        proc.start()
        child_conn.close()

        # Wait for the child to send back connection info
        if not parent_conn.poll(timeout=60):
            proc.kill()
            raise RuntimeError("GPU worker process did not start in time")

        info = parent_conn.recv()
        parent_conn.close()

        port = info["port"]
        wid = _PyWorkerID.py_from_hex(info["worker_id_hex"])

        RemoteClass._counter += 1
        namespace = self._options.get("namespace", "default")
        name = self._options.get(
            "name", f"{self._cls.__name__}_{RemoteClass._counter}"
        )
        actor_id = rt.gcs.register_actor(name, namespace)

        rt.driver.setup_actor(
            actor_id, name, namespace, "127.0.0.1", port,
            cluster.node_id(), wid,
        )

        rt._gpu_processes.append(proc)
        aid_key = bytes(actor_id.binary()) if hasattr(actor_id, 'binary') else actor_id
        rt._actor_id_to_process[aid_key] = proc
        return ActorHandle(
            actor_id, name, namespace, remote_cls=self._cls,
            port=port, worker_id=wid, node_id=cluster.node_id(),
        )

    def options(self, **kwargs) -> RemoteClass:
        """Return a copy with overridden options (name, namespace, ...)."""
        merged = {**self._options, **kwargs}
        return RemoteClass(self._cls, merged)


# =====================================================================
# Runtime singleton
# =====================================================================


class _RayRuntime:
    """Internal singleton managing cluster state."""

    def __init__(self):
        self.cluster = None
        self.gcs = None
        self.driver = None
        self._task_pool: list = []
        self._func_registry: dict = {}
        self._next_worker = 0
        self._actor_workers: list = []
        self._task_call_counts: list = []
        self._initialized = False
        # GPU management
        self._num_gpus = 0
        self._next_gpu = 0
        self._gpu_processes: list = []
        self._actor_id_to_process: dict = {}
        self._killed_actors: set = set()

    # -- lifecycle ---------------------------------------------------

    def init(self, *, num_task_workers: int = 2, num_gpus: int = 0,
             **_kwargs):
        if self._initialized:
            return
        self.cluster = _start_cluster()
        self.gcs = _PyGcsClient(self.cluster.gcs_address())
        self.driver = _PyCoreWorker(
            1, "127.0.0.1", self.cluster.gcs_address(), 1,
            node_id=self.cluster.node_id(),
        )
        self._task_call_counts = [0] * num_task_workers
        for _ in range(num_task_workers):
            self._add_task_worker()
        self._num_gpus = num_gpus
        self._next_gpu = 0
        self._initialized = True
        # Initialize a driver-side RDT manager for tracking metadata
        from ray._compat_rdt import RDTManager
        from ray._private.worker import global_worker
        global_worker.rdt_manager = RDTManager()

    def shutdown(self):
        if not self._initialized:
            return
        # Terminate GPU child processes
        for proc in self._gpu_processes:
            if proc.is_alive():
                proc.terminate()
                proc.join(timeout=5)
                if proc.is_alive():
                    proc.kill()
        self._gpu_processes.clear()
        self._actor_id_to_process.clear()
        self._killed_actors.clear()
        if self.cluster is not None:
            self.cluster.shutdown()
        self.cluster = None
        self.gcs = None
        self.driver = None
        self._task_pool.clear()
        self._func_registry.clear()
        self._actor_workers.clear()
        self._task_call_counts.clear()
        self._next_worker = 0
        self._num_gpus = 0
        self._next_gpu = 0
        self._initialized = False
        RemoteClass._counter = 0
        # Reset collective group state
        from ray.experimental.collective import _actor_rank_map, _group_actors
        _actor_rank_map.clear()
        _group_actors.clear()
        # Reset transport managers
        from ray._compat_rdt import reset_transport_managers
        reset_transport_managers()
        # Reset worker global state
        from ray._private.worker import global_worker
        global_worker.reset()
        # Signal all pending events so daemon threads can exit cleanly
        with _pending_nixl_lock:
            for actor_key, entries in _pending_nixl_refs.items():
                for event, ref_wr in entries:
                    event.set()
            _pending_nixl_refs.clear()
        with _ref_id_to_binary_lock:
            _ref_id_to_binary.clear()
        # Shut down the dispatch pool to release accumulated threads,
        # then recreate it fresh for the next ray.init() session.
        # Use wait=False so stuck driver.get() threads don't block teardown.
        # cancel_futures=True prevents queued-but-not-started work from running.
        global _dispatch_pool
        old_pool = _dispatch_pool
        _dispatch_pool = _ThreadPoolExecutor(max_workers=256, thread_name_prefix="ray-dispatch")
        old_pool.shutdown(wait=False, cancel_futures=True)
        # Brief pause lets idle pool threads notice the shutdown sentinel and exit.
        import time as _time
        _time.sleep(0.3)

    def is_initialized(self) -> bool:
        return self._initialized

    # -- GPU allocation ----------------------------------------------

    def _allocate_gpu(self) -> int:
        """Return the next available GPU ID."""
        if self._num_gpus <= 0:
            raise RuntimeError(
                "No GPUs configured. Call ray.init(num_gpus=N) first."
            )
        gpu_id = self._next_gpu % self._num_gpus
        self._next_gpu += 1
        return gpu_id

    # -- task worker pool --------------------------------------------

    def _add_task_worker(self):
        runtime = self
        worker_idx = len(self._task_pool)
        wid = _PyWorkerID.py_from_random()
        worker = _PyCoreWorker(
            0, "127.0.0.1", self.cluster.gcs_address(), 1,
            worker_id=wid, node_id=self.cluster.node_id(),
        )

        def callback(method, raw_args, num_returns=1):
            runtime._task_call_counts[worker_idx] += 1
            func = runtime._func_registry[method]
            deserialized = [_pickle.loads(a) for a in raw_args]
            result = func(*deserialized)
            if num_returns > 1 and isinstance(result, (list, tuple)):
                return [_pickle.dumps(r) for r in result]
            return _pickle.dumps(result)

        worker.set_task_callback(callback)
        port = worker.start_grpc_server()

        task_driver = _PyCoreWorker(
            1, "127.0.0.1", self.cluster.gcs_address(), 1,
            node_id=self.cluster.node_id(),
        )
        task_driver.setup_task_dispatch("127.0.0.1", port, wid)
        self._task_pool.append((worker, wid, port, task_driver))

    def _pick_task_worker(self):
        idx = self._next_worker % len(self._task_pool)
        self._next_worker += 1
        return self._task_pool[idx]

    # -- decorator ---------------------------------------------------

    def remote(self, func_or_class=None, **options):
        def decorator(target):
            if isinstance(target, type):
                return RemoteClass(target, options)
            self._func_registry[target.__name__] = target
            return RemoteFunction(target, options)
        if func_or_class is not None:
            return decorator(func_or_class)
        return decorator

    # -- object store ------------------------------------------------

    def put(self, obj: Any, *, _tensor_transport: str = None) -> ObjectRef:
        if _tensor_transport:
            # For tensor transport, we need to handle this in the worker context
            # If called from within a GPU worker, it goes through the worker's RDT manager
            from ray._private.worker import global_worker
            if global_worker.rdt_manager is not None:
                return self._put_with_transport(obj, _tensor_transport, global_worker)

            # If called from driver, just pickle it (driver doesn't have GPU)
            data = _pickle.dumps(obj)
            worker = self.driver or self._get_worker()
            oid = worker.put(data, b"pickle")
            return ObjectRef(oid.binary(), worker)
        else:
            data = _pickle.dumps(obj)
            worker = self.driver or self._get_worker()
            oid = worker.put(data, b"pickle")
            return ObjectRef(oid.binary(), worker)

    def _get_worker(self):
        """Get the core worker for the current context (driver or worker process)."""
        from ray._private.worker import global_worker
        if global_worker._core_worker is not None:
            return global_worker._core_worker
        return self.driver

    def _put_with_transport(self, obj, transport_name, worker):
        """Put with tensor transport from within a GPU worker."""
        import torch

        transport_name = transport_name.upper()
        rdt_manager = worker.rdt_manager

        # Extract tensors from the object
        tensors = []
        if isinstance(obj, torch.Tensor):
            tensors = [obj]
        elif isinstance(obj, (list, tuple)):
            tensors = [t for t in obj if isinstance(t, torch.Tensor)]

        if not tensors:
            # No tensors, just pickle
            data = _pickle.dumps(obj)
            oid = worker._core_worker.put(data, b"pickle")
            return ObjectRef(oid.binary(), worker._core_worker)

        # Generate object ID and store in RDT
        import uuid
        obj_id_hex = uuid.uuid4().hex

        # Store tensors and extract NIXL metadata
        meta = rdt_manager.put_object(obj_id_hex, tensors, transport_name)

        # Serialize and store in object store (metadata only, not tensor data)
        meta_dict = {
            "__nixl_rdt__": True,
            "rdt_obj_id": obj_id_hex,
            "tensor_meta": meta.tensor_meta,
            "tensor_device": meta.tensor_device,
            "nixl_serialized_descs": meta.nixl_serialized_descs,
            "nixl_agent_meta": meta.nixl_agent_meta,
            "nixl_agent_name": meta.nixl_agent_name,
            "nixl_agent_meta_version": meta.nixl_agent_meta_version,
            # Store original object structure info for reconstruction
            "is_single_tensor": isinstance(obj, torch.Tensor),
            "is_list": isinstance(obj, list),
            "num_tensors": len(tensors),
        }
        data = _pickle.dumps(meta_dict)
        oid = worker._core_worker.put(data, b"pickle")
        ref = ObjectRef(oid.binary(), worker._core_worker)

        ref_hex = ref.hex()

        # Register ref_hex → rdt_obj_id mapping for lookups by ObjectRef hex
        rdt_manager.register_ref_mapping(ref_hex, obj_id_hex)

        # Add ref_hex as alias in _managed_meta_nixl and _metadata so that
        # tests checking ref.hex() as key work (test_put_gc)
        from ray.experimental.rdt.util import get_tensor_transport_manager
        nixl_transport = get_tensor_transport_manager(transport_name)
        if obj_id_hex in nixl_transport._managed_meta_nixl:
            nixl_transport._managed_meta_nixl[ref_hex] = (
                nixl_transport._managed_meta_nixl[obj_id_hex]
            )
        with rdt_manager._metadata_lock:
            if obj_id_hex in rdt_manager._metadata:
                rdt_manager._metadata[ref_hex] = rdt_manager._metadata[obj_id_hex]

        ref._rdt_source = worker._actor_handle
        ref._rdt_transport = "NIXL"
        ref._rdt_meta = meta
        ref._rdt_obj_id = ref_hex
        return ref

    def get(
        self,
        refs: Union[ObjectRef, Sequence[ObjectRef]],
        *,
        timeout: float = 120.0,
        _use_object_store: bool = False,
    ):
        single = isinstance(refs, ObjectRef)
        if single:
            refs = [refs]
        timeout_ms = int(timeout * 1000)

        results = [None] * len(refs)
        for i, ref in enumerate(refs):
            # Wait for pending metadata resolution
            if ref._rdt_pending is not None:
                ref._rdt_pending.wait(timeout=timeout)
            if ref._rdt_error is not None:
                raise ref._rdt_error
            if (ref._rdt_meta is not None and ref._rdt_transport == "NIXL"):
                if _use_object_store and self.driver is not None and ref._rdt_source is not None:
                    # Driver context: fetch via object store
                    results[i] = self._get_nixl_via_object_store(
                        {"rdt_obj_id": ref._rdt_obj_id}, ref)
                else:
                    # NIXL direct path (also fallback when object store isn't shared)
                    results[i] = self._get_nixl(ref)
            elif ref._owner is not None:
                raw = ref._owner.get([ref._binary], timeout_ms)
                if raw and raw[0] is not None:
                    result = _pickle.loads(raw[0][0])
                    # Check for serialized errors from actor callbacks
                    if isinstance(result, dict) and result.get("__ray_error__"):
                        raise result["error"]
                    # Check if this is NIXL metadata that needs fetching
                    if isinstance(result, dict) and result.get("__nixl_rdt__"):
                        if _use_object_store:
                            # Fetch via object store: get from source actor's rdt_store
                            results[i] = self._get_nixl_via_object_store(result, ref)
                        else:
                            results[i] = self._get_nixl_from_meta(result, ref)
                    else:
                        results[i] = result
            else:
                # Use driver if no owner (or worker core worker)
                worker = self.driver or self._get_worker()
                if worker is None:
                    continue
                raw = worker.get([ref._binary], timeout_ms)
                if raw and raw[0] is not None:
                    result = _pickle.loads(raw[0][0])
                    # Check for serialized errors
                    if isinstance(result, dict) and result.get("__ray_error__"):
                        raise result["error"]
                    if isinstance(result, dict) and result.get("__nixl_rdt__"):
                        if _use_object_store:
                            results[i] = self._get_nixl_via_object_store(result, ref)
                        else:
                            results[i] = self._get_nixl_from_meta(result, ref)
                    else:
                        results[i] = result

        return results[0] if single else results

    def _get_nixl(self, ref):
        """Get tensor via NIXL one-sided READ."""
        obj_id_hex = ref._rdt_obj_id or ref.hex()

        # Check local rdt_store first (same-worker case)
        from ray._private.worker import global_worker
        if global_worker.rdt_manager is not None:
            if global_worker.rdt_manager.rdt_store.has_object(obj_id_hex):
                tensors = global_worker.rdt_manager.rdt_store.get_object(obj_id_hex)
                if len(tensors) == 1:
                    return tensors[0]
                return tensors

        from ray.experimental.rdt.util import get_tensor_transport_manager
        from ray.experimental.rdt.nixl_tensor_transport import NixlCommunicatorMetadata

        meta = ref._rdt_meta
        transport = get_tensor_transport_manager("NIXL")
        target_tensors = getattr(ref, '_target_tensors', None)

        # Validate target buffers before attempting NIXL transfer
        if target_tensors is not None and meta is not None:
            from ray.experimental.rdt.rdt_store import validate_tensor_buffers
            validate_tensor_buffers(
                target_tensors,
                meta.tensor_meta,
                meta.tensor_device,
            )

        tensors = transport.recv_multiple_tensors(
            obj_id_hex,
            meta,
            NixlCommunicatorMetadata(),
            target_buffers=target_tensors,
        )

        if len(tensors) == 1:
            return tensors[0]
        return tensors

    def _get_nixl_from_meta(self, meta_dict, ref):
        """Reconstruct NIXL transport metadata and fetch tensor."""
        obj_id_hex = meta_dict.get("rdt_obj_id", ref.hex())

        # Check local rdt_store first (same-worker case, e.g. ray.put + ray.get in same actor)
        from ray._private.worker import global_worker
        if global_worker.rdt_manager is not None:
            if global_worker.rdt_manager.rdt_store.has_object(obj_id_hex):
                tensors = global_worker.rdt_manager.rdt_store.get_object(obj_id_hex)
                # Set RDT metadata on the ref for GC
                ref._rdt_obj_id = obj_id_hex
                ref._rdt_transport = "NIXL"
                if meta_dict.get("is_single_tensor") and len(tensors) == 1:
                    return tensors[0]
                if meta_dict.get("is_dict") and meta_dict.get("dict_keys"):
                    return dict(zip(meta_dict["dict_keys"], tensors))
                if meta_dict.get("is_list"):
                    return tensors
                if len(tensors) == 1:
                    return tensors[0]
                return tensors

        from ray.experimental.rdt.nixl_tensor_transport import (
            NixlTransportMetadata, NixlCommunicatorMetadata,
        )
        from ray.experimental.rdt.util import get_tensor_transport_manager

        meta = NixlTransportMetadata(
            tensor_meta=meta_dict["tensor_meta"],
            tensor_device=meta_dict.get("tensor_device"),
            nixl_serialized_descs=meta_dict.get("nixl_serialized_descs"),
            nixl_agent_meta=meta_dict.get("nixl_agent_meta"),
            nixl_agent_name=meta_dict.get("nixl_agent_name"),
            nixl_agent_meta_version=meta_dict.get("nixl_agent_meta_version", 0),
        )
        target_tensors = getattr(ref, '_target_tensors', None)

        transport = get_tensor_transport_manager("NIXL")
        tensors = transport.recv_multiple_tensors(
            obj_id_hex,
            meta,
            NixlCommunicatorMetadata(),
            target_buffers=target_tensors,
        )

        # Set RDT metadata on the ref for GC
        ref._rdt_meta = meta
        ref._rdt_obj_id = obj_id_hex
        ref._rdt_transport = "NIXL"

        if meta_dict.get("is_single_tensor") and len(tensors) == 1:
            return tensors[0]
        if meta_dict.get("is_dict") and meta_dict.get("dict_keys"):
            return dict(zip(meta_dict["dict_keys"], tensors))
        if meta_dict.get("is_list"):
            return tensors
        if len(tensors) == 1:
            return tensors[0]
        return tensors

    def _get_nixl_via_object_store(self, meta_dict, ref):
        """Get tensor via object store fallback instead of NIXL."""
        rdt_obj_id = meta_dict.get("rdt_obj_id", ref.hex())

        # Check local rdt_store first (same-worker case)
        from ray._private.worker import global_worker
        if global_worker.rdt_manager is not None:
            if global_worker.rdt_manager.rdt_store.has_object(rdt_obj_id):
                tensors = global_worker.rdt_manager.rdt_store.get_object(rdt_obj_id)
                if len(tensors) == 1:
                    return tensors[0]
                return tensors

        # Submit __ray_fetch_gpu_object__ to source actor to fetch from rdt_store
        source_handle = ref._rdt_source
        driver = self.driver or self._get_worker()

        if source_handle is not None and driver is not None:
            ser = [_pickle.dumps(rdt_obj_id)]
            oid = driver.submit_actor_method(
                source_handle._actor_id, "__ray_fetch_gpu_object__", ser
            )
            data = driver.get([oid.binary()], 60000)
            if data and data[0] is not None:
                result = _pickle.loads(bytes(data[0][0]))
                return result

        # Fallback: just return the metadata dict
        return meta_dict

    def wait(
        self,
        refs: Sequence[ObjectRef],
        *,
        num_returns: int = 1,
        timeout: Optional[float] = None,
    ):
        timeout_val = timeout if timeout is not None else 30.0
        timeout_ms = int(timeout_val * 1000)

        # First, wait for any pending refs to resolve
        for ref in refs:
            if ref._rdt_pending is not None:
                ref._rdt_pending.wait(timeout=timeout_val)

        by_owner: dict = {}
        for ref in refs:
            key = id(ref._owner) if ref._owner else 0
            owner = ref._owner or self.driver
            by_owner.setdefault(key, (owner, []))[1].append(ref)
        ready, remaining = [], []
        for owner, items in by_owner.values():
            binaries = [ref._binary for ref in items]
            flags = owner.wait(binaries, len(binaries), timeout_ms)
            for ref, flag in zip(items, flags):
                (ready if flag else remaining).append(ref)
        if len(ready) > num_returns:
            remaining.extend(ready[num_returns:])
            ready = ready[:num_returns]
        return ready, remaining

    def kill(self, actor: ActorHandle, *, no_restart: bool = True):
        """Kill an actor by terminating its process."""
        actor_id_key = bytes(actor._actor_id.binary()) if hasattr(actor._actor_id, 'binary') else actor._actor_id
        if not isinstance(actor_id_key, bytes):
            actor_id_key = bytes(actor_id_key)
        self._killed_actors.add(actor_id_key)
        proc = self._actor_id_to_process.get(actor_id_key)
        if proc is not None:
            if proc.is_alive():
                proc.terminate()
                proc.join(timeout=5)
                if proc.is_alive():
                    proc.kill()
            del self._actor_id_to_process[actor_id_key]
        else:
            # Try core worker kill
            try:
                self.driver.kill_actor(actor._actor_id, True, no_restart)
            except Exception:
                pass

        # Signal all pending NIXL metadata resolution threads for this actor
        with _pending_nixl_lock:
            entries = _pending_nixl_refs.pop(actor_id_key, [])
        for event, ref_wr in entries:
            ref = ref_wr()
            if ref is not None:
                ref._rdt_error = exceptions.ActorDiedError()
            event.set()

    # -- actor lookup ------------------------------------------------

    def get_actor(self, name: str, namespace: str = "default") -> ActorHandle:
        actor_id = self.gcs.get_named_actor(name, namespace)
        if actor_id is None:
            raise ValueError(
                f"Actor '{name}' not found in namespace '{namespace}'"
            )
        return ActorHandle(actor_id, name, namespace)


# Module-level singleton
_runtime = _RayRuntime()


# =====================================================================
# Module-level API (matches standard Ray)
# =====================================================================


def init(*, num_task_workers: int = 2, num_gpus: int = 0, **kwargs):
    """Initialize Ray (start in-process cluster)."""
    _runtime.init(num_task_workers=num_task_workers, num_gpus=num_gpus,
                  **kwargs)


def shutdown():
    """Shut down Ray."""
    _runtime.shutdown()


def is_initialized() -> bool:
    """Return True if Ray has been initialized."""
    return _runtime.is_initialized()


def remote(func_or_class=None, **options):
    """Decorator to mark a function or class as remote.

    Usage::

        @ray.remote
        def f(x): ...

        @ray.remote(num_returns=2)
        def g(x): ...

        @ray.remote
        class MyActor: ...
    """
    return _runtime.remote(func_or_class, **options)


def put(obj: Any, *, _tensor_transport: str = None) -> ObjectRef:
    """Store an object in the object store."""
    return _runtime.put(obj, _tensor_transport=_tensor_transport)


def get(
    refs: Union[ObjectRef, List[ObjectRef]],
    *,
    timeout: float = 120.0,
    _use_object_store: bool = False,
) -> Any:
    """Retrieve object(s) from the object store."""
    return _runtime.get(refs, timeout=timeout, _use_object_store=_use_object_store)


def wait(
    refs: List[ObjectRef],
    *,
    num_returns: int = 1,
    timeout: Optional[float] = None,
):
    """Wait for a list of ObjectRefs to become ready.

    Returns (ready, remaining) lists.
    """
    return _runtime.wait(refs, num_returns=num_returns, timeout=timeout)


def kill(actor: ActorHandle, *, no_restart: bool = True):
    """Kill an actor forcefully."""
    _runtime.kill(actor, no_restart=no_restart)


def get_actor(name: str, namespace: str = "default") -> ActorHandle:
    """Look up a named actor via GCS."""
    return _runtime.get_actor(name, namespace)


def get_runtime_context():
    """Return a minimal runtime context."""
    return _RuntimeContext()


class _RuntimeContext:
    """Minimal runtime context for NIXL agent naming."""

    _actor_id = None

    def get_actor_id(self):
        return _RuntimeContext._actor_id
