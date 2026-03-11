"""RDT (Rapid Data Transport) support for the Rust Ray backend.

Provides:
- GPUObjectStore: thread-safe per-worker GPU tensor store (legacy NCCL)
- make_rdt_callback: task callback factory for GPU actor workers
- gpu_worker_main: entry point for GPU actor child processes
- NIXL integration via RDTManager
"""

from __future__ import annotations

import asyncio
import os
import pickle
import threading
import traceback
import uuid
from typing import Any, Dict, Optional, Tuple

# Legacy NCCL sentinel key
RDT_MARKER = "__rdt__"


class GPUObjectStore:
    """Thread-safe store for CUDA tensors on a GPU worker (legacy NCCL).

    Maps object IDs (bytes) to tensors kept on the local GPU device.
    """

    def __init__(self):
        self._store: Dict[bytes, Any] = {}
        self._lock = threading.Lock()

    def put(self, obj_id: bytes, tensor) -> None:
        with self._lock:
            self._store[obj_id] = tensor

    def get(self, obj_id: bytes):
        with self._lock:
            return self._store.get(obj_id)

    def pop(self, obj_id: bytes):
        with self._lock:
            return self._store.pop(obj_id, None)

    def __contains__(self, obj_id: bytes) -> bool:
        with self._lock:
            return obj_id in self._store


def _is_rdt_meta(obj) -> bool:
    """Return True if *obj* is a legacy NCCL RDT metadata dict."""
    return isinstance(obj, dict) and obj.get(RDT_MARKER) is True


def make_rdt_meta(obj_id: bytes, shape: list, dtype: str) -> dict:
    """Build a legacy NCCL RDT metadata dict."""
    return {
        RDT_MARKER: True,
        "obj_id": obj_id,
        "shape": list(shape),
        "dtype": dtype,
    }


def _is_object_ref(obj) -> bool:
    """Return True if obj is a ray.ObjectRef (works across process boundary)."""
    return hasattr(obj, '_binary') and hasattr(obj, '_rdt_transport')


def _resolve_object_ref(ref, rdt_manager):
    """Resolve an ObjectRef arg in a GPU worker.

    Fetches the data from the object store, and if it's NIXL metadata,
    does an NIXL READ to get the actual tensor.
    """
    import sys
    from ray._private.worker import global_worker

    core_worker = global_worker._core_worker
    if core_worker is None:
        print(f"[RDT] _resolve_object_ref: core_worker is None, returning ref as-is", file=sys.stderr, flush=True)
        return ref

    try:
        binary = ref._binary
        print(f"[RDT] _resolve_object_ref: fetching binary={binary[:8].hex()}..., ref type={type(ref).__name__}", file=sys.stderr, flush=True)
        raw = core_worker.get([binary], 60000)
        if raw and raw[0] is not None:
            result = pickle.loads(raw[0][0])
            print(f"[RDT] _resolve_object_ref: got result type={type(result).__name__}, is_dict={isinstance(result, dict)}", file=sys.stderr, flush=True)
            # Check for serialized errors
            if isinstance(result, dict) and result.get("__ray_error__"):
                err = result["error"]
                from ray.exceptions import ActorDiedError, RayTaskError
                if isinstance(err, (ActorDiedError, RayTaskError)):
                    raise err
                raise err
            # Check if NIXL metadata
            if isinstance(result, dict) and result.get("__nixl_rdt__"):
                print(f"[RDT] _resolve_object_ref: resolving NIXL metadata", file=sys.stderr, flush=True)
                return _resolve_nixl_metadata(result, rdt_manager)
            return result
        else:
            print(f"[RDT] _resolve_object_ref: get returned None/empty", file=sys.stderr, flush=True)
    except Exception as e:
        print(f"[RDT] _resolve_object_ref: exception: {e}", file=sys.stderr, flush=True)
        from ray.exceptions import ActorDiedError, RayTaskError
        if isinstance(e, (ActorDiedError, RayTaskError)):
            raise
        # Connection failure likely means actor died
        raise ActorDiedError() from e
    return ref


def _resolve_nixl_metadata(meta_dict, rdt_manager):
    """Resolve NIXL metadata to actual tensor via NIXL READ or local store."""
    rdt_obj_id = meta_dict.get("rdt_obj_id")

    # Check local rdt_store first
    if rdt_manager is not None and rdt_manager.rdt_store.has_object(rdt_obj_id):
        tensors = rdt_manager.rdt_store.get_object(rdt_obj_id)
        if len(tensors) == 1:
            return tensors[0]
        return tensors

    # Do NIXL READ
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
    transport = get_tensor_transport_manager("NIXL")
    tensors = transport.recv_multiple_tensors(
        rdt_obj_id,
        meta,
        NixlCommunicatorMetadata(),
    )

    # Store in local rdt_store
    if rdt_manager is not None:
        rdt_manager.rdt_store.add_object(rdt_obj_id, tensors)

    if meta_dict.get("is_single_tensor") and len(tensors) == 1:
        return tensors[0]
    if meta_dict.get("is_dict") and meta_dict.get("dict_keys"):
        return dict(zip(meta_dict["dict_keys"], tensors))
    if meta_dict.get("is_list"):
        return tensors
    if len(tensors) == 1:
        return tensors[0]
    return tensors


def make_rdt_callback(instance, gpu_object_store, rdt_manager, actor_handle,
                       options=None):
    """Return a task callback for a GPU actor worker.

    Handles both NIXL and NCCL tensor transport, plus special RDT
    control methods.
    """
    options = options or {}
    max_concurrency = options.get("max_concurrency", 1)

    # For async support: use a persistent running event loop in a background thread
    _loop = None
    _loop_thread = None
    _loop_lock = threading.Lock()

    def _get_loop():
        nonlocal _loop, _loop_thread
        with _loop_lock:
            if _loop is None:
                _loop = asyncio.new_event_loop()
                _loop_thread = threading.Thread(
                    target=_loop.run_forever, daemon=True
                )
                _loop_thread.start()
            return _loop

    def _run_async(coro):
        """Run an async coroutine on the shared event loop."""
        loop = _get_loop()
        future = asyncio.run_coroutine_threadsafe(coro, loop)
        return future.result(timeout=120)

    # Background thread pool for _ray_system concurrency group
    _system_semaphore = threading.Semaphore(1)

    # Thread pool for dispatching method calls off of the tokio
    # spawn_blocking thread.  The Rust callback runs on a spawn_blocking
    # thread whose tokio runtime context prevents nested block_on() calls
    # (e.g. when a method calls signal_actor.wait.remote()).  By bouncing
    # to a plain Python thread we avoid this issue and also allow true
    # concurrent method execution via GIL multiplexing.
    from concurrent.futures import ThreadPoolExecutor as _TPE
    _executor = _TPE(max_workers=max(max_concurrency, 10))

    def callback(method_name: str, raw_args: list, num_returns: int = 1):
        # Dispatch to a Python thread to avoid tokio runtime.block_on deadlock
        future = _executor.submit(_callback_safe, method_name, raw_args, num_returns)
        return future.result(timeout=300)

    def _callback_safe(method_name: str, raw_args: list, num_returns: int = 1):
        try:
            return _callback_inner(method_name, raw_args, num_returns)
        except Exception as e:
            # Serialize the exception so the caller gets a proper error
            from ray.exceptions import RayTaskError
            if isinstance(e, RayTaskError):
                return pickle.dumps({"__ray_error__": True, "error": e})
            err = RayTaskError(
                function_name=method_name,
                cause=e,
                traceback_str=traceback.format_exc(),
            )
            return pickle.dumps({"__ray_error__": True, "error": err})

    def _callback_inner(method_name: str, raw_args: list, num_returns: int = 1):
        import torch

        # ── Legacy NCCL control methods ──────────────────────────────
        if method_name == "__init_nccl__":
            rank, world_size, init_method = [pickle.loads(a) for a in raw_args]
            import torch.distributed as dist
            if not dist.is_initialized():
                dist.init_process_group(
                    backend="nccl",
                    init_method=init_method,
                    world_size=world_size,
                    rank=rank,
                )
            return pickle.dumps({"status": "ok", "rank": rank})

        if method_name == "__ray_send__":
            obj_id, dst_rank = [pickle.loads(a) for a in raw_args]
            import torch.distributed as dist
            tensor = gpu_object_store.get(obj_id)
            if tensor is None:
                return pickle.dumps({"status": "error", "msg": "tensor not found"})
            dist.send(tensor, dst=dst_rank)
            return pickle.dumps({"status": "ok"})

        if method_name == "__ray_recv__":
            obj_id, shape, dtype_str, src_rank = [pickle.loads(a) for a in raw_args]
            import torch.distributed as dist
            dtype = getattr(torch, dtype_str, torch.float32)
            buf = torch.empty(shape, dtype=dtype, device="cuda")
            dist.recv(buf, src=src_rank)
            gpu_object_store.put(obj_id, buf)
            return pickle.dumps({"status": "ok"})

        if method_name == "__rdt_fetch__":
            deserialized = [pickle.loads(a) for a in raw_args]
            real_method = deserialized[0]
            rdt_obj_id = deserialized[1]
            extra_args = deserialized[2:]
            tensor = gpu_object_store.get(rdt_obj_id)
            if tensor is None:
                return pickle.dumps({"status": "error", "msg": "rdt tensor not found"})
            result = getattr(instance, real_method)(tensor, *extra_args)
            return pickle.dumps(result)

        # ── NIXL control methods ─────────────────────────────────────

        if method_name == "__ray_recv_and_call__":
            """Receive tensor via NIXL one-sided READ, then call actual method."""
            deserialized = [pickle.loads(a) for a in raw_args]
            real_method = deserialized[0]
            meta = deserialized[1]  # NixlTransportMetadata
            rdt_obj_id = deserialized[2]
            target_tensors = deserialized[3]  # may be None
            result_info = deserialized[4] if len(deserialized) > 4 else None
            other_args = deserialized[5:]

            # Validate target buffers before attempting NIXL transfer
            if target_tensors is not None and meta is not None:
                from ray.experimental.rdt.rdt_store import validate_tensor_buffers
                validate_tensor_buffers(
                    target_tensors,
                    meta.tensor_meta,
                    meta.tensor_device,
                )

            # Check if tensor is already in local rdt_store (intra-actor case)
            if rdt_manager is not None and rdt_manager.rdt_store.has_object(rdt_obj_id):
                tensors = rdt_manager.rdt_store.get_object(rdt_obj_id)
            else:
                from ray.experimental.rdt.util import get_tensor_transport_manager
                from ray.experimental.rdt.nixl_tensor_transport import NixlCommunicatorMetadata

                transport = get_tensor_transport_manager("NIXL")

                # Perform NIXL READ to fetch tensor from source
                try:
                    tensors = transport.recv_multiple_tensors(
                        rdt_obj_id,
                        meta,
                        NixlCommunicatorMetadata(),
                        target_buffers=target_tensors,
                    )
                except Exception as e:
                    # Wrap in RayTaskError-compatible format
                    from ray.exceptions import RayTaskError
                    err = RayTaskError(
                        function_name=real_method,
                        cause=e,
                        traceback_str=traceback.format_exc(),
                    )
                    raise err

                # Store received tensors in local rdt_store for potential reuse
                if rdt_manager is not None:
                    rdt_manager.rdt_store.add_object(rdt_obj_id, tensors)

            # Reconstruct original data structure
            if result_info and result_info.get("is_dict") and result_info.get("dict_keys"):
                data_arg = dict(zip(result_info["dict_keys"], tensors))
            elif result_info and result_info.get("is_single_tensor") and len(tensors) == 1:
                data_arg = tensors[0]
            elif len(tensors) == 1:
                data_arg = tensors[0]
            else:
                data_arg = tensors

            # Call the actual method
            all_args = [data_arg] + list(other_args)

            fn = getattr(instance, real_method)
            if asyncio.iscoroutinefunction(fn):
                result = _run_async(fn(*all_args))
            else:
                result = fn(*all_args)

            # Check if this method has tensor_transport → store result
            method_fn = getattr(type(instance), real_method, None)
            transport_name = getattr(method_fn, "__ray_tensor_transport__", None)

            if transport_name and transport_name == "NIXL":
                return _handle_nixl_return(result, real_method)

            return pickle.dumps(result)

        if method_name == "__ray_free__":
            """Garbage-collect an RDT object (NIXL)."""
            deserialized = [pickle.loads(a) for a in raw_args]
            obj_id_hex = deserialized[0]
            meta = deserialized[1]

            if rdt_manager is not None:
                rdt_manager.free_object(obj_id_hex)
            return pickle.dumps({"status": "ok"})

        if method_name == "__ray_fetch_gpu_object__":
            """Fetch tensor data via object store (fallback path)."""
            deserialized = [pickle.loads(a) for a in raw_args]
            obj_id_hex = deserialized[0]

            if rdt_manager is not None and rdt_manager.rdt_store.has_object(obj_id_hex):
                tensors = rdt_manager.rdt_store.get_object(obj_id_hex)
                if len(tensors) == 1:
                    return pickle.dumps(tensors[0])
                return pickle.dumps(tensors)
            return pickle.dumps(None)

        # ── Regular methods ──────────────────────────────────────────
        deserialized = [pickle.loads(a) for a in raw_args]

        # Log to file for debugging
        with open("/tmp/rdt_callback_debug.log", "a") as _dbg:
            _dbg.write(f"callback method={method_name} num_args={len(deserialized)}\n")
            for _i, _a in enumerate(deserialized):
                _dbg.write(f"  arg[{_i}] type={type(_a).__name__} is_obj_ref={_is_object_ref(_a)}\n")
            _dbg.flush()

        # Resolve any RDT metadata or ObjectRefs in args
        resolved_args = []
        for idx, arg in enumerate(deserialized):
            if _is_rdt_meta(arg):
                # Legacy NCCL path
                t = gpu_object_store.get(arg["obj_id"])
                if t is not None:
                    resolved_args.append(t)
                else:
                    resolved_args.append(arg)
            elif _is_object_ref(arg):
                with open("/tmp/rdt_callback_debug.log", "a") as _dbg:
                    _dbg.write(f"  resolving ObjectRef arg[{idx}]\n")
                    _dbg.flush()
                # Resolve ObjectRef by fetching from object store
                resolved = _resolve_object_ref(arg, rdt_manager)
                resolved_args.append(resolved)
            else:
                resolved_args.append(arg)

        fn = getattr(instance, method_name)
        if asyncio.iscoroutinefunction(fn):
            result = _run_async(fn(*resolved_args))
        else:
            result = fn(*resolved_args)

        # Check if this method has tensor_transport
        method_fn = getattr(type(instance), method_name, None)
        transport_name = getattr(method_fn, "__ray_tensor_transport__", None)

        if transport_name and transport_name == "NIXL":
            return _handle_nixl_return(result, method_name)
        elif transport_name and transport_name == "NCCL":
            # Legacy NCCL path
            if torch.is_tensor(result) and result.is_cuda:
                obj_id = uuid.uuid4().bytes
                gpu_object_store.put(obj_id, result)
                meta = make_rdt_meta(
                    obj_id,
                    list(result.shape),
                    str(result.dtype).replace("torch.", ""),
                )
                return pickle.dumps(meta)

        return pickle.dumps(result)

    def _handle_nixl_return(result, method_name):
        """Handle a method return that should use NIXL transport."""
        import torch

        # Extract tensors from result
        tensors = []
        is_dict = isinstance(result, dict)
        is_single = False

        if isinstance(result, torch.Tensor):
            tensors = [result]
            is_single = True
        elif isinstance(result, dict):
            # Dict of tensors
            tensors = [v for v in result.values() if isinstance(v, torch.Tensor)]
        elif isinstance(result, (list, tuple)):
            tensors = [t for t in result if isinstance(t, torch.Tensor)]

        if not tensors:
            return pickle.dumps(result)

        # Store in RDT manager and register with NIXL
        obj_id_hex = uuid.uuid4().hex

        if rdt_manager is not None:
            meta = rdt_manager.put_object(obj_id_hex, tensors, "NIXL")

            # Return NIXL metadata so driver can coordinate future transfers
            meta_dict = {
                "__nixl_rdt__": True,
                "rdt_obj_id": obj_id_hex,
                "tensor_meta": [(tuple(t.shape), t.dtype) for t in tensors],
                "tensor_device": tensors[0].device.type if tensors else None,
                "nixl_serialized_descs": meta.nixl_serialized_descs,
                "nixl_agent_meta": meta.nixl_agent_meta,
                "nixl_agent_name": meta.nixl_agent_name,
                "nixl_agent_meta_version": meta.nixl_agent_meta_version,
                "is_single_tensor": is_single,
                "is_dict": is_dict,
                "is_list": isinstance(result, (list, tuple)),
                "num_tensors": len(tensors),
            }

            if is_dict:
                # Store the keys for reconstruction
                meta_dict["dict_keys"] = list(result.keys())

            return pickle.dumps(meta_dict)

        return pickle.dumps(result)

    return callback


def gpu_worker_main_from_bytes(
    pipe_conn,
    gpu_id: int,
    gcs_address: str,
    node_id_bytes: bytes,
    cls_bytes: bytes,
    args_bytes: bytes,
    kwargs_bytes: bytes,
    options_bytes: bytes = None,
):
    """Entry point for a GPU actor child process (cloudpickle variant).

    Accepts the class and constructor args as cloudpickle-serialized bytes
    to avoid pickling issues with ``@ray.remote``-wrapped classes.
    """
    import cloudpickle
    cls = cloudpickle.loads(cls_bytes)
    args = cloudpickle.loads(args_bytes)
    kwargs = cloudpickle.loads(kwargs_bytes)
    options = cloudpickle.loads(options_bytes) if options_bytes else {}
    return gpu_worker_main(pipe_conn, gpu_id, gcs_address, node_id_bytes,
                           cls, args, kwargs, options)


def gpu_worker_main(
    pipe_conn,
    gpu_id: int,
    gcs_address: str,
    node_id_bytes: bytes,
    cls,
    args: tuple,
    kwargs: dict,
    options: dict = None,
):
    """Entry point for a GPU actor child process.

    Spawned via ``multiprocessing.Process``. Sets ``CUDA_VISIBLE_DEVICES``,
    creates a PyCoreWorker, initializes RDT manager, and starts gRPC server.
    """
    options = options or {}

    # Pin GPU before importing torch
    os.environ["CUDA_VISIBLE_DEVICES"] = str(gpu_id)

    from _raylet import PyCoreWorker, PyWorkerID, PyNodeID

    # Set up NIXL actor ID before creating the instance
    import ray
    actor_name = f"GPU-{gpu_id}-{uuid.uuid4().hex[:8]}"
    ray._RuntimeContext._actor_id = actor_name

    # Initialize RDT manager
    from ray._compat_rdt import RDTManager
    from ray._private.worker import global_worker

    rdt_mgr = RDTManager()
    global_worker.rdt_manager = rdt_mgr

    instance = cls(*args, **kwargs)
    gpu_store = GPUObjectStore()

    wid = PyWorkerID.py_from_random()
    worker = PyCoreWorker(
        0,  # Worker type
        "127.0.0.1",
        gcs_address,
        1,
        worker_id=wid,
        node_id=PyNodeID(node_id_bytes),
    )

    # Create a minimal worker info object for ray.put/ray.get from within actor
    global_worker._core_worker = worker
    global_worker._actor_handle = None  # Will be set via pipe if needed

    # Create a dummy actor handle for GC tracking
    class _WorkerActorHandle:
        def __init__(self, wid_hex):
            self._actor_id = None
            self._worker_id_hex = wid_hex

    global_worker._actor_handle = _WorkerActorHandle(wid.hex())

    callback = make_rdt_callback(instance, gpu_store, rdt_mgr, None, options)
    worker.set_task_callback(callback)
    port = worker.start_grpc_server()

    # Send connection info back to parent
    pipe_conn.send({
        "port": port,
        "worker_id_hex": wid.hex(),
        "gpu_id": gpu_id,
    })
    pipe_conn.close()

    # Keep the process alive – the gRPC server runs in a background thread
    import time
    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        pass
