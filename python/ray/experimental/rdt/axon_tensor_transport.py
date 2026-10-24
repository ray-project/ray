"""Receiver-initiated Axon transport for NumPy and PyTorch tensors.

Ray calls this synchronous interface from several threads. A manager lock
serializes local operations; a separate export lock protects the fetch callback.
Axon's Python completion queue needs one running asyncio loop per process.
Ray shuts down cached managers when the local worker disconnects.
"""

from __future__ import annotations

import asyncio
import atexit
import logging
import math
import os
import threading
import time
import uuid
from concurrent.futures import wait
from dataclasses import dataclass
from typing import Any, List, Optional

import ray
from ray.experimental.rdt.tensor_transport_manager import (
    CommunicatorMetadata,
    TensorTransportManager,
    TensorTransportMetadata,
)

_AXON_FETCH_FUNCTION_ID = 5
_DEFAULT_TIMEOUT_S = 300.0
_runtime_loop_lock = threading.Lock()
_runtime_loop: Optional[asyncio.AbstractEventLoop] = None
_runtime_loop_thread: Optional[threading.Thread] = None
_runtime_users: set[Any] = set()
logger = logging.getLogger(__name__)


def _get_runtime_loop(owner: Any) -> tuple[asyncio.AbstractEventLoop, threading.Thread]:
    """AXON's process-wide wake queue must dispatch onto one Python loop."""
    global _runtime_loop, _runtime_loop_thread
    with _runtime_loop_lock:
        if (
            _runtime_loop is None
            or _runtime_loop.is_closed()
            or _runtime_loop_thread is None
            or not _runtime_loop_thread.is_alive()
        ):
            if _runtime_users:
                raise RuntimeError("Axon's shared runtime loop stopped unexpectedly.")
            loop = asyncio.new_event_loop()

            def run() -> None:
                asyncio.set_event_loop(loop)
                try:
                    loop.run_forever()
                finally:
                    loop.close()

            thread = threading.Thread(target=run, name="ray-axon-runtime", daemon=True)
            _runtime_loop, _runtime_loop_thread = loop, thread
            thread.start()
        _runtime_users.add(owner)
        return _runtime_loop, _runtime_loop_thread


def _release_runtime_loop(owner: Any, timeout: float) -> None:
    global _runtime_loop, _runtime_loop_thread
    with _runtime_loop_lock:
        if owner not in _runtime_users:
            return
        if len(_runtime_users) == 1:
            if _runtime_loop_thread.is_alive():
                _runtime_loop.call_soon_threadsafe(_runtime_loop.stop)
                _runtime_loop_thread.join(timeout=timeout)
                if _runtime_loop_thread.is_alive():
                    raise TimeoutError("Timed out stopping Axon's runtime loop.")
            _runtime_loop = _runtime_loop_thread = None
        _runtime_users.remove(owner)


def _shutdown_runtime_managers() -> None:
    # Keep started managers alive until explicit shutdown or normal process exit:
    # native registered callbacks retain their manager and cannot rely on __del__.
    with _runtime_loop_lock:
        managers = list(_runtime_users)
    for manager in managers:
        try:
            manager.shutdown(timeout=5.0)
        except Exception:
            logger.exception("Failed to shut down Axon transport at process exit")


atexit.register(_shutdown_runtime_managers)


@dataclass
class AxonTransportMetadata(TensorTransportMetadata):
    """Source endpoint and object ID accompanying Ray's shape/dtype metadata."""

    source_worker: str = ""
    source_address: bytes = b""
    source_object_id: str = ""


def _axon_transport_available_in_process(backend: str) -> bool:
    """Probe the optional Axon runtime in the process that will use it."""
    try:
        from ray.experimental.rdt.util import get_tensor_transport_manager

        get_tensor_transport_manager(backend)._ensure_runtime()
        return True
    except Exception:
        logger.debug("Axon tensor transport unavailable on actor.", exc_info=True)
        return False


class _AxonTensorTransportBase(TensorTransportManager):
    """Shared RDT implementation; subclasses only configure Ray registration."""

    _backend = ""
    _array_kind = ""
    _axon_device = "cpu"

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._export_lock = threading.Lock()
        self._exports: dict[str, tuple[Any, ...]] = {}
        self._connected_workers: set[str] = set()
        self._runtime = None
        self._loop = self._loop_thread = None
        self._startup_future = self._shutdown_future = None
        self._pending_futures: set[Any] = set()
        self._closed = False
        self._worker_name = f"ray-{self._backend.lower()}-{uuid.uuid4().hex}"
        self._local_address = b""
        self._from_dlpack = None
        self._cuda_device_index = None
        self._receive_timeout_s = float(
            os.environ.get("AXON_RDT_RECV_TIMEOUT_S", str(_DEFAULT_TIMEOUT_S))
        )

    def tensor_transport_backend(self) -> str:
        return self._backend

    @staticmethod
    def is_one_sided() -> bool:
        # Ray schedules only a receive. Axon serves the source's fetch callback.
        return True

    @staticmethod
    def can_abort_transport() -> bool:
        return False

    def actor_has_tensor_transport(self, actor: "ray.actor.ActorHandle") -> bool:
        return ray.get(
            actor.__ray_call__.options(concurrency_group="_ray_system").remote(
                lambda self, backend: _axon_transport_available_in_process(backend),
                self._backend,
            )
        )

    def _ensure_runtime(self) -> None:
        with self._lock:
            if self._closed:
                raise RuntimeError("Axon transport is closed.")
            if self._startup_future is None:
                if self._axon_device == "cuda":
                    import torch

                    self._cuda_device_index = torch.cuda.current_device()
                self._loop, self._loop_thread = _get_runtime_loop(self)
                self._startup_future = self._schedule(self._initialize_runtime())
            self._startup_future.result(timeout=self._receive_timeout_s)

    async def _initialize_runtime(self) -> None:
        try:
            import axon
        except ImportError as exc:
            raise ImportError(
                "Axon tensor transport requires the execution-ucx Axon Python "
                "package. Install it in every process that uses this transport."
            ) from exc

        device = axon.cpu()
        if self._axon_device == "cuda":
            import torch

            torch.cuda.set_device(self._cuda_device_index)
            device = axon.cuda(device_id=self._cuda_device_index)
        self._runtime = axon.AxonRuntime(
            self._worker_name, timeout=self._receive_timeout_s, device=device
        )
        try:
            self._runtime.start()
            self._local_address = self._runtime.get_local_address()
            array_type, self._from_dlpack = self._array_support()
            self._register_fetch_function(array_type)
        except BaseException:
            await self._stop_runtime()
            raise

    def _register_fetch_function(self, array_type: type) -> None:
        async def fetch(obj_id: str, index: int) -> Any:
            with self._export_lock:
                tensors = self._exports[obj_id]
                if not 0 <= index < len(tensors):
                    raise ValueError("Invalid Axon tensor index")
                # The returned tensor keeps its storage alive during the RPC.
                return tensors[index]

        fetch.__annotations__ = {"obj_id": str, "index": int, "return": array_type}
        self._runtime.register_function(fetch, _AXON_FETCH_FUNCTION_ID)

    def extract_tensor_transport_metadata(
        self, obj_id: str, rdt_object: List[Any]
    ) -> AxonTransportMetadata:
        with self._lock:
            if self._closed:
                raise RuntimeError("Axon transport is closed.")
            for tensor in rdt_object:
                if self._device_type(tensor) != self._axon_device:
                    raise ValueError(
                        f"{self._backend} requires {self._axon_device} tensors."
                    )
                if not self._is_contiguous(tensor):
                    raise ValueError("Axon RDT tensors must be contiguous.")
            metadata = AxonTransportMetadata(
                tensor_meta=[(tensor.shape, tensor.dtype) for tensor in rdt_object],
                tensor_device=self._axon_device if rdt_object else None,
            )
            if rdt_object:
                self._ensure_runtime()
                if self._axon_device == "cuda":
                    import torch

                    # UCX reads storage outside PyTorch's producer streams.
                    for device in {tensor.device for tensor in rdt_object}:
                        torch.cuda.synchronize(device)
                with self._export_lock:
                    self._exports[obj_id] = tuple(rdt_object)
                metadata.source_worker = self._worker_name
                metadata.source_address = self._local_address
                metadata.source_object_id = obj_id
            return metadata

    def get_communicator_metadata(
        self,
        src_actor: "ray.actor.ActorHandle",
        dst_actor: "ray.actor.ActorHandle",
        backend: Optional[str] = None,
    ) -> CommunicatorMetadata:
        # The source endpoint travels with the object; Axon correlates RPCs.
        return CommunicatorMetadata()

    def recv_multiple_tensors(
        self,
        obj_id: str,
        tensor_transport_metadata: TensorTransportMetadata,
        communicator_metadata: CommunicatorMetadata,
        target_buffers: Optional[List[Any]] = None,
    ) -> List[Any]:
        metadata = tensor_transport_metadata
        assert isinstance(metadata, AxonTransportMetadata)
        with self._lock:
            if self._closed:
                raise RuntimeError("Axon transport is closed.")
            if target_buffers is not None:
                if len(target_buffers) != len(metadata.tensor_meta):
                    raise ValueError(
                        "Target buffer count does not match tensor metadata."
                    )
                for target, (shape, dtype) in zip(target_buffers, metadata.tensor_meta):
                    if (
                        tuple(target.shape) != tuple(shape)
                        or target.dtype != dtype
                        or self._device_type(target) != self._axon_device
                        or not self._is_contiguous(target)
                    ):
                        raise ValueError(
                            "Target buffer shape, dtype, device or layout mismatch."
                        )
            if not metadata.tensor_meta:
                return target_buffers if target_buffers is not None else []
            if metadata.tensor_device != self._axon_device:
                raise ValueError(
                    f"{self._backend} requires {self._axon_device} tensors."
                )
            self._ensure_runtime()
            device = "cpu"
            if self._axon_device == "cuda":
                import torch

                # Capture the receiving Ray thread's device, not the loop thread's.
                device = torch.device("cuda", torch.cuda.current_device())

            async def fetch() -> List[Any]:
                if metadata.source_worker not in self._connected_workers:
                    await self._runtime.connect_endpoint_async(
                        metadata.source_address, metadata.source_worker
                    )
                    self._connected_workers.add(metadata.source_worker)
                tensors = []
                for index, (shape, dtype) in enumerate(metadata.tensor_meta):
                    # Receive into framework-owned storage so returned tensors
                    # remain valid after the Axon/UCX runtime shuts down.
                    buffer = self._empty_tensor(shape, dtype, device)
                    if math.prod(shape) == 0:
                        tensors.append(buffer)
                        continue

                    def policy(_, buffer=buffer):
                        return buffer

                    tensor = await self._runtime.invoke(
                        metadata.source_object_id,
                        index,
                        worker_name=metadata.source_worker,
                        session_id=0,
                        function=_AXON_FETCH_FUNCTION_ID,
                        memory_policy=policy,
                        from_dlpack_fn=self._from_dlpack,
                    )
                    if self._axon_device == "cuda" and tensor.device.type != "cuda":
                        raise RuntimeError(
                            "AXON_CUDA received a host payload. Rebuild native Axon "
                            "with CUDA rendezvous support."
                        )
                    if self._axon_device == "cpu" and tensor is not buffer:
                        # Eager CPU replies bypass the memory policy and borrow
                        # UCX receive storage. Copy before that storage can be
                        # released by runtime shutdown. Rendezvous replies use
                        # the supplied buffer directly.
                        if self._array_kind == "numpy":
                            import numpy as np

                            np.copyto(buffer, tensor)
                        else:
                            buffer.copy_(tensor)
                        tensor = buffer
                    tensors.append(tensor)
                return tensors

            tensors = self._submit(fetch())
            if target_buffers is not None:
                for target, tensor in zip(target_buffers, tensors):
                    if self._array_kind == "numpy":
                        import numpy as np

                        np.copyto(target, tensor)
                    else:
                        target.copy_(tensor)
                return target_buffers
            return tensors

    def send_multiple_tensors(
        self,
        tensors: List[Any],
        tensor_transport_metadata: TensorTransportMetadata,
        communicator_metadata: CommunicatorMetadata,
    ) -> None:
        raise NotImplementedError(
            "Axon transfers are initiated by recv_multiple_tensors"
        )

    def garbage_collect(
        self,
        obj_id: str,
        tensor_transport_meta: TensorTransportMetadata,
        tensors: List[Any],
    ) -> None:
        with self._export_lock:
            self._exports.pop(obj_id, None)

    def abort_transport(
        self, obj_id: str, communicator_metadata: CommunicatorMetadata
    ) -> None:
        raise NotImplementedError("Axon does not expose per-transfer cancellation.")

    def _array_support(self) -> tuple[type, Any]:
        if self._array_kind == "numpy":
            import numpy as np

            return np.ndarray, np.from_dlpack
        import torch

        return torch.Tensor, torch.from_dlpack

    def _device_type(self, tensor: Any) -> str:
        return "cpu" if self._array_kind == "numpy" else tensor.device.type

    def _is_contiguous(self, tensor: Any) -> bool:
        if self._array_kind == "numpy":
            return bool(tensor.flags.c_contiguous)
        return bool(tensor.is_contiguous())

    def _empty_tensor(self, shape, dtype, device):
        if self._array_kind == "numpy":
            import numpy as np

            return np.empty(tuple(shape), dtype=dtype)
        import torch

        return torch.empty(tuple(shape), dtype=dtype, device=device)

    def _schedule(self, coroutine):
        # Retain timed-out operations: a caller timeout does not cancel native IO.
        self._pending_futures = {f for f in self._pending_futures if not f.done()}
        future = asyncio.run_coroutine_threadsafe(coroutine, self._loop)
        self._pending_futures.add(future)
        return future

    def _submit(self, coroutine):
        future = self._schedule(coroutine)
        try:
            return future.result(timeout=self._receive_timeout_s)
        finally:
            if future.done():
                self._pending_futures.discard(future)

    async def _stop_runtime(self) -> None:
        if self._runtime is not None:
            self._runtime.stop()
            self._runtime = None

    def shutdown(self, timeout: Optional[float] = None) -> None:
        """Close after all consumers finish; do not race shutdown with new work.

        Wait for submitted IO before stopping on the owning loop. Timeouts retain
        resources for a later shutdown retry. This cannot detect future peer fetches.
        """
        if threading.current_thread() is self._loop_thread:
            raise RuntimeError("Cannot shut down Axon inside a runtime callback.")
        timeout = self._receive_timeout_s if timeout is None else timeout
        if not math.isfinite(timeout) or timeout < 0:
            raise ValueError("Shutdown timeout must be finite and nonnegative.")
        deadline = time.monotonic() + timeout
        if not self._lock.acquire(timeout=timeout):
            raise TimeoutError("Timed out waiting for Axon operations.")
        try:
            self._closed = True
            if self._loop is None:
                return
            if wait(
                self._pending_futures, timeout=max(0, deadline - time.monotonic())
            ).not_done:
                raise TimeoutError("Timed out draining Axon runtime operations.")
            # Drop completed results while the native buffers' owner is alive.
            self._pending_futures.clear()
            if self._shutdown_future is None:
                self._shutdown_future = asyncio.run_coroutine_threadsafe(
                    self._stop_runtime(), self._loop
                )
            try:
                self._shutdown_future.result(
                    timeout=max(0, deadline - time.monotonic())
                )
            except Exception:
                if self._shutdown_future.done():
                    self._shutdown_future = None
                raise
            _release_runtime_loop(self, max(0, deadline - time.monotonic()))
            with self._export_lock:
                self._exports.clear()
            self._connected_workers.clear()
            self._loop = self._loop_thread = self._from_dlpack = None
            self._startup_future = self._shutdown_future = None
            self._local_address = b""
        finally:
            self._lock.release()


class AxonNumpyTensorTransport(_AxonTensorTransportBase):
    """Axon transport for contiguous CPU NumPy arrays."""

    _backend = "AXON"
    _array_kind = "numpy"


class AxonTorchTensorTransport(_AxonTensorTransportBase):
    """Axon transport for contiguous CPU PyTorch tensors."""

    _backend = "AXON_TORCH"
    _array_kind = "torch"


class AxonCudaTensorTransport(_AxonTensorTransportBase):
    """Axon transport for contiguous CUDA PyTorch tensors."""

    _backend = "AXON_CUDA"
    _array_kind = "torch"
    _axon_device = "cuda"
