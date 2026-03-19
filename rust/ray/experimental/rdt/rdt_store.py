"""RDT Store and actor task helper functions.

RDTStore is implemented in Rust (via _raylet).
Helper functions (__ray_send__, __ray_recv__, etc.) stay in Python
since they interact with global_worker and Python transport backends.
"""

from typing import TYPE_CHECKING, Any, List, Optional, Tuple

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

from _raylet import PyRDTStore as _RustRDTStore


class RDTStore:
    """Wrapper around Rust ``PyRDTStore``.

    Delegates all core storage operations to the Rust implementation for
    GIL-free waits. ``add_object_primary`` stays in Python so transport
    managers can still be mocked in unit tests.
    """

    def __init__(self):
        self._inner = _RustRDTStore()

    def has_object(self, obj_id):
        return self._inner.has_object(obj_id)

    def has_tensor(self, tensor):
        return self._inner.has_tensor(tensor)

    def get_object(self, obj_id):
        return self._inner.get_object(obj_id)

    def add_object(self, obj_id, rdt_object, is_primary=False):
        return self._inner.add_object(obj_id, rdt_object, is_primary)

    def add_object_primary(self, obj_id, tensors, tensor_transport):
        self.add_object(obj_id, tensors, is_primary=True)
        transport_mgr = get_tensor_transport_manager(tensor_transport)
        return transport_mgr.extract_tensor_transport_metadata(obj_id, tensors)

    def is_primary_copy(self, obj_id):
        return self._inner.is_primary_copy(obj_id)

    def wait_and_get_object(self, obj_id, timeout=None):
        return self._inner.wait_and_get_object(obj_id, timeout)

    def wait_and_pop_object(self, obj_id, timeout=None):
        return self._inner.wait_and_pop_object(obj_id, timeout)

    def pop_object(self, obj_id):
        return self._inner.pop_object(obj_id)

    def wait_tensor_freed(self, tensor, timeout=None):
        return self._inner.wait_tensor_freed(tensor, timeout)

    def get_num_objects(self):
        return self._inner.get_num_objects()


# ── Actor task helper functions (stay in Python) ─────────────────────


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

    device = tensor_transport_meta.tensor_device
    tensor_meta = tensor_transport_meta.tensor_meta

    if tensor_meta and not device_match_transport(device, backend):
        raise ValueError(
            f"Tensor transport backend {backend} does not support tensor transfer on device {device}."
        )

    tensor_transport_manager = get_tensor_transport_manager(backend)
    tensor_transport_manager.send_multiple_tensors(
        tensors,
        tensor_transport_meta,
        communicator_meta,
    )


def validate_tensor_buffers(
    tensor_buffers: List["torch.Tensor"],
    tensor_meta: List[Tuple["torch.Size", "torch.dtype"]],
    device: str,
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
        if single_buffer.device.type != device:
            raise ValueError(
                tensor_buffer_mismatch_msg(
                    "Device", idx, single_buffer.device.type, device
                )
            )
        if not single_buffer.is_contiguous():
            raise ValueError(f"Tensor buffer at index {idx} is not contiguous.")


def __ray_recv__(
    self,
    obj_id: str,
    tensor_transport_meta: TensorTransportMetadata,
    communicator_meta: CommunicatorMetadata,
    backend: str,
    target_buffers: Optional[List["torch.Tensor"]] = None,
):
    """Helper function that runs on the dst actor to receive tensors from the src actor."""
    from ray._private.worker import global_worker

    rdt_store = global_worker.rdt_manager.rdt_store

    try:
        device = tensor_transport_meta.tensor_device
        tensor_meta = tensor_transport_meta.tensor_meta

        if tensor_meta and not device_match_transport(device, backend):
            raise ValueError(
                f"Tensor transport backend {backend} does not support tensor transfer on device {device}."
            )

        tensor_transport_manager = get_tensor_transport_manager(backend)
        received_tensors = tensor_transport_manager.recv_multiple_tensors(
            obj_id,
            tensor_transport_meta,
            communicator_meta,
            target_buffers=target_buffers,
        )
        rdt_store.add_object(obj_id, received_tensors)
    except Exception as e:
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

        tensor_transport_manager = get_tensor_transport_manager(
            tensor_transport_backend
        )
        rdt_manager = global_worker.rdt_manager
        rdt_store = rdt_manager._rdt_store
        if rdt_store is None:
            return
        if rdt_store.has_object(obj_id):
            tensors = rdt_store.pop_object(obj_id)
            tensor_transport_manager.garbage_collect(
                obj_id, tensor_transport_meta, tensors
            )
    except Exception:
        pass


def __ray_fetch_rdt_object__(self, obj_id: str):
    from ray._private.worker import global_worker

    rdt_store = global_worker.rdt_manager.rdt_store
    rdt_object = rdt_store.wait_and_get_object(obj_id)
    return rdt_object
